{
  nixConfig = {
    extra-substituters = [ "https://valeratrades.cachix.org" ];
    extra-trusted-public-keys = [ "valeratrades.cachix.org-1:gXVwhzO5YB+BaiEJYT48qZgzdaErGQew6xtZcz4Fo1Q=" ];
  };

  inputs = {
    v_flakes.url = "github:valeratrades/v_flakes?ref=v1.6";
  };
  outputs = { self, v_flakes }:
    let
      inherit (v_flakes) flake-utils pre-commit-hooks;
    in
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import v_flakes.default_nixpkgs { inherit system; config.allowUnfree = true; };
        rust = v_flakes.rs.default_nightly system;
        pre-commit-check = pre-commit-hooks.lib.${system}.run (v_flakes.files.preCommit { inherit pkgs; });
        manifest = (pkgs.lib.importTOML ./trading_data/Cargo.toml).package;
        pname = manifest.name;
        stdenv = pkgs.stdenvAdapters.useMoldLinker pkgs.stdenv;
        # Base of our port range. Each app that listens claims `PORT + <its ordinal>` — simple 1,
        # live 2, spl 3 — so they can all be up at once and the URL says which is which.
        port_range_base = 59990;

        rs = v_flakes.rs { inherit pkgs rust; };
        github = v_flakes.github {
          inherit pkgs pname rs;
          enable = true;
          lastSupportedVersion = "nightly-2026-07-14";
          jobs.default = true;
          lfs = false;
        };
        readme = v_flakes.readme-fw {
          inherit pkgs pname;
          defaults = true;
          lastSupportedVersion = "nightly-1.92";
          rootDir = ./.;
          badges = [ "msrv" "crates_io" "docs_rs" "loc" "ci" ];
        };
        combined = v_flakes.utils.combine { inherit rust; modules = [ rs github readme ]; };

        # `viz <simple|live|spl>` — our example owns the runtime, the graph and the port; exec_viz is a
        # library plus a bundle builder, so all we take from it is a directory to serve. It builds
        # from the sibling working tree (path deps span three checkouts), hence the `cd`.
        viz = pkgs.writeShellApplication {
          name = "viz";
          runtimeInputs = with pkgs; [ rust git nix pkg-config openssl mold psmisc ];
          text = ''
            case "''${1:-spl}" in
              simple) pkg=trading_data_simple; ordinal=1 ;;
              live) pkg=trading_data_live_example; ordinal=2 ;;
              spl) pkg=trading_data_spl; ordinal=3 ;;
              *) echo "usage: viz <simple|live|spl> [-- <app args>]" >&2; exit 1 ;;
            esac
            shift || true
            # A previous run of ourselves, usually. Whoever it is, the port is ours.
            fuser -k "$((${toString port_range_base} + ordinal))/tcp" 2>/dev/null || true
            repo="$(git rev-parse --show-toplevel)"
            EXEC_VIZ_WEB_DIR="$(cd "$repo/../exec_viz" && nix run .)"
            export EXEC_VIZ_WEB_DIR
            # The range base every app claims its ordinal in; an app's own flag overrides it.
            export PORT=${toString port_range_base}
            # `--release`: `spl` replays two days through the graph with a `Viz` attached to every
            # fire, and a debug build spends ~9 minutes on it against ~3. The bar is unwatchable
            # otherwise, and `spl_bench` already reads the same profile.
            exec cargo run --release --manifest-path "$repo/Cargo.toml" -p "$pkg" -- "$@"
          '';
        };
        # `examples/spl/benches` — the same strategy through our DAG and through NautilusTrader, over
        # one tape. Nothing listens, so no port; the build is `cargo bench`'s own profile.
        #
        # `BENCH_CPUS` places the run on a fixed CPU set. Placement alone stops migration, not
        # contention: a core is only ours if everything else is confined to the complement, which is
        # privileged (a user slice is delegated `cpu io memory pids`, never `cpuset`). Cheapest is to
        # have PID 1 hold the reservation — `systemd.settings.Manager.CPUAffinity`, inherited by every
        # unit and session — and that is what the check below looks for. Failing that we take the
        # reservation for the run if sudo answers, and say so loudly if it cannot.
        #
        # SMT means a set has to name both siblings of every physical core it claims (`lscpu -e` pairs
        # them), or what it reserved is threads sharing a contended core.
        spl_bench = pkgs.writeShellApplication {
          name = "spl_bench";
          runtimeInputs = with pkgs; [ rust git nix pkg-config openssl mold util-linux ];
          text = ''
            repo="$(git rev-parse --show-toplevel)"
            if [ -z "''${BENCH_CPUS:-}" ]; then
              exec cargo bench --manifest-path "$repo/Cargo.toml" -p trading_data_spl "$@"
            fi

            # Lexicographic throughout: `comm` compares as strings, so both sides must sort the same way.
            expand() {
              tr -s ', ' '\n' <<<"$1" | while read -r r; do
                case "$r" in "") ;; *-*) seq "''${r%-*}" "''${r#*-}" ;; *) echo "$r" ;; esac
              done | sort -u
            }
            confined="$(awk '/Cpus_allowed_list/{print $2}' /proc/1/status)"
            slices=(init.scope system.slice user.slice)

            if ! comm -12 <(expand "$BENCH_CPUS") <(expand "$confined") | grep -q .; then
              taskset -c "$BENCH_CPUS" cargo bench --manifest-path "$repo/Cargo.toml" -p trading_data_spl "$@"
              exit
            fi

            every="0-$(($(getconf _NPROCESSORS_CONF) - 1))"
            complement="$(comm -23 <(expand "$every") <(expand "$BENCH_CPUS") | paste -sd,)"
            if ! sudo -n true 2>/dev/null; then
              echo "spl_bench: WARNING — $BENCH_CPUS is not reserved and sudo declined, so anything else on this machine shares those cores. Wall clock will read the scheduler as much as the code." >&2
              taskset -c "$BENCH_CPUS" cargo bench --manifest-path "$repo/Cargo.toml" -p trading_data_spl "$@"
              exit
            fi

            for u in "''${slices[@]}"; do sudo -n systemctl set-property --runtime "$u" AllowedCPUs="$complement"; done
            # Restored by naming every CPU, not by clearing the property: an empty value unsets it in
            # systemd but leaves the unit's `cpuset.cpus` at whatever it last wrote, and the machine
            # stays confined until reboot. `--runtime`, so a killed run cannot outlive one anyway.
            trap 'for u in "''${slices[@]}"; do sudo -n systemctl set-property --runtime "$u" AllowedCPUs="$every"; done' EXIT
            echo "spl_bench: took $BENCH_CPUS for this run; set systemd.settings.Manager.CPUAffinity=$complement to hold them" >&2
            # cgroup cpusets only narrow, so the bench cannot live under a slice it just confined — it
            # gets a top-level one instead. A scope is forked by this shell, so the trap still runs.
            # Spelled out rather than inherited: `sudo` has already reset HOME and PATH by the time
            # systemd-run reads them, and a bench that builds into /root is not the same bench.
            env=(-E "PATH=$PATH" -E "HOME=$HOME")
            for v in CARGO_HOME RUSTUP_HOME; do
              if [ -n "''${!v:-}" ]; then env+=(-E "$v=''${!v}"); fi
            done
            sudo -n systemd-run --scope --quiet --collect --slice=spl_bench \
              -p AllowedCPUs="$BENCH_CPUS" --uid="$(id -u)" --gid="$(id -g)" --same-dir "''${env[@]}" \
              cargo bench --manifest-path "$repo/Cargo.toml" -p trading_data_spl "$@"
          '';
        };
        # `trading_data` is a library — there is no default binary — so a bare `nix run .` lands here.
        help = {
          type = "app";
          program = pkgs.lib.getExe (pkgs.writeShellScriptBin "help" ''
            cat <<'EOF'
            nix run .          this listing
            nix run .#simple   examples/simple — one day, one RSI dag (port ${toString (port_range_base + 1)})
            nix run .#live     examples/live   — live Bybit til ctrl-c (port ${toString (port_range_base + 2)})
            nix run .#spl      examples/spl    — scam_pump_liqs port  (port ${toString (port_range_base + 3)})
            nix run .#spl_bench  that same strategy timed against NautilusTrader

            `nix develop` adds `viz <simple|live|spl>`, the same runner against your working tree.
            `cargo r -p trading_data_live_equiv` is the headless live≡replay proof — 15s, no server.
            Args after `--` reach the app: `nix run .#spl -- --config other.nix`.
            EOF
          '');
        };
      in
      {
        apps = {
          default = help;
          inherit help;
          simple = { type = "app"; program = "${pkgs.writeShellScript "simple" ''exec ${viz}/bin/viz simple "$@"''}"; };
          live = { type = "app"; program = "${pkgs.writeShellScript "live" ''exec ${viz}/bin/viz live "$@"''}"; };
          spl = { type = "app"; program = "${pkgs.writeShellScript "spl" ''exec ${viz}/bin/viz spl "$@"''}"; };
          spl_bench = { type = "app"; program = pkgs.lib.getExe spl_bench; };
        };

        packages =
          let
            rustc = rust;
            cargo = rust;
            rustPlatform = pkgs.makeRustPlatform {
              inherit rustc cargo stdenv;
            };
          in
          {
            default = rustPlatform.buildRustPackage {
              inherit pname;
              version = manifest.version;

              buildInputs = with pkgs; [
                openssl.dev
              ];
              nativeBuildInputs = with pkgs; [ pkg-config ];

              cargoLock.lockFile = ./Cargo.lock;
              src = pkgs.lib.cleanSource ./.;
            };
          };

        devShells.default =
          with pkgs;
          mkShell {
            inherit stdenv;
            shellHook =
              pre-commit-check.shellHook
              + combined.shellHook
              + ''
                cp -f ${(v_flakes.files.treefmt) { inherit pkgs; }} ./.treefmt.toml
                cp -f ${(v_flakes.files.gitattributes) { inherit pkgs; lfs = false; }} ./.gitattributes
              '';

            packages = [
              mold
              openssl
              pkg-config
              rust
              # iai-callgrind, the bench harness, shells out to it
              valgrind
              viz
            ] ++ pre-commit-check.enabledPackages ++ combined.enabledPackages;

            env.PORT = port_range_base;
            env.RUST_BACKTRACE = 1;
            env.RUST_LIB_BACKTRACE = 0;
          };
      }
    );
}
