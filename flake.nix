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
              *) echo "usage: viz <simple|live|spl> [-r|--release] [-- <app args>]" >&2; exit 1 ;;
            esac
            shift || true
            # Debug by default: a rebuild costs more than any of these runs. `-r` is for the one
            # that doesn't hold — `spl` replays two days with a `Viz` on every fire, ~9 minutes
            # debug against ~3.
            profile=()
            app_args=()
            for a in "$@"; do
              case "$a" in
                -r|--release) profile=(--release) ;;
                *) app_args+=("$a") ;;
              esac
            done
            # A previous run of ourselves, usually. Whoever it is, the port is ours.
            fuser -k "$((${toString port_range_base} + ordinal))/tcp" 2>/dev/null || true
            repo="$(git rev-parse --show-toplevel)"
            EXEC_VIZ_WEB_DIR="$(cd "$repo/../exec_viz" && nix run .)"
            export EXEC_VIZ_WEB_DIR
            # The range base every app claims its ordinal in; an app's own flag overrides it.
            export PORT=${toString port_range_base}
            exec cargo run "''${profile[@]}" --manifest-path "$repo/Cargo.toml" -p "$pkg" -- "''${app_args[@]}"
          '';
        };
        # `measure <bench|stat|flame|cost> …` — one CPU reservation, four readings of it. What it is
        # and why it reserves is `scripts/measure.rs`'s own header. Built by `rustc` rather than
        # `cargo`: it has no dependencies, and a member of this workspace would rebuild against every
        # graph change to a tool whose whole job is to time those changes.
        measure = pkgs.runCommand "measure"
          {
            nativeBuildInputs = [ rust pkgs.makeWrapper ];
            meta.mainProgram = "measure";
          } ''
          mkdir -p $out/bin
          rustc --edition 2024 -O ${./scripts/measure.rs} -o $out/bin/measure
          # `--prefix`, not `--set`: `sudo` is a setuid wrapper outside the store, and it has to stay
          # reachable through the caller's own PATH.
          wrapProgram $out/bin/measure --prefix PATH : ${pkgs.lib.makeBinPath (with pkgs; [ rust git nix pkg-config openssl mold util-linux valgrind perf cargo-flamegraph ])}
        '';
        # `examples/spl/benches` — the same strategy through our DAG and through NautilusTrader, over
        # one tape. Nothing listens, so no port; the build is `cargo bench`'s own profile.
        spl_bench = pkgs.writeShellScriptBin "spl_bench" ''exec ${pkgs.lib.getExe measure} bench -p trading_data_spl "$@"'';
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
            nix run .#measure  <bench|stat|flame|cost> — counts, `perf stat`, a flamegraph, or the
                               replay's itemized wall clock, all on a reserved CPU set. No verb
                               prints what it can do, and `BENCH_CPUS=<list>` is what it reserves.

            `nix develop` adds `viz <simple|live|spl>`, the same runner against your working tree,
            and `measure` on PATH.
            Args after `--` reach the app: `nix run .#spl -- --config other.nix`. The examples build
            debug; `-r` there builds release, which `spl`'s two-day replay wants.
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
          measure = { type = "app"; program = pkgs.lib.getExe measure; };
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
              cargo-flamegraph
              perf
              measure
              mold
              openssl
              pkg-config
              rust
              # The Firefox Profiler UI over the same samples, and the one that needs no root.
              samply
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
