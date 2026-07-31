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
        # Base of our port range. Each app that listens claims `PORT + <its ordinal>` — demo 1,
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

        # `viz <demo|live|spl>` — our example owns the runtime, the graph and the port; exec_viz is a
        # library plus a bundle builder, so all we take from it is a directory to serve. It builds
        # from the sibling working tree (path deps span three checkouts), hence the `cd`.
        viz = pkgs.writeShellApplication {
          name = "viz";
          runtimeInputs = with pkgs; [ rust git nix pkg-config openssl mold ];
          text = ''
            case "''${1:-demo}" in
              demo) pkg=trading_data_demo ;;
              live) pkg=trading_data_live_example ;;
              spl) pkg=trading_data_spl ;;
              *) echo "usage: viz <demo|live|spl> [-- <app args>]" >&2; exit 1 ;;
            esac
            shift || true
            repo="$(git rev-parse --show-toplevel)"
            EXEC_VIZ_WEB_DIR="$(cd "$repo/../exec_viz" && nix run .)"
            export EXEC_VIZ_WEB_DIR
            # The range base every app claims its ordinal in; an app's own flag overrides it.
            export PORT=${toString port_range_base}
            exec cargo run --manifest-path "$repo/Cargo.toml" -p "$pkg" -- "$@"
          '';
        };
        # `trading_data` is a library — there is no default binary — so a bare `nix run .` lands here.
        help = {
          type = "app";
          program = pkgs.lib.getExe (pkgs.writeShellScriptBin "help" ''
            cat <<'EOF'
            nix run .          this listing
            nix run .#demo     examples/demo — replays a cached day  (port ${toString (port_range_base + 1)})
            nix run .#live     examples/live — 15s of live Bybit     (port ${toString (port_range_base + 2)})
            nix run .#spl      examples/spl  — scam_pump_liqs port   (port ${toString (port_range_base + 3)})

            `nix develop` adds `viz <demo|live|spl>`, the same runner against your working tree.
            Args after `--` reach the app: `nix run .#spl -- --config other.nix`.
            EOF
          '');
        };
      in
      {
        apps = {
          default = help;
          inherit help;
          demo = { type = "app"; program = "${pkgs.writeShellScript "demo" ''exec ${viz}/bin/viz demo "$@"''}"; };
          live = { type = "app"; program = "${pkgs.writeShellScript "live" ''exec ${viz}/bin/viz live "$@"''}"; };
          spl = { type = "app"; program = "${pkgs.writeShellScript "spl" ''exec ${viz}/bin/viz spl "$@"''}"; };
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
