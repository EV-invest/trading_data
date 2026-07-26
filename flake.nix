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
        # live 2 — so they can all be up at once and the URL says which is which.
        port_range_base = 59990;

        rs = v_flakes.rs {
          inherit pkgs rust;
          build.workspace = {
            "./trading_data/" = [ "git_version" "log_directives" ];
          };
        };
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
      in
      {
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
              # `viz demo` / `viz live` — the wasm front-end's toolchain is pinned in the sibling
              # exec_viz flake, so that one owns the build; the example it runs lives here. It
              # locates itself via `git rev-parse`, hence the cd.
              (writeShellScriptBin "viz" ''cd "$(git rev-parse --show-toplevel)/../exec_viz" && exec nix run . -- "$@"'')
            ] ++ pre-commit-check.enabledPackages ++ combined.enabledPackages;

            env.PORT = port_range_base;
            env.RUST_BACKTRACE = 1;
            env.RUST_LIB_BACKTRACE = 0;
          };
      }
    );
}
