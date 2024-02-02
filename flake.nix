{
  description = "Development environment for Omicron";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };
          # use the Rust toolchain defined in the `rust-toolchain.toml` file.
          rustToolchain = pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

          buildInputs = with pkgs; [
            # libs
            openssl
            postgresql
            xmlsec
            sqlite
            libclang
            libxml2
          ];

          downloadOpenAPI = { name, apiFile, versionPath }: with pkgs.lib; let
            extractHash = { line, prefix }: strings.removeSuffix "\"" (strings.removePrefix "${prefix}=\"" line);
            versionFile = strings.fileContents versionPath;
            parts = strings.splitString "\n" versionFile;
            commit = extractHash { line = elemAt parts 0; prefix = "COMMIT"; };
            sha = extractHash { line = elemAt parts 1; prefix = "SHA2"; };
          in
          {
            file = builtins.fetchurl {
              url = "https://buildomat.eng.oxide.computer/public/file/oxidecomputer/${name}/openapi/${commit}/${apiFile}.json";
              sha256 = sha;
            };
            filename =
              let
                f = "${apiFile}-${commit}.json";
              in
              debug.traceValFn (v: " downloaded ${name} OpenAPI: ${v}") f;
          };

          dendriteOpenAPI = downloadOpenAPI {
            name = "dendrite";
            apiFile = "dpd";
            versionPath = ./tools/dendrite_openapi_version;
          };

          ddmOpenAPI = downloadOpenAPI {
            name = "maghemite";
            apiFile = "ddm-admin";
            versionPath = ./tools/maghemite_ddm_openapi_version;
          };

          mgOpenAPI = downloadOpenAPI {
            name = "maghemite";
            apiFile = "mg-admin";
            versionPath = ./tools/maghemite_mg_openapi_version;
          };

        in
        with pkgs;
        {
          devShells.default = mkShell.override
            {
              # use Clang as the C compiler for all C libraries
              stdenv = clangStdenv;
            }
            {
              inherit buildInputs;
              nativeBuildInputs = with pkgs; [
                rustToolchain
                cmake
                stdenv
                pkg-config
                # The Clickhouse binary downloaded by
                # `tools/install_builder_prerequisites.sh` doesn't work nicely
                # on NixOS due to dynamically loading a bunch of libraries in a
                # way that `nix-ld` doesn't seem to help with. Therefore, depend
                # on the pre-built patched clickhouse package from nixpkgs,
                # instead.
                clickhouse
              ];

              name = "omicron";
              DEP_PQ_LIBDIRS = " ${postgresql.lib}/lib";
              LIBCLANG_PATH = "${libclang.lib}/lib";
              OPENSSL_DIR = "${openssl.dev}";
              OPENSSL_LIB_DIR = "${openssl.out}/lib";

              # Needed by rustfmt-wrapper, see:
              # https://github.com/oxidecomputer/rustfmt-wrapper/blob/main/src/lib.rs
              RUSTFMT = "${rustToolchain}/bin/rustfmt";

              shellHook = ''
                rm -r out/downloads
                mkdir -p out/downloads
                ln -s ${dendriteOpenAPI.file} out/downloads/${dendriteOpenAPI.filename}
                ln -s ${mgOpenAPI.file} out/downloads/${mgOpenAPI.filename}
                ln -s ${ddmOpenAPI.file} out/downloads/${ddmOpenAPI.filename}
              '';
            };
        }
      );
}

