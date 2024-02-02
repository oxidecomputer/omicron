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

          downloadBuildomat = let baseURL = "https://buildomat.eng.oxide.computer/public/file/oxidecomputer"; in kind: ({ repo, file, versionPath }: with pkgs.lib; let
            versionFile = strings.fileContents versionPath;
            parts = strings.splitString "\n" versionFile;

            extractHash = prefix: (line: trivial.pipe (elemAt parts line) [
              (strings.removeSuffix "\"")
              (strings.removePrefix "${prefix}=\"")
              (debug.traceValFn (v: "${repo} ${file} ${prefix}=${v}"))
            ]);

            commit = extractHash "COMMIT" 0;
            sha = extractHash "SHA2" 1;
            path = "${repo}/${kind}/${commit}/${file}";
          in
          builtins.fetchurl {
            url = "${baseURL}/${path}";
            sha256 = sha;
          });

          downloadOpenAPI = downloadBuildomat "openapi";

          dendriteOpenAPI = downloadOpenAPI {
            repo = "dendrite";
            file = "dpd.json";
            versionPath = ./tools/dendrite_openapi_version;
          };

          ddmOpenAPI = downloadOpenAPI {
            repo = "maghemite";
            file = "ddm-admin.json";
            versionPath = ./tools/maghemite_ddm_openapi_version;
          };

          mgOpenAPI = downloadOpenAPI {
            repo = "maghemite";
            file = "mg-admin.json";
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

              MG_OPENAPI_PATH = mgOpenAPI;
              DDM_OPENAPI_PATH = ddmOpenAPI;
              DENDRITE_OPENAPI_PATH = dendriteOpenAPI;

              # Needed by rustfmt-wrapper, see:
              # https://github.com/oxidecomputer/rustfmt-wrapper/blob/main/src/lib.rs
              RUSTFMT = "${rustToolchain}/bin/rustfmt";
            };
        }
      );
}

