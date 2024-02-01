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
                # instead. We'll symlink the binary into `out/clickhouse` in the
                # `shellHook`.
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
                rm -r out/clickhouse
                mkdir -p out/clickhouse/
                ln -s ${clickhouse.out}/bin/clickhouse out/clickhouse/clickhouse
                ln -s ${clickhouse.out}/etc/config.xml out/clickhouse/config.xml
              '';
            };
        }
      );
}
