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
          nativeBuildInputs = with pkgs; [
            rustToolchain
            cmake
            stdenv
            pkg-config
          ];
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
              inherit buildInputs nativeBuildInputs;

              name = "omicron";
              DEP_PQ_LIBDIRS = " ${postgresql.lib}/lib";
              LIBCLANG_PATH = "${libclang.lib}/lib";
              OPENSSL_DIR = "${openssl.dev}";
              OPENSSL_LIB_DIR = "${openssl.out}/lib";
            };
        }
      );
}
