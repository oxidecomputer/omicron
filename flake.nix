{
  description = "Development environment for Omicron";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, rust-overlay }:
    let
      overlays = [ (import rust-overlay) ];
      pkgs = import nixpkgs {
        inherit overlays;
        system = "x86_64-linux";
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

      openAPIVersion = with pkgs.lib; path:
        let
          file = strings.fileContents path;
          parts = strings.splitString "\n" file;
          extractHash = prefix: (line: trivial.pipe line [
            (elemAt parts)
            (strings.removeSuffix "\"")
            (strings.removePrefix "${prefix}=\"")
          ]);
        in
        {
          commit = extractHash "COMMIT" 0;
          sha = extractHash "SHA2" 1;
        };

      downloadBuildomat =
        let baseURL = "https://buildomat.eng.oxide.computer/public/file/oxidecomputer";
        in { kind, repo, file, commit, sha }:
          builtins.fetchurl {
            url = "${baseURL}/${repo}/${kind}/${commit}/${file}";
            sha256 = sha;
          };

      downloadOpenAPI = { repo, file, version }:
        downloadBuildomat
          {
            inherit repo file;
            kind = "openapi";
            commit = pkgs.lib.debug.traceValFn
              (v: "${file}: commit=${v}")
              version.commit;
            sha = version.sha;
          };

      dendriteVersion = openAPIVersion ./tools/dendrite_openapi_version;
      mgVersion = openAPIVersion ./tools/maghemite_mg_openapi_version;

      dendriteOpenAPI = downloadOpenAPI {
        repo = "dendrite";
        file = "dpd.json";
        version = dendriteVersion;
      };

      ddmOpenAPI = downloadOpenAPI {
        repo = "maghemite";
        file = "ddm-admin.json";
        version = openAPIVersion ./tools/maghemite_ddm_openapi_version;
      };

      mgOpenAPI = downloadOpenAPI {
        repo = "maghemite";
        file = "mg-admin.json";
        version = mgVersion;
      };

      findSha = with pkgs.lib; shas: (name:
        let
          upperName = strings.toUpper name;
          prefix = "${upperName}=\"";
        in
        trivial.pipe shas [
          (lists.findFirst (strings.hasPrefix prefix) "")
          (strings.removePrefix prefix)
          (strings.removeSuffix "\"")
        ]);

      dendriteStub = with pkgs.lib;
        let
          commit = dendriteVersion.commit;
          repo = "dendrite";
          stubShas =
            let file = builtins.readFile ./tools/dendrite_stub_checksums;
            in strings.splitString "\n" file;
          findStubSha = name: findSha stubShas "CIDL_SHA256_${name}";
          fetchLinuxBin = file:
            downloadBuildomat {
              inherit commit file repo;
              sha = findStubSha "linux_${file}";
              kind = "linux-bin";
            };

          # get stuff
          tarball = downloadBuildomat
            {
              inherit commit repo;
              sha = findStubSha "illumos";
              kind = "image";
              file = "dendrite-stub.tar.gz";
            };
          swadm = fetchLinuxBin "swadm";
          dpd = fetchLinuxBin "dpd";
        in
        pkgs.stdenv.mkDerivation
          {
            name = "dendrite-stub";
            src = tarball;
            installPhase = ''
              mkdir -p $out/bin
              cp ${swadm} $out/bin/swadm
              chmod +x $out/bin/swadm
              cp ${dpd} $out/bin/dpd
              chmod +x $out/bin/dpd
            '';
          };

      maghemiteMgd = with pkgs.lib;
        let
          commit = mgVersion.commit;
          repo = "maghemite";
          shas =
            let file = builtins.readFile ./tools/maghemite_mgd_checksums;
            in strings.splitString "\n" file;
          # get stuff
          tarball = downloadBuildomat
            {
              inherit commit repo;
              sha = findSha shas "CIDL_SHA256";
              kind = "image";
              file = "mgd.tar.gz";
            };
          linuxBin =
            downloadBuildomat {
              inherit commit repo;
              sha = findSha shas "MGD_LINUX_SHA256";
              kind = "linux";
              file = "mgd";
            };
        in
        pkgs.stdenv.mkDerivation
          {
            name = "mgd";
            src = tarball;
            installPhase = ''
              mkdir -p $out/bin
              cp ${linuxBin} $out/bin/mgd
              chmod +x $out/bin/mgd
            '';
          };

      # omicronDrv = with pkgs; clangStdenv.mkDerivation {

      # };

    in
    with pkgs;
    {
      packages.x86_64-linux = {
        dendrite-stub = dendriteStub;
        mgd = maghemiteMgd;
        # omicron = omicronDrv;
        # default = omicron;

      };


      devShells.x86_64-linux.default = mkShell.override
        {
          # use Clang as the C compiler for all C libraries
          stdenv = clangStdenv;
        }
        {
          inherit buildInputs;

          nativeBuildInputs = [
            rustToolchain
            cmake
            stdenv
            pkg-config
            dendriteStub
            maghemiteMgd
          ];

          name = "omicron";
          src = ./.;
          DEP_PQ_LIBDIRS = "${postgresql.lib}/lib";
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
    };
}




