{
  description = "Development environment for Omicron";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, rust-overlay, ... }:
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
        libtool
      ];

      nativeBuildInputs = with pkgs; [
        rustToolchain
        cmake
        stdenv
        pkg-config
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

      dendriteVersion = openAPIVersion
        ./tools/dendrite_openapi_version;
      mgVersion = openAPIVersion
        ./tools/maghemite_mg_openapi_version;

      dendriteOpenAPI = downloadOpenAPI
        {
          repo = "dendrite";
          file = "dpd.json";
          version = dendriteVersion;
        };

      ddmOpenAPI = downloadOpenAPI
        {
          repo = "maghemite";
          file = "ddm-admin.json";
          version = openAPIVersion ./tools/maghemite_ddm_openapi_version;
        };

      mgOpenAPI = downloadOpenAPI
        {
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

      dendrite-stub = with pkgs.lib;
        let
          commit = dendriteVersion.commit;
          repo = "dendrite";
          stubShas =
            let
              file = builtins.readFile
                ./tools/dendrite_stub_checksums;
            in
            strings.splitString
              "\n"
              file;
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
          swadm = fetchLinuxBin
            "swadm";
          dpd = fetchLinuxBin
            "dpd";
        in
        with pkgs; stdenv.mkDerivation
          {
            name = "dendrite-stub";
            version = commit;
            src = tarball;
            nativeBuildInputs = [
              # patch the binary to use the right dynamic library paths.
              autoPatchelfHook
            ];

            buildInputs = [
              glibc
              gcc-unwrapped
              openssl
            ];

            installPhase =
              let
                binPath = "root/opt/oxide/dendrite/bin";
              in
              ''
                mkdir -p $out/${binPath}
                cp -r . $out/root
                cp ${swadm} $out/${binPath}/swadm
                chmod +x $out/${binPath}/swadm
                cp ${dpd} $out/${binPath}/dpd
                chmod +x $out/${binPath}/dpd

                mkdir -p $out/bin
                ln -s $out/${binPath}/swadm $out/bin/swadm
                ln -s $out/${binPath}/dpd $out/bin/dpd
              '';
          };

      mgd = with pkgs.lib;
        let
          commit = mgVersion.commit;
          repo = "maghemite";
          shas =
            let
              file = builtins.readFile
                ./tools/maghemite_mgd_checksums;
            in
            strings.splitString
              "\n"
              file;
          # get stuff
          tarball = downloadBuildomat
            {
              inherit commit repo;
              sha = findSha shas "CIDL_SHA256";
              kind = "image";
              file = "mgd.tar.gz";
            };
          linuxBin =
            downloadBuildomat
              {
                inherit commit repo;
                sha = findSha shas "MGD_LINUX_SHA256";
                kind = "linux";
                file = "mgd";
              };
        in
        with pkgs;
        stdenv.mkDerivation
          {
            name = "mgd";
            src = tarball;
            version = commit;
            nativeBuildInputs = [
              # patch the binary to use the right dynamic library paths.
              autoPatchelfHook
            ];

            buildInputs = [
              glibc
              gcc-unwrapped
            ];

            installPhase =
              let
                binPath = "root/opt/oxide/mgd/bin";
              in
              ''
                mkdir -p $out/${binPath}
                cp -r . $out/root
                cp ${linuxBin} $out/${binPath}/mgd
                chmod +x $out/${binPath}/mgd

                mkdir -p $out/bin
                ln -s $out/${binPath}/mgd $out/bin/mgd
              '';
          };

      clickhouse = with pkgs;
        let
          # TODO(eliza): it would be nice if this lived in a file that was also used
          # by `tools/ci_download_clickhouse`, so these could be kept consistent...
          version = "22.8.9.24";
          # also, N.B. that unlike maghemite and dendrite, the Clickhouse hashes
          # in `tools/clickhouse_checksums` are MD5 rather than SHA256, so we
          # can't give Nix those hashes and must instead determine it ourselves.
          # this means that we will have to update this SHA if the clickhouse
          # version changes.
          sha256 = "1lgxwh67apgl386ilpg0iy5xkyz12q4lgnz08zswjbxv88ra0qxj";
          name = "clickhouse";
          src = builtins.fetchurl
            {
              inherit sha256;
              url = "https://oxide-clickhouse-build.s3.us-west-2.amazonaws.com/${name}-v${version}.linux.tar.gz";
            };
        in
        stdenv.mkDerivation
          {
            inherit src name version;
            sourceRoot = ".";
            nativeBuildInputs = [
              # patch the binary to use the right dynamic library paths.
              autoPatchelfHook
            ];

            buildInputs = [
              glibc
              gcc-unwrapped
            ];
            installPhase = ''
              mkdir -p $out/bin
              mkdir -p $out/etc
              cp ./${name} $out/bin/${name}
              cp ./._config.xml $out/bin/config.xml
            '';
          };

      cockroachdb = with pkgs;
        let
          binName = "cockroach";
          version = with pkgs.lib; trivial.pipe
            ./tools/cockroachdb_version
            [
              (builtins.readFile)
              (strings.removeSuffix "\n")
            ];
          src = builtins.fetchurl
            {
              url = "https://binaries.cockroachdb.com/${binName}-${version}.linux-amd64.tgz";
              sha256 = "1aglbwh27275bicyvij11s3as4zypqwc26p9gyh5zr3y1s123hr4";
            };
        in
        stdenv.mkDerivation
          {
            name = "cockroachdb";
            inherit src version;
            nativeBuildInputs = [
              # patch the binary to use the right dynamic library paths.
              autoPatchelfHook
            ];

            buildInputs = [
              glibc
              # gcc-unwrapped
            ];
            installPhase = ''
              mkdir -p $out/bin
              cp ./${binName} $out/bin/${binName}
            '';
          };
    in
    {
      packages.x86_64-linux = {
        inherit dendrite-stub mgd cockroachdb clickhouse;
      };

      checks.x86_64-linux = with pkgs;
        {
          clickhouseVersion =
            let
              clickhousePkg = self.packages.x86_64-linux.clickhouse;
              version = clickhousePkg.version;
              bin = clickhousePkg.out + "/bin/clickhouse";
            in
            runCommand "clickhouse-version-test" { } ''
              # the check derivation must have an output.
              touch $out
              actualVersion=$(${bin} server --version | cut -d ' ' -f 4)
              if [ "$actualVersion" != "${version}" ]; then
                echo "expected ClickHouse version ${version}, got $actualVersion"
                exit 1
              fi
            '';
        };

      devShells.x86_64-linux.default =
        pkgs.mkShell.override
          {
            # use Clang as the C compiler for all C libraries
            stdenv = pkgs.clangStdenv;
          }
          {
            inherit buildInputs;
            nativeBuildInputs = nativeBuildInputs ++ [
              # Dendrite and maghemite, for running tests.
              dendrite-stub
              mgd
              clickhouse
              cockroachdb
            ];

            name = "omicron";
            DEP_PQ_LIBDIRS = "${pkgs.postgresql.lib}/lib";
            # LIBCLANG_PATH = "${libclang.lib}/lib";
            OPENSSL_DIR = "${pkgs.openssl.dev}";
            OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";

            MG_OPENAPI_PATH = mgOpenAPI;
            DDM_OPENAPI_PATH = ddmOpenAPI;
            DPD_OPENAPI_PATH = dendriteOpenAPI;

            shellHook = ''
              rm out/mgd
              rm out/dendrite-stub
              rm -r out/clickhouse
              rm -r out/cockroachdb

              mkdir -p out/clickhouse
              mkdir -p out/cockroachdb/

              ln -s ${mgd.out} -T out/mgd
              ln -s ${dendrite-stub.out} -T out/dendrite-stub
              ln -s ${clickhouse.out}/bin/clickhouse out/clickhouse/clickhouse
              ln -s ${clickhouse.out}/etc/config.xml out/clickhouse
              ln -s ${cockroachdb.out}/bin out/cockroachdb/bin
            '';

            # Needed by rustfmt-wrapper, see:
            # https://github.com/oxidecomputer/rustfmt-wrapper/blob/main/src/lib.rs
            RUSTFMT = "${rustToolchain} /bin/rustfmt";
          };
    };
}





