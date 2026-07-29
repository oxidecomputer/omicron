{
  description = "Development environment for Omicron";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, rust-overlay, ... }:
    let
      overlays = [ (import rust-overlay) ];
      pkgs = import nixpkgs {
        inherit overlays;
        system = "x86_64-linux";
      };
      # use the Rust toolchain defined in the `rust-toolchain.toml` file.
      rustToolchain = (pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override {
        extensions = [
          "rust-src" # for rust-analyzer
        ];
      };

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

      extractHash = with pkgs.lib; { prefix, parts }: (line: trivial.pipe line [
        (elemAt parts)
        (strings.removeSuffix "\"")
        (strings.removePrefix "${prefix}=\"")
      ]);

      openAPIVersion = with pkgs.lib; path:
        let
          file = strings.fileContents path;
          parts = strings.splitString "\n" file;
          extractHash' = prefix: extractHash { inherit prefix; inherit parts; };
        in
        {
          commit = extractHash' "COMMIT" 0;
          sha = extractHash' "SHA2" 1;
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

      mgVersion = openAPIVersion
        ./tools/maghemite_mg_openapi_version;

      mgDdmdCommit = with pkgs.lib;
        let
          file = strings.fileContents ./tools/maghemite_ddm_openapi_version;
          parts = strings.splitString "\n" file;
        in
        extractHash { prefix = "COMMIT"; inherit parts; } 0;

      dendriteCommit = with pkgs.lib;
        let
          file = strings.fileContents ./tools/dendrite_version;
          parts = strings.splitString "\n" file;
        in
        extractHash { prefix = "COMMIT"; inherit parts; } 0;

      # given a list of strings of the form `PREFIX="SHA256"`, finds the string
      # starting with the provided `name` and returns the hash for that prefix.
      findSha = with pkgs.lib;
        shas: (name:
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
          commit = dendriteCommit;
          repo = "dendrite";
          stubShas =
            let
              file = builtins.readFile
                ./tools/dendrite_stub_checksums;
            in
            strings.splitString "\n" file;
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

      maghemiteShas = with pkgs.lib;
        let
          file = builtins.readFile
            ./tools/maghemite_mgd_checksums;
        in
        strings.splitString
          "\n"
          file;

      # downloads a maghemite thing from buildomat, and produces a derivation
      # with the maghemite thing's binary symlinked into its outputs.
      mkMaghemiteThing =
        {
          # name of the maghemite thing to download. this is the filename stem
          # for the tarball to fetch, and the name of the directory for the
          # maghemite thing's outputs (nested under `opt/oxide/`) tarball and
          # binary to symlink into the outputs.
          name
          # name of the Linux binary to download and symlink into the outputs.
          #
          # this defaults to `name` if not specified explicitly
        , binFilename ? name
          # Git SHA of the maghemite repo commit to fetch.
        , commit
          # name of the tarball SHA variable in `maghemite_mgd_checksums`
        , tarballShaName
          # name of the Linux binary SHA variable in `maghemite_mgd_checksums`
        , linuxBinShaName
        }:
          with pkgs.lib; let
            repo = "maghemite";
            # get stuff
            tarball = downloadBuildomat
              {
                inherit commit repo;
                sha = findSha maghemiteShas tarballShaName;
                kind = "image";
                file = "${name}.tar.gz";
              };
            linuxBin =
              downloadBuildomat
                {
                  inherit commit repo;
                  sha = findSha maghemiteShas linuxBinShaName;
                  kind = "linux";
                  file = binFilename;
                };
          in
          with pkgs;
          stdenv.mkDerivation
            {
              inherit name;
              src = tarball;
              version = commit;
              nativeBuildInputs = [
                # patch the binary to use the right dynamic library paths.
                autoPatchelfHook
              ];

              buildInputs = [
                glibc
                gcc-unwrapped
                openssl.dev
              ];

              installPhase =
                let
                  binPath = "root/opt/oxide/${name}/bin";
                in
                ''
                  mkdir -p $out/${binPath}
                  cp -r . $out/root
                  cp ${linuxBin} $out/${binPath}/${binFilename}
                  chmod +x $out/${binPath}/${binFilename}

                  mkdir -p $out/bin
                  ln -s $out/${binPath}/${binFilename} $out/bin/${binFilename}
                '';
            };

      mgd = mkMaghemiteThing {
        name = "mgd";
        commit = mgVersion.commit;
        tarballShaName = "CIDL_SHA256";
        linuxBinShaName = "MGD_LINUX_SHA256";
      };

      mgDdmd = mkMaghemiteThing {
        name = "mg-ddm";
        binFilename = "ddmd";
        commit = mgDdmdCommit;
        tarballShaName = "MG_DDM_SHA256";
        linuxBinShaName = "DDMD_LINUX_SHA256";
      };

      # reads the version for Clickhouse or Cockroachdb from the
      # `tools/clickhouse_version` and `tools/cockroachdb_version` files.
      readVersionFile = with pkgs.lib; file: trivial.pipe ./tools/${file} [
        (builtins.readFile)
        (strings.removeSuffix "\n")
        (strings.removePrefix "v")
        (debug.traceValFn (v: "${file}: ${v}"))
      ];

      clickhouse = with pkgs;
        let
          name = "clickhouse";
          version = readVersionFile "${name}_version";
          sha256 =
            let
              shaFile = builtins.readFile  ./tools/${name}_checksums;
              shas = lib.strings.splitString "\n" shaFile;
            in
            findSha shas "CIDL_SHA256_LINUX";
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
          name = "cockroachdb";
          binName = "cockroach";
          version = readVersionFile "${name}_version";
          lines =
            let
              shaFile = builtins.readFile  ./tools/${name}_checksums;
            in
            lib.strings.splitString "\n" shaFile;
          commit = findSha lines "COCKROACH_COMMIT";
          sha256 = findSha lines "CIDL_SHA256_LINUX";
          src = builtins.fetchurl
            {
              inherit sha256;
              url = "https://buildomat.eng.oxide.computer/public/file/oxidecomputer/cockroach/linux-amd64/${commit}/cockroach.tgz";
            };
        in
        stdenv.mkDerivation
          {
            inherit name src version;
            nativeBuildInputs = [
              # patch the binary to use the right dynamic library paths.
              autoPatchelfHook
            ];

            buildInputs = [
              glibc
              gcc-unwrapped
              ncurses6
            ];
            installPhase = ''
              mkdir -p $out/bin
              cp ./${binName} $out/bin/${binName}
            '';
          };
    in
    {
      packages.x86_64-linux = {
        inherit dendrite-stub mgd mgDdmd cockroachdb clickhouse;
      };

      checks.x86_64-linux = with pkgs;
        let
          # produces a check derivation that ensures a package's executable has
          # the expected version.
          mkVersionCheck = { pkg, cmd }: runCommand "check-${pkg.name}-version"
            {
              PATH = "${pkg.out}";
            } ''
              actualVersion=$(${pkg.out}/bin/${cmd})
              if [ "$actualVersion" != "${pkg.version}" ]; then
                echo "expected ${pkg.name} version \"${pkg.version}\", got \"$actualVersion\""
            exit 1
            fi

            # the check derivation must have an output.
            touch $out
          '';
          # produces a check derivation that ensures a package's executable
          # runs.
          mkExecCheck = { pkg, cmd }: runCommand "check-${pkg.name}-${cmd}-exec"
            { } ''
            ${pkg.out}/bin/${cmd} && touch $out
          '';
        in
        {
          clickhouseVersion = mkVersionCheck
            {
              pkg = clickhouse;
              cmd = "clickhouse server --version | cut -d ' ' -f 4";
            };

          cockroachdbVersion = mkVersionCheck
            {
              pkg = cockroachdb;
              cmd = "cockroach version --build-tag | tr -d 'v'";
            };

          mgdCanExec = mkExecCheck {
            pkg = mgd;
            cmd = "mgd help";
          };

          dpdCanExec = mkExecCheck {
            pkg = dendrite-stub;
            cmd = "dpd help";
          };

          swadmCanExec = mkExecCheck {
            pkg = dendrite-stub;
            cmd = "swadm help";
          };
        };

      devShells.x86_64-linux.default = with pkgs;
        mkShell.override
          {
            stdenv =
              # use Mold as the linker rather than ld, for faster builds. Mold
              # to require substantially less memory to link Nexus and its
              # avoiding swapping on memory-constrained dev systems.
              stdenvAdapters.useMoldLinker
                # use Clang as the C compiler for all C libraries.
                clangStdenv;
          }
          {
            inherit buildInputs;

            # The test suite finds all of these on the PATH (see
            # `test-utils/src/dev/{db,clickhouse,dendrite,maghemite}.rs`, which
            # spawn them by bare name). Placing them in `nativeBuildInputs`
            # ensures that the versions we just stuck in the Nix store are
            # always on the PATH.
            nativeBuildInputs = nativeBuildInputs ++ [
              dendrite-stub
              mgd
              mgDdmd
              clickhouse
              cockroachdb
            ];

            name = "omicron";
            DEP_PQ_LIBDIRS = "${postgresql.lib}/lib";
            LIBCLANG_PATH = "${libclang.lib}/lib";
            OPENSSL_DIR = "${openssl.dev}";
            OPENSSL_LIB_DIR = "${openssl.out}/lib";

            # Needed by rustfmt-wrapper, see:
            # https://github.com/oxidecomputer/rustfmt-wrapper/blob/main/src/lib.rs
            RUSTFMT = "${rustToolchain}/bin/rustfmt";
          };
    };
}
