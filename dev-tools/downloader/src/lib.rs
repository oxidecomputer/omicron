// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask download
//!
//! This is a separate binary because it requires many dependencies that other
//! parts of `cargo xtask` do not.

use anyhow::{Context, Result, bail};
use byte_wrapper::HexArray;
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use clap::ValueEnum;
use flate2::bufread::GzDecoder;
use futures::StreamExt;
use sha2::Digest;
use slog::{Drain, Logger, info, o, warn};
use std::collections::{BTreeMap, BTreeSet, btree_map};
use std::fmt;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::sync::OnceLock;
use std::time::Duration;
use strum::Display;
use strum::EnumIter;
use strum::IntoEnumIterator;
use tar::Archive;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

const BUILDOMAT_URL: &'static str =
    "https://buildomat.eng.oxide.computer/public/file";
const CARGO_HACK_URL: &'static str =
    "https://github.com/taiki-e/cargo-hack/releases/download";

const RETRY_ATTEMPTS: usize = 3;

/// What is being downloaded?
#[derive(
    Copy,
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    ValueEnum,
    EnumIter,
)]
enum Target {
    /// Download all targets
    All,

    /// `cargo hack` binary
    CargoHack,

    /// Clickhouse binary
    Clickhouse,

    /// CockroachDB binary
    Cockroach,

    /// Web console assets
    Console,

    /// Stub Dendrite binary tarball
    DendriteStub,

    /// Maghemite mgd binary
    MaghemiteMgd,

    /// Maghemite ddmd binary
    MaghemiteDdmd,

    /// SoftNPU, an admin program (scadm) and a pre-compiled P4 program.
    Softnpu,

    /// Transceiver Control binary
    TransceiverControl,
}

#[derive(Parser)]
pub struct DownloadArgs {
    /// The targets to be downloaded. This list is additive.
    #[clap(required = true)]
    targets: Vec<Target>,

    /// The path to the "out" directory of omicron.
    #[clap(long, default_value = "out")]
    output_dir: Utf8PathBuf,

    /// The path to the versions and checksums directory.
    #[clap(long, default_value = "tools")]
    versions_dir: Utf8PathBuf,
}

pub async fn run_cmd(args: DownloadArgs) -> Result<()> {
    let mut targets = BTreeSet::new();

    for target in args.targets {
        match target {
            Target::All => {
                // Add all targets, then remove the "All" variant because that
                // isn't a real thing we can download.
                let mut all = BTreeSet::from_iter(Target::iter());
                all.remove(&Target::All);
                targets.append(&mut all);
            }
            _ => _ = targets.insert(target),
        }
    }

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!());

    let mut all_downloads = targets
        .into_iter()
        .map(|target| {
            let log = log.new(o!("target" => format!("{target:?}")));
            let output_dir = args.output_dir.clone();
            let versions_dir = args.versions_dir.clone();
            tokio::task::spawn(async move {
                info!(&log, "Starting download");

                let downloader = Downloader::new(
                    log.clone(),
                    &output_dir,
                    &versions_dir,
                );

                match target {
                    Target::All => {
                        bail!("We should have already filtered this 'All' target out?");
                    }
                    Target::CargoHack => downloader.download_cargo_hack().await,
                    Target::Clickhouse => downloader.download_clickhouse().await,
                    Target::Cockroach => downloader.download_cockroach().await,
                    Target::Console => downloader.download_console().await,
                    Target::DendriteStub => downloader.download_dendrite_stub().await,
                    Target::MaghemiteMgd => downloader.download_maghemite_mgd().await,
                    Target::MaghemiteDdmd => downloader.download_maghemite_ddmd().await,
                    Target::Softnpu => downloader.download_softnpu().await,
                    Target::TransceiverControl => {
                        downloader.download_transceiver_control().await
                    }
                }.context(format!("Failed to download {target:?}"))?;

                info!(&log, "Download complete");
                Ok(())
            })
        })
        .collect::<futures::stream::FuturesUnordered<_>>();

    while let Some(result) = all_downloads.next().await {
        result??;
    }

    Ok(())
}

#[derive(Display)]
enum Os {
    Illumos,
    Linux,
    Mac,
}

#[derive(Display)]
enum Arch {
    X86_64,
    Aarch64,
}

impl Os {
    fn env_name(&self) -> &'static str {
        match self {
            Os::Illumos => "ILLUMOS",
            Os::Linux => "LINUX",
            Os::Mac => "DARWIN",
        }
    }
}

fn os_name() -> Result<Os> {
    let os = match std::env::consts::OS {
        "linux" => Os::Linux,
        "macos" => Os::Mac,
        "solaris" | "illumos" => Os::Illumos,
        other => bail!("OS not supported: {other}"),
    };
    Ok(os)
}

fn arch() -> Result<Arch> {
    let arch = match std::env::consts::ARCH {
        "x86_64" => Arch::X86_64,
        "aarch64" => Arch::Aarch64,
        other => bail!("Architecture not supported: {other}"),
    };
    Ok(arch)
}

/// An Oxide repository at `revision`.
///
/// This assumes that the repo URL is `github.com/oxidecomputer/{name}`.
#[derive(Clone, Copy)]
struct OxideRepoRevision {
    name: &'static str,
    revision: GitCommitHash,
}

/// Represents a file that needs to be retrieved from Buildomat.
///
/// This assumes that:
///
/// * The repo URL is `github.com/oxidecomputer/{name}`.
/// * The file is hosted in Buildomat, under
///   `{BUILDOMAT_URL}/oxidecomputer/{name}/{series}`.
/// * The file is published for `revision`, at `{revision}/{filename}`.
/// * The SHA2-256 checksum for the file is also published.
struct OxideBuildomatFile {
    repo: OxideRepoRevision,
    series: &'static str,
    filename: &'static str,
    sha2: Sha256Digest,
}

impl OxideBuildomatFile {
    fn url(&self) -> String {
        let Self {
            repo: OxideRepoRevision { name, revision },
            series,
            filename,
            sha2: _,
        } = self;
        format!(
            "{BUILDOMAT_URL}/oxidecomputer/{name}/{series}/{revision}/{filename}"
        )
    }
}

/// A binary that needs to either be downloaded or built on a non-illumos
/// platform.
///
/// On illumos, the image tarball (i.e., with series `image`) already provides
/// all binaries.
struct HostNativeBinary {
    name: &'static str,
    /// If downloading the binary for Linux, the SHA2-256 checksum of the file.
    x86_64_linux_sha2: Sha256Digest,
    /// If building the binary from source, the arguments to pass to cargo.
    source_build_args: &'static [&'static str],
}

struct Downloader<'a> {
    log: Logger,

    /// The path to the "out" directory of omicron.
    output_dir: &'a Utf8Path,

    /// The path to the versions and checksums directory.
    versions_dir: &'a Utf8Path,
}

impl<'a> Downloader<'a> {
    fn new(
        log: Logger,
        output_dir: &'a Utf8Path,
        versions_dir: &'a Utf8Path,
    ) -> Self {
        Self { log, output_dir, versions_dir }
    }

    /// Build a binary from a git repository at a specific commit.
    ///
    /// This function:
    /// 1. Checks for cached binaries at `out/.build-cache/{repo_name}/{commit}/`
    /// 2. If not cached, shallow clones the repo to a temp directory
    /// 3. Builds the specified binaries with cargo
    /// 4. Caches the built binaries
    /// 5. Returns paths to the cached binaries
    async fn build_from_git(
        &self,
        repo_name: &str,
        commit: GitCommitHash,
        binaries: &[(&str, &[&str])], // (binary_name, cargo_args)
    ) -> Result<Vec<Utf8PathBuf>> {
        let cache_dir = self
            .output_dir
            .join(".build-cache")
            .join(repo_name)
            .join(commit.to_string());

        // Check if all binaries are already cached
        let mut cached_paths = Vec::new();
        let mut all_cached = true;
        for (binary_name, _) in binaries {
            let cached_path = cache_dir.join(binary_name);
            if !cached_path.exists() {
                all_cached = false;
                break;
            }
            cached_paths.push(cached_path);
        }

        if all_cached {
            info!(self.log, "Found cached binaries for {repo_name} at {commit}"; "cache_dir" => %cache_dir);
            return Ok(cached_paths);
        }

        // Need to build - create temp directory
        info!(self.log, "Building {repo_name} from source at commit {commit}");
        let temp_dir = camino_tempfile::tempdir()?;
        let temp_path = temp_dir.path().to_owned();

        // Clone and checkout the specific commit
        let repo_url =
            format!("https://github.com/oxidecomputer/{}", repo_name);
        info!(self.log, "Cloning {repo_url}");
        let mut clone_cmd = Command::new("git");
        clone_cmd
            .arg("clone")
            .arg("--filter=blob:none")
            .arg(&repo_url)
            .arg(&temp_path);

        let clone_output = clone_cmd.output().await?;
        if !clone_output.status.success() {
            let stderr = String::from_utf8_lossy(&clone_output.stderr);
            bail!("Failed to clone {repo_url}: {stderr}");
        }

        // Checkout the specific commit
        info!(self.log, "Checking out commit {commit}");
        let mut checkout_cmd = Command::new("git");
        checkout_cmd
            .arg("checkout")
            .arg(commit.to_string())
            .current_dir(&temp_path);

        let checkout_output = checkout_cmd.output().await?;
        if !checkout_output.status.success() {
            let stderr = String::from_utf8_lossy(&checkout_output.stderr);
            bail!("Failed to checkout {commit}: {stderr}");
        }

        // Build each binary
        tokio::fs::create_dir_all(&cache_dir).await?;
        let mut result_paths = Vec::new();

        for (binary_name, cargo_args) in binaries {
            info!(self.log, "Building {binary_name}"; "args" => ?cargo_args);

            let mut build_cmd = Command::new("cargo");
            build_cmd
                .arg("build")
                .arg("--release")
                .arg("--bin")
                .arg(binary_name)
                .args(*cargo_args)
                .current_dir(&temp_path);

            let build_output = build_cmd.output().await?;
            if !build_output.status.success() {
                let stderr = String::from_utf8_lossy(&build_output.stderr);
                bail!("Failed to build {binary_name}: {stderr}");
            }

            // Always build in release mode
            let source_path =
                temp_path.join("target").join("release").join(binary_name);

            if !source_path.exists() {
                bail!("Expected binary not found at {source_path}");
            }

            // Copy to cache
            let cached_path = cache_dir.join(binary_name);
            tokio::fs::copy(&source_path, &cached_path).await?;
            set_permissions(&cached_path, 0o755).await?;

            result_paths.push(cached_path);
        }

        info!(self.log, "Successfully built and cached {repo_name} binaries");
        Ok(result_paths)
    }
}

/// A file containing data in the format:
///
/// ```ignore
/// KEY1="value1"
/// KEY2="value2"
/// ```
///
/// These files are kept under `tools/`, and are formatted so that both shell
/// scripts and Rust can read them.
#[derive(Debug)]
struct KvFile {
    path: Utf8PathBuf,
    values: BTreeMap<String, String>,
}

impl KvFile {
    async fn read(path: Utf8PathBuf) -> Result<Self> {
        let contents = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("Failed to read {path}"))?;
        Self::parse(path, &contents)
    }

    fn parse(path: Utf8PathBuf, contents: &str) -> Result<Self> {
        let mut values = BTreeMap::new();
        for (index, line) in contents.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let line_number = index + 1;
            let Some((key, value)) =
                line.split_once('=').filter(|(key, _)| !key.is_empty())
            else {
                // Ban non-empty lines that aren't in the correct KEY="value"
                // format.
                bail!(
                    "{path}:{line_number}: expected KEY=\"value\", found {line:?}"
                );
            };
            match values.entry(key.to_owned()) {
                btree_map::Entry::Vacant(entry) => {
                    // We permit both quoted and unquoted values.
                    entry.insert(value.trim_matches('"').to_owned());
                }
                btree_map::Entry::Occupied(_) => {
                    // Ban duplicate keys.
                    bail!(
                        "{path}:{line_number}: key {key:?} is already set earlier in the file"
                    );
                }
            }
        }
        Ok(Self { path, values })
    }

    fn get_string_value(&self, key: &str) -> Result<&str> {
        let Some(value) = self.values.get(key) else {
            bail!(
                "Could not find key {key:?} in {}, which sets {:?}",
                self.path,
                self.values.keys().collect::<Vec<_>>(),
            );
        };
        Ok(value)
    }

    fn get_sha256_value(&self, key: &str) -> Result<Sha256Digest> {
        let value = self.get_string_value(key)?;
        let bytes = value.parse().with_context(|| {
            format!(
                "Invalid SHA-256 checksum {value:?} for {key} in {}",
                self.path,
            )
        })?;
        Ok(Sha256Digest(bytes))
    }

    fn get_git_commit_hash_value(&self, key: &str) -> Result<GitCommitHash> {
        let value = self.get_string_value(key)?;
        let bytes = value.parse().with_context(|| {
            format!(
                "Invalid git commit hash {value:?} for {key} in {}",
                self.path
            )
        })?;
        Ok(GitCommitHash(bytes))
    }
}

/// Send a GET request to `url`, downloading the contents to `path`.
///
/// Writes the response to the file as it is received.
async fn streaming_download(url: &str, path: &Utf8Path) -> Result<()> {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

    let client = CLIENT.get_or_init(|| {
        reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(3600))
            .tcp_keepalive(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(15))
            .build()
            .unwrap()
    });
    let mut response = client.get(url).send().await?.error_for_status()?;
    let mut tarball = tokio::fs::File::create(&path).await?;
    while let Some(chunk) = response.chunk().await? {
        tarball.write_all(chunk.as_ref()).await?;
    }
    tarball.flush().await?;
    Ok(())
}

/// A SHA-256 digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Sha256Digest(HexArray<32>);

impl Sha256Digest {
    /// Computes and returns the SHA-256 digest of the file at `path`.
    async fn from_path(path: &Utf8Path) -> Result<Self> {
        let mut buf = vec![0u8; 65536];
        let mut file = tokio::fs::File::open(path).await?;
        let mut ctx = sha2::Sha256::new();
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            ctx.write_all(&buf[0..n])?;
        }

        Ok(Self(HexArray::new(ctx.finalize().into())))
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A full (non-abbreviated) Git commit hash.
///
/// Currently we only support SHA-1 commit hashes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GitCommitHash(HexArray<20>);

impl fmt::Display for GitCommitHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

async fn unpack_tarball(
    log: &Logger,
    tarball_path: &Utf8Path,
    destination_dir: &Utf8Path,
) -> Result<()> {
    info!(log, "Unpacking {tarball_path} to {destination_dir}");
    let tarball_path = tarball_path.to_owned();
    let destination_dir = destination_dir.to_owned();

    let task = tokio::task::spawn_blocking(move || {
        let reader = std::fs::File::open(tarball_path)?;
        let buf_reader = std::io::BufReader::new(reader);
        let gz = GzDecoder::new(buf_reader);
        let mut archive = Archive::new(gz);
        archive.unpack(&destination_dir)?;
        Ok(())
    });
    task.await?
}

async fn unpack_gzip(
    log: &Logger,
    gzip_path: &Utf8Path,
    destination: &Utf8Path,
) -> Result<()> {
    info!(log, "Unpacking {gzip_path} to {destination}");
    let gzip_path = gzip_path.to_owned();
    let destination = destination.to_owned();

    let task = tokio::task::spawn_blocking(move || {
        let reader = std::fs::File::open(gzip_path)?;
        let buf_reader = std::io::BufReader::new(reader);
        let mut gz = GzDecoder::new(buf_reader);

        let mut destination = std::fs::File::create(destination)?;
        std::io::copy(&mut gz, &mut destination)?;
        Ok(())
    });
    task.await?
}

async fn confirm_binary_works(binary: &Utf8Path, args: &[&str]) -> Result<()> {
    let mut cmd = Command::new(binary);
    cmd.args(args);

    let output = cmd
        .output()
        .await
        .with_context(|| format!("Failed to run {binary}"))?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "{binary} {args:?} failed: {} (stdout: {stdout}, stderr: {stderr})",
            output.status
        );
    }
    Ok(())
}

fn copy_dir_all(src: &Utf8Path, dst: &Utf8Path) -> Result<()> {
    std::fs::create_dir_all(&dst)?;
    for entry in src.read_dir_utf8()? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), &dst.join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), &dst.join(entry.file_name()))?;
        }
    }
    Ok(())
}

async fn set_permissions(path: &Utf8Path, mode: u32) -> Result<()> {
    let mut p = tokio::fs::metadata(&path).await?.permissions();
    p.set_mode(mode);
    tokio::fs::set_permissions(&path, p).await?;
    Ok(())
}

/// Downloads a file and verifies the checksum.
///
/// If the file already exists and the checksum matches,
/// avoids performing the download altogether.
async fn download_file_and_verify(
    log: &Logger,
    path: &Utf8Path,
    url: &str,
    checksum: Sha256Digest,
) -> Result<()> {
    let do_download = if path.exists() {
        info!(log, "Already downloaded ({path})");
        if Sha256Digest::from_path(path).await? == checksum {
            info!(
                log,
                "Checksum matches already downloaded file - skipping download"
            );
            false
        } else {
            warn!(log, "Checksum mismatch - retrying download");
            true
        }
    } else {
        true
    };

    if do_download {
        for attempt in 1..=RETRY_ATTEMPTS {
            info!(
                log,
                "Downloading {path} (attempt {attempt}/{RETRY_ATTEMPTS})"
            );
            match streaming_download(&url, &path).await {
                Ok(()) => break,
                Err(err) => {
                    if attempt == RETRY_ATTEMPTS {
                        return Err(err);
                    } else {
                        warn!(log, "Download failed, retrying: {err}");
                    }
                }
            }
        }
    }

    let observed_checksum = Sha256Digest::from_path(path).await?;
    if observed_checksum != checksum {
        bail!(
            "Checksum mismatch (saw {observed_checksum}, expected {checksum})"
        );
    }
    Ok(())
}

impl Downloader<'_> {
    async fn read_version_file(&self, filename: &str) -> Result<String> {
        let path = self.versions_dir.join(filename);
        let contents = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("Failed to read version from {path}"))?;
        let version = contents.trim();
        if version.is_empty() {
            bail!("Version file {path} should not be empty");
        }
        Ok(version.to_string())
    }

    /// Downloads a file off of Buildomat to the given path, and verifies its
    /// checksum.
    ///
    /// If the path is already present and matches the checksum, it isn't
    /// downloaded again.
    async fn download_oxide_buildomat_file(
        &self,
        file: &OxideBuildomatFile,
        path: &Utf8Path,
    ) -> Result<()> {
        download_file_and_verify(&self.log, path, &file.url(), file.sha2).await
    }

    /// Downloads an executable file off of Buildomat to the given path, and
    /// verifies its checksum.
    ///
    /// The file is staged in `{self.output_dir}/downloads`.
    async fn download_oxide_buildomat_binary(
        &self,
        binary: &OxideBuildomatFile,
        binary_dir: &Utf8Path,
    ) -> Result<()> {
        let path = self.output_dir.join("downloads").join(binary.filename);
        self.download_oxide_buildomat_file(binary, &path).await?;
        set_permissions(&path, 0o755).await?;
        let dest = binary_dir.join(binary.filename);
        tokio::fs::copy(&path, &dest)
            .await
            .with_context(|| format!("Failed to copy {path} to {dest}"))?;

        // A prebuilt binary can potentially be incompatible with this host
        // (e.g., due to a libc version or dynamically linked libraries). Do a
        // little smoke test to make sure the binary works.
        info!(self.log, "Checking that {} works", binary.filename);
        confirm_binary_works(&dest, &["--help"]).await?;
        Ok(())
    }

    /// Installs binaries for the host into `binary_dir`.
    ///
    /// Uses prebuilt binaries where available, otherwise builds from source.
    ///
    /// This doesn't do anything on illumos, since the image tarball already
    /// provides the binaries.
    async fn install_host_native_binaries(
        &self,
        repo: OxideRepoRevision,
        linux_series: &'static str,
        binaries: &[HostNativeBinary],
        binary_dir: &Utf8Path,
    ) -> Result<()> {
        match os_name()? {
            Os::Linux => {
                for binary in binaries {
                    let file = OxideBuildomatFile {
                        repo,
                        series: linux_series,
                        filename: binary.name,
                        sha2: binary.x86_64_linux_sha2,
                    };
                    self.download_oxide_buildomat_binary(&file, binary_dir)
                        .await?;
                }
            }
            Os::Mac => {
                info!(self.log, "Building {} from source for macOS", repo.name,);

                // Perform a single `build_from_git` call for all provided
                // binaries, so they share a checkout and target directory.
                let to_build: Vec<_> = binaries
                    .iter()
                    .map(|binary| (binary.name, binary.source_build_args))
                    .collect();
                let built_binaries = self
                    .build_from_git(repo.name, repo.revision, &to_build)
                    .await?;

                for (built_path, binary) in built_binaries.iter().zip(binaries)
                {
                    let dest = binary_dir.join(binary.name);
                    tokio::fs::copy(built_path, &dest).await?;
                    set_permissions(&dest, 0o755).await?;
                }
            }
            Os::Illumos => {
                // The image tarball already provides all binaries.
            }
        }

        Ok(())
    }

    async fn download_cargo_hack(&self) -> Result<()> {
        let os = os_name()?;
        let arch = arch()?;

        let download_dir = self.output_dir.join("downloads");
        let destination_dir = self.output_dir.join("cargo-hack");

        let checksums =
            KvFile::read(self.versions_dir.join("cargo_hack_checksum")).await?;
        let checksum = checksums
            .get_sha256_value(&format!("CIDL_SHA256_{}", os.env_name()))?;

        let version = self.read_version_file("cargo_hack_version").await?;

        let (platform, supported_arch) = match (os, arch) {
            (Os::Illumos, Arch::X86_64) => ("unknown-illumos", "x86_64"),
            (Os::Linux, Arch::X86_64) => ("unknown-linux-gnu", "x86_64"),
            (Os::Linux, Arch::Aarch64) => ("unknown-linux-gnu", "aarch64"),
            (Os::Mac, Arch::X86_64) => ("apple-darwin", "x86_64"),
            (Os::Mac, Arch::Aarch64) => ("apple-darwin", "aarch64"),
            (os, arch) => bail!("Unsupported OS/arch: {os}/{arch}"),
        };

        let tarball_filename =
            format!("cargo-hack-{supported_arch}-{platform}.tar.gz");
        let tarball_url =
            format!("{CARGO_HACK_URL}/v{version}/{tarball_filename}");

        let tarball_path = download_dir.join(&tarball_filename);

        tokio::fs::create_dir_all(&download_dir).await?;
        tokio::fs::create_dir_all(&destination_dir).await?;

        download_file_and_verify(
            &self.log,
            &tarball_path,
            &tarball_url,
            checksum,
        )
        .await?;

        unpack_tarball(&self.log, &tarball_path, &destination_dir).await?;

        Ok(())
    }

    async fn download_clickhouse(&self) -> Result<()> {
        let os = os_name()?;

        let download_dir = self.output_dir.join("downloads");
        let destination_dir = self.output_dir.join("clickhouse");

        let checksums =
            KvFile::read(self.versions_dir.join("clickhouse_checksums"))
                .await?;
        let checksum = checksums
            .get_sha256_value(&format!("CIDL_SHA256_{}", os.env_name()))?;

        let version = self.read_version_file("clickhouse_version").await?;

        const S3_BUCKET: &'static str =
            "https://oxide-clickhouse-build.s3.us-west-2.amazonaws.com";

        let platform = match os {
            Os::Illumos => "illumos",
            Os::Linux => "linux",
            Os::Mac => "macos",
        };
        let tarball_filename =
            format!("clickhouse-{version}.{platform}.tar.gz");
        let tarball_url = format!("{S3_BUCKET}/{tarball_filename}");

        let tarball_path = download_dir.join(tarball_filename);

        tokio::fs::create_dir_all(&download_dir).await?;
        tokio::fs::create_dir_all(&destination_dir).await?;

        download_file_and_verify(
            &self.log,
            &tarball_path,
            &tarball_url,
            checksum,
        )
        .await?;

        unpack_tarball(&self.log, &tarball_path, &destination_dir).await?;
        let clickhouse_binary = destination_dir.join("clickhouse");

        info!(self.log, "Checking that binary works");
        confirm_binary_works(&clickhouse_binary, &["server", "--version"])
            .await?;

        Ok(())
    }

    async fn download_cockroach(&self) -> Result<()> {
        let os = os_name()?;

        let download_dir = self.output_dir.join("downloads");
        let destination_dir = self.output_dir.join("cockroachdb");

        let checksums =
            KvFile::read(self.versions_dir.join("cockroachdb_checksums"))
                .await?;
        let commit = checksums.get_git_commit_hash_value("COCKROACH_COMMIT")?;
        let checksum = checksums
            .get_sha256_value(&format!("CIDL_SHA256_{}", os.env_name()))?;

        let build = match os {
            Os::Illumos => "illumos-amd64",
            Os::Linux => "linux-amd64",
            Os::Mac => "darwin-amd64",
        };

        let tarball = OxideBuildomatFile {
            repo: OxideRepoRevision { name: "cockroach", revision: commit },
            series: build,
            filename: "cockroach.tgz",
            sha2: checksum,
        };
        let tarball_path = download_dir.join(tarball.filename);

        tokio::fs::create_dir_all(&download_dir).await?;
        tokio::fs::create_dir_all(&destination_dir).await?;

        self.download_oxide_buildomat_file(&tarball, &tarball_path).await?;

        // We unpack the tarball in the download directory to emulate the old
        // behavior. This could be a little more consistent with Clickhouse.
        info!(self.log, "tarball path: {tarball_path}");
        unpack_tarball(&self.log, &tarball_path, &download_dir).await?;

        // This is where the binary will end up eventually
        let cockroach_binary = destination_dir.join("bin/cockroach");

        // Re-shuffle the downloaded tarball to our "destination" location.
        //
        // This ensures some uniformity, even though different platforms bundle
        // the Cockroach package differently.
        let binary_dir = destination_dir.join("bin");
        tokio::fs::create_dir_all(&binary_dir).await?;
        let src = tarball_path.with_file_name("cockroach").join("cockroach");
        tokio::fs::copy(&src, &cockroach_binary).await.with_context(|| {
            format!("Failed to copy {src} to {cockroach_binary}")
        })?;

        info!(self.log, "Checking that binary works");
        confirm_binary_works(&cockroach_binary, &["version"]).await?;

        Ok(())
    }

    async fn download_console(&self) -> Result<()> {
        let download_dir = self.output_dir.join("downloads");
        let tarball_path = download_dir.join("console.tar.gz");

        let checksums =
            KvFile::read(self.versions_dir.join("console_version")).await?;
        let commit = checksums.get_git_commit_hash_value("COMMIT")?;
        let checksum = checksums.get_sha256_value("SHA2")?;

        tokio::fs::create_dir_all(&download_dir).await?;
        let tarball_url = format!(
            "https://dl.oxide.computer/releases/console/{commit}.tar.gz"
        );
        download_file_and_verify(
            &self.log,
            &tarball_path,
            &tarball_url,
            checksum,
        )
        .await?;

        let destination_dir = self.output_dir.join("console-assets");
        let _ = tokio::fs::remove_dir_all(&destination_dir).await;
        tokio::fs::create_dir_all(&destination_dir).await?;

        unpack_tarball(&self.log, &tarball_path, &destination_dir).await?;

        Ok(())
    }

    async fn download_dendrite_stub(&self) -> Result<()> {
        let download_dir = self.output_dir.join("downloads");
        let destination_dir = self.output_dir.join("dendrite-stub");

        let stub_checksums =
            KvFile::read(self.versions_dir.join("dendrite_stub_checksums"))
                .await?;
        let sha2 = stub_checksums.get_sha256_value("CIDL_SHA256_ILLUMOS")?;
        let dpd_linux_sha2 =
            stub_checksums.get_sha256_value("CIDL_SHA256_LINUX_DPD")?;
        let swadm_linux_sha2 =
            stub_checksums.get_sha256_value("CIDL_SHA256_LINUX_SWADM")?;
        let version =
            KvFile::read(self.versions_dir.join("dendrite_version")).await?;
        let commit = version.get_git_commit_hash_value("COMMIT")?;

        let repo = OxideRepoRevision { name: "dendrite", revision: commit };
        let tarball = OxideBuildomatFile {
            repo,
            series: "image",
            filename: "dendrite-stub.tar.gz",
            sha2,
        };
        let tarball_path = download_dir.join(tarball.filename);

        tokio::fs::create_dir_all(&download_dir).await?;
        tokio::fs::create_dir_all(&destination_dir).await?;

        self.download_oxide_buildomat_file(&tarball, &tarball_path).await?;

        // Unpack in the download directory, then copy everything into the
        // destination directory.
        unpack_tarball(&self.log, &tarball_path, &download_dir).await?;

        let _ = tokio::fs::remove_dir_all(&destination_dir).await;
        tokio::fs::create_dir_all(&destination_dir).await?;
        let destination_root = destination_dir.join("root");
        tokio::fs::create_dir_all(&destination_root).await?;
        copy_dir_all(&download_dir.join("root"), &destination_root)?;

        let bin_dir = destination_dir.join("root/opt/oxide/dendrite/bin");

        // Symbolic links for backwards compatibility with existing setups
        std::os::unix::fs::symlink(
            bin_dir.canonicalize()?,
            destination_dir.canonicalize()?.join("bin"),
        )
        .context("Failed to create a symlink to dendrite's bin directory")?;

        let binaries = [
            HostNativeBinary {
                name: "dpd",
                x86_64_linux_sha2: dpd_linux_sha2,
                source_build_args: &["--features=tofino_stub"],
            },
            HostNativeBinary {
                name: "swadm",
                x86_64_linux_sha2: swadm_linux_sha2,
                source_build_args: &[],
            },
        ];
        self.install_host_native_binaries(
            repo,
            "linux-bin",
            &binaries,
            &bin_dir,
        )
        .await
    }

    async fn download_maghemite_mgd(&self) -> Result<()> {
        let download_dir = self.output_dir.join("downloads");
        tokio::fs::create_dir_all(&download_dir).await?;

        let checksums =
            KvFile::read(self.versions_dir.join("maghemite_mgd_checksums"))
                .await?;
        let mgd_sha2 = checksums.get_sha256_value("CIDL_SHA256")?;
        let mgd_linux_sha2 = checksums.get_sha256_value("MGD_LINUX_SHA256")?;
        let version = KvFile::read(
            self.versions_dir.join("maghemite_mg_openapi_version"),
        )
        .await?;
        let commit = version.get_git_commit_hash_value("COMMIT")?;

        let repo = OxideRepoRevision { name: "maghemite", revision: commit };
        let tarball = OxideBuildomatFile {
            repo,
            series: "image",
            filename: "mgd.tar.gz",
            sha2: mgd_sha2,
        };
        let tarball_path = download_dir.join(tarball.filename);
        self.download_oxide_buildomat_file(&tarball, &tarball_path).await?;
        unpack_tarball(&self.log, &tarball_path, &download_dir).await?;

        let destination_dir = self.output_dir.join("mgd");
        let _ = tokio::fs::remove_dir_all(&destination_dir).await;
        tokio::fs::create_dir_all(&destination_dir).await?;
        copy_dir_all(
            &download_dir.join("root"),
            &destination_dir.join("root"),
        )?;

        let binary_dir = destination_dir.join("root/opt/oxide/mgd/bin");

        let binaries = [HostNativeBinary {
            name: "mgd",
            x86_64_linux_sha2: mgd_linux_sha2,
            source_build_args: &["--no-default-features"],
        }];
        self.install_host_native_binaries(repo, "linux", &binaries, &binary_dir)
            .await
    }

    async fn download_maghemite_ddmd(&self) -> Result<()> {
        let download_dir = self.output_dir.join("downloads");
        tokio::fs::create_dir_all(&download_dir).await?;

        let checksums =
            KvFile::read(self.versions_dir.join("maghemite_mgd_checksums"))
                .await?;
        let mg_ddm_sha2 = checksums.get_sha256_value("MG_DDM_SHA256")?;
        let ddmd_linux_sha2 =
            checksums.get_sha256_value("DDMD_LINUX_SHA256")?;
        let version = KvFile::read(
            self.versions_dir.join("maghemite_ddm_openapi_version"),
        )
        .await?;
        let commit = version.get_git_commit_hash_value("COMMIT")?;

        let repo = OxideRepoRevision { name: "maghemite", revision: commit };
        let tarball = OxideBuildomatFile {
            repo,
            series: "image",
            filename: "mg-ddm.tar.gz",
            sha2: mg_ddm_sha2,
        };
        let tarball_path = download_dir.join(tarball.filename);
        self.download_oxide_buildomat_file(&tarball, &tarball_path).await?;
        unpack_tarball(&self.log, &tarball_path, &download_dir).await?;

        let destination_dir = self.output_dir.join("mg-ddm");
        let _ = tokio::fs::remove_dir_all(&destination_dir).await;
        tokio::fs::create_dir_all(&destination_dir).await?;
        copy_dir_all(
            &download_dir.join("root"),
            &destination_dir.join("root"),
        )?;

        let binary_dir = destination_dir.join("root/opt/oxide/mg-ddm/bin");

        let binaries = [HostNativeBinary {
            name: "ddmd",
            x86_64_linux_sha2: ddmd_linux_sha2,
            source_build_args: &["--no-default-features"],
        }];
        self.install_host_native_binaries(repo, "linux", &binaries, &binary_dir)
            .await
    }

    async fn download_softnpu(&self) -> Result<()> {
        let destination_dir = self.output_dir.join("npuzone");
        tokio::fs::create_dir_all(&destination_dir).await?;

        let checksums =
            KvFile::read(self.versions_dir.join("softnpu_version")).await?;
        let commit = checksums.get_git_commit_hash_value("COMMIT")?;
        let sha2 = checksums.get_sha256_value("SHA2")?;

        let npuzone = OxideBuildomatFile {
            repo: OxideRepoRevision { name: "softnpu", revision: commit },
            series: "image",
            filename: "npuzone",
            sha2,
        };
        let path = destination_dir.join(npuzone.filename);
        self.download_oxide_buildomat_file(&npuzone, &path).await?;
        set_permissions(&path, 0o755).await?;

        Ok(())
    }

    async fn download_transceiver_control(&self) -> Result<()> {
        let destination_dir = self.output_dir.join("transceiver-control");
        let download_dir = self.output_dir.join("downloads");
        tokio::fs::create_dir_all(&download_dir).await?;

        let version =
            KvFile::read(self.versions_dir.join("transceiver_control_version"))
                .await?;
        let commit = version.get_git_commit_hash_value("COMMIT")?;
        let sha2 = version.get_sha256_value("CIDL_SHA256_ILLUMOS")?;

        let xcvradm_gz = OxideBuildomatFile {
            repo: OxideRepoRevision {
                name: "transceiver-control",
                revision: commit,
            },
            series: "bins",
            filename: "xcvradm.gz",
            sha2,
        };
        let filename = "xcvradm";
        let gzip_path = download_dir.join(xcvradm_gz.filename);
        self.download_oxide_buildomat_file(&xcvradm_gz, &gzip_path).await?;

        let download_bin_dir = download_dir.join("root/opt/oxide/bin");
        tokio::fs::create_dir_all(&download_bin_dir).await?;
        let path = download_bin_dir.join(filename);
        unpack_gzip(&self.log, &gzip_path, &path).await?;
        set_permissions(&path, 0o755).await?;

        let _ = tokio::fs::remove_dir_all(&destination_dir).await;
        tokio::fs::create_dir_all(&destination_dir).await?;
        copy_dir_all(
            &download_dir.join("root"),
            &destination_dir.join("root"),
        )?;

        match os_name()? {
            Os::Illumos => (),
            _ => {
                let binary_dir = destination_dir.join("opt/oxide/bin");
                tokio::fs::create_dir_all(&binary_dir).await?;

                let path = binary_dir.join(filename);
                warn!(self.log, "Unsupported OS for transceiver-control - Creating stub"; "path" => %path);
                tokio::fs::write(&path, "echo 'unsupported os' && exit 1")
                    .await?;
                set_permissions(&path, 0o755).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example_path() -> Utf8PathBuf {
        Utf8PathBuf::from("tools/example_checksums")
    }

    #[test]
    fn kv_file_rejects_malformed_files() {
        let err =
            KvFile::parse(example_path(), "COMMIT=\"abc\"\n\nnot a pair\n")
                .expect_err("line without `=` is rejected");
        assert_eq!(
            format!("{err:#}"),
            "tools/example_checksums:3: expected KEY=\"value\", \
             found \"not a pair\"",
        );

        let err =
            KvFile::parse(example_path(), "COMMIT=\"abc\"\nCOMMIT=\"def\"\n")
                .expect_err("duplicate key is rejected");
        assert_eq!(
            format!("{err:#}"),
            "tools/example_checksums:2: key \"COMMIT\" is already set \
             earlier in the file",
        );
    }

    #[test]
    fn kv_file_errors_name_key_and_file() {
        let file =
            KvFile::parse(example_path(), "CIDL_SHA256_LINUX=\"abc123\"\n")
                .expect("parsed example file");

        let err = file
            .get_sha256_value("CIDL_SHA256_LINUX")
            .expect_err("truncated checksum is rejected");
        assert_eq!(
            format!("{err:#}"),
            "Invalid SHA-256 checksum \"abc123\" for CIDL_SHA256_LINUX in \
             tools/example_checksums: expected 64 hex characters, got 6",
        );

        let err = file
            .get_git_commit_hash_value("CIDL_SHA256_LINUX")
            .expect_err("abbreviated commit is rejected");
        assert_eq!(
            format!("{err:#}"),
            "Invalid git commit hash \"abc123\" for CIDL_SHA256_LINUX in \
             tools/example_checksums: expected 40 hex characters, got 6",
        );

        let err = file
            .get_string_value("COMMIT")
            .expect_err("missing key is rejected");
        assert_eq!(
            format!("{err:#}"),
            "Could not find key \"COMMIT\" in tools/example_checksums, \
             which sets [\"CIDL_SHA256_LINUX\"]",
        );
    }
}
