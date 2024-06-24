// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask download

use anyhow::{bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use clap::ValueEnum;
use flate2::bufread::GzDecoder;
use futures::StreamExt;
use sha2::Digest;
use slog::{info, o, warn, Drain, Logger};
use std::collections::{BTreeSet, HashMap};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::sync::OnceLock;
use std::time::Duration;
use strum::EnumIter;
use strum::IntoEnumIterator;
use tar::Archive;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

const BUILDOMAT_URL: &'static str =
    "https://buildomat.eng.oxide.computer/public/file";
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

    /// Clickhouse binary
    Clickhouse,

    /// CockroachDB binary
    Cockroach,

    /// Web console assets
    Console,

    /// Dendrite OpenAPI spec
    DendriteOpenapi,

    /// Stub Dendrite binary tarball
    DendriteStub,

    /// Maghemite mgd binary
    MaghemiteMgd,

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
                    Target::Clickhouse => downloader.download_clickhouse().await,
                    Target::Cockroach => downloader.download_cockroach().await,
                    Target::Console => downloader.download_console().await,
                    Target::DendriteOpenapi => {
                        downloader.download_dendrite_openapi().await
                    }
                    Target::DendriteStub => downloader.download_dendrite_stub().await,
                    Target::MaghemiteMgd => downloader.download_maghemite_mgd().await,
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

enum Os {
    Illumos,
    Linux,
    Mac,
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
}

/// Parses a file of the format:
///
/// ```ignore
/// KEY1="value1"
/// KEY2="value2"
/// ```
///
/// And returns an array of the values in the same order as keys.
async fn get_values_from_file<const N: usize>(
    keys: [&str; N],
    path: &Utf8Path,
) -> Result<[String; N]> {
    // Map of "key" => "Position in output".
    let mut keys: HashMap<&str, usize> =
        keys.into_iter().enumerate().map(|(i, s)| (s, i)).collect();

    const EMPTY_STRING: String = String::new();
    let mut values = [EMPTY_STRING; N];

    let content = tokio::fs::read_to_string(&path)
        .await
        .context("Failed to read {path}")?;
    for line in content.lines() {
        let line = line.trim();
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim_matches('"');
        if let Some(i) = keys.remove(key) {
            values[i] = value.to_string();
        }
    }
    if !keys.is_empty() {
        bail!("Could not find keys: {:?}", keys.keys().collect::<Vec<_>>(),);
    }
    Ok(values)
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
    Ok(())
}

/// Returns the hex, lowercase md5 checksum of a file at `path`.
async fn md5_checksum(path: &Utf8Path) -> Result<String> {
    let mut buf = vec![0u8; 65536];
    let mut file = tokio::fs::File::open(path).await?;
    let mut ctx = md5::Context::new();
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        ctx.write_all(&buf[0..n])?;
    }

    let digest = ctx.compute();
    Ok(format!("{digest:x}"))
}

/// Returns the hex, lowercase sha2 checksum of a file at `path`.
async fn sha2_checksum(path: &Utf8Path) -> Result<String> {
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

    let digest = ctx.finalize();
    Ok(format!("{digest:x}"))
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

async fn clickhouse_confirm_binary_works(binary: &Utf8Path) -> Result<()> {
    let mut cmd = Command::new(binary);
    cmd.args(["server", "--version"]);

    let output =
        cmd.output().await.context(format!("Failed to run {binary}"))?;
    if !output.status.success() {
        let stderr =
            String::from_utf8(output.stderr).unwrap_or_else(|_| String::new());
        bail!("{binary} failed: {} (stderr: {stderr})", output.status);
    }
    Ok(())
}

async fn cockroach_confirm_binary_works(binary: &Utf8Path) -> Result<()> {
    let mut cmd = Command::new(binary);
    cmd.arg("version");

    let output =
        cmd.output().await.context(format!("Failed to run {binary}"))?;
    if !output.status.success() {
        let stderr =
            String::from_utf8(output.stderr).unwrap_or_else(|_| String::new());
        bail!("{binary} failed: {} (stderr: {stderr})", output.status);
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

enum ChecksumAlgorithm {
    Md5,
    Sha2,
}

impl ChecksumAlgorithm {
    async fn checksum(&self, path: &Utf8Path) -> Result<String> {
        match self {
            ChecksumAlgorithm::Md5 => md5_checksum(path).await,
            ChecksumAlgorithm::Sha2 => sha2_checksum(path).await,
        }
    }
}

/// Downloads a file and verifies the checksum.
///
/// If the file already exists and the checksum matches,
/// avoids performing the download altogether.
async fn download_file_and_verify(
    log: &Logger,
    path: &Utf8Path,
    url: &str,
    algorithm: ChecksumAlgorithm,
    checksum: &str,
) -> Result<()> {
    let do_download = if path.exists() {
        info!(log, "Already downloaded ({path})");
        if algorithm.checksum(&path).await? == checksum {
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

    let observed_checksum = algorithm.checksum(&path).await?;
    if observed_checksum != checksum {
        bail!(
            "Checksum mismatch (saw {observed_checksum}, expected {checksum})"
        );
    }
    Ok(())
}

impl<'a> Downloader<'a> {
    async fn download_clickhouse(&self) -> Result<()> {
        let os = os_name()?;

        let download_dir = self.output_dir.join("downloads");
        let destination_dir = self.output_dir.join("clickhouse");

        let checksums_path = self.versions_dir.join("clickhouse_checksums");
        let [checksum] = get_values_from_file(
            [&format!("CIDL_MD5_{}", os.env_name())],
            &checksums_path,
        )
        .await?;

        let versions_path = self.versions_dir.join("clickhouse_version");
        let version = tokio::fs::read_to_string(&versions_path)
            .await
            .context("Failed to read version from {versions_path}")?;
        let version = version.trim();

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
            ChecksumAlgorithm::Md5,
            &checksum,
        )
        .await?;

        unpack_tarball(&self.log, &tarball_path, &destination_dir).await?;
        let clickhouse_binary = destination_dir.join("clickhouse");

        info!(self.log, "Checking that binary works");
        clickhouse_confirm_binary_works(&clickhouse_binary).await?;

        Ok(())
    }

    async fn download_cockroach(&self) -> Result<()> {
        let os = os_name()?;

        let download_dir = self.output_dir.join("downloads");
        let destination_dir = self.output_dir.join("cockroachdb");

        let checksums_path = self.versions_dir.join("cockroachdb_checksums");
        let [checksum] = get_values_from_file(
            [&format!("CIDL_SHA256_{}", os.env_name())],
            &checksums_path,
        )
        .await?;

        let versions_path = self.versions_dir.join("cockroachdb_version");
        let version = tokio::fs::read_to_string(&versions_path)
            .await
            .context("Failed to read version from {versions_path}")?;
        let version = version.trim();

        let (url_base, suffix) = match os {
            Os::Illumos => ("https://illumos.org/downloads", "tar.gz"),
            Os::Linux | Os::Mac => ("https://binaries.cockroachdb.com", "tgz"),
        };
        let build = match os {
            Os::Illumos => "illumos",
            Os::Linux => "linux-amd64",
            Os::Mac => "darwin-10.9-amd64",
        };

        let version_directory = format!("cockroach-{version}");
        let tarball_name = format!("{version_directory}.{build}");
        let tarball_filename = format!("{tarball_name}.{suffix}");
        let tarball_url = format!("{url_base}/{tarball_filename}");

        let tarball_path = download_dir.join(tarball_filename);

        tokio::fs::create_dir_all(&download_dir).await?;
        tokio::fs::create_dir_all(&destination_dir).await?;

        download_file_and_verify(
            &self.log,
            &tarball_path,
            &tarball_url,
            ChecksumAlgorithm::Sha2,
            &checksum,
        )
        .await?;

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
        match os {
            Os::Illumos => {
                let src = tarball_path.with_file_name(version_directory);
                let dst = &destination_dir;
                info!(self.log, "Copying from {src} to {dst}");
                copy_dir_all(&src, &dst)?;
            }
            Os::Linux | Os::Mac => {
                let src =
                    tarball_path.with_file_name(tarball_name).join("cockroach");
                tokio::fs::copy(src, &cockroach_binary).await?;
            }
        }

        info!(self.log, "Checking that binary works");
        cockroach_confirm_binary_works(&cockroach_binary).await?;

        Ok(())
    }

    async fn download_console(&self) -> Result<()> {
        let download_dir = self.output_dir.join("downloads");
        let tarball_path = download_dir.join("console.tar.gz");

        let checksums_path = self.versions_dir.join("console_version");
        let [commit, checksum] =
            get_values_from_file(["COMMIT", "SHA2"], &checksums_path).await?;

        tokio::fs::create_dir_all(&download_dir).await?;
        let tarball_url = format!(
            "https://dl.oxide.computer/releases/console/{commit}.tar.gz"
        );
        download_file_and_verify(
            &self.log,
            &tarball_path,
            &tarball_url,
            ChecksumAlgorithm::Sha2,
            &checksum,
        )
        .await?;

        let destination_dir = self.output_dir.join("console-assets");
        let _ = tokio::fs::remove_dir_all(&destination_dir).await;
        tokio::fs::create_dir_all(&destination_dir).await?;

        unpack_tarball(&self.log, &tarball_path, &destination_dir).await?;

        Ok(())
    }

    async fn download_dendrite_openapi(&self) -> Result<()> {
        let download_dir = self.output_dir.join("downloads");

        let checksums_path = self.versions_dir.join("dendrite_openapi_version");
        let [commit, checksum] =
            get_values_from_file(["COMMIT", "SHA2"], &checksums_path).await?;

        let url = format!(
            "{BUILDOMAT_URL}/oxidecomputer/dendrite/openapi/{commit}/dpd.json"
        );
        let path = download_dir.join(format!("dpd-{commit}.json"));

        tokio::fs::create_dir_all(&download_dir).await?;
        download_file_and_verify(
            &self.log,
            &path,
            &url,
            ChecksumAlgorithm::Sha2,
            &checksum,
        )
        .await?;

        Ok(())
    }

    async fn download_dendrite_stub(&self) -> Result<()> {
        let download_dir = self.output_dir.join("downloads");
        let destination_dir = self.output_dir.join("dendrite-stub");

        let stub_checksums_path =
            self.versions_dir.join("dendrite_stub_checksums");

        // NOTE: This seems odd to me -- the "dendrite_openapi_version" file also
        // contains a SHA2, but we're ignoring it?
        //
        // Regardless, this is currenlty the one that actually matches, regardless
        // of host OS.
        let [sha2, dpd_sha2, swadm_sha2] = get_values_from_file(
            [
                "CIDL_SHA256_ILLUMOS",
                "CIDL_SHA256_LINUX_DPD",
                "CIDL_SHA256_LINUX_SWADM",
            ],
            &stub_checksums_path,
        )
        .await?;
        let checksums_path = self.versions_dir.join("dendrite_openapi_version");
        let [commit, _sha2] =
            get_values_from_file(["COMMIT", "SHA2"], &checksums_path).await?;

        let tarball_file = "dendrite-stub.tar.gz";
        let tarball_path = download_dir.join(tarball_file);
        let repo = "oxidecomputer/dendrite";
        let url_base = format!("{BUILDOMAT_URL}/{repo}/image/{commit}");

        tokio::fs::create_dir_all(&download_dir).await?;
        tokio::fs::create_dir_all(&destination_dir).await?;

        download_file_and_verify(
            &self.log,
            &tarball_path,
            &format!("{url_base}/{tarball_file}"),
            ChecksumAlgorithm::Sha2,
            &sha2,
        )
        .await?;

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

        match os_name()? {
            Os::Linux => {
                let base_url =
                    format!("{BUILDOMAT_URL}/{repo}/linux-bin/{commit}");
                let filename = "dpd";
                let path = download_dir.join(filename);
                download_file_and_verify(
                    &self.log,
                    &path,
                    &format!("{base_url}/{filename}"),
                    ChecksumAlgorithm::Sha2,
                    &dpd_sha2,
                )
                .await?;
                set_permissions(&path, 0o755).await?;
                tokio::fs::copy(path, bin_dir.join(filename)).await?;

                let filename = "swadm";
                let path = download_dir.join(filename);
                download_file_and_verify(
                    &self.log,
                    &path,
                    &format!("{base_url}/{filename}"),
                    ChecksumAlgorithm::Sha2,
                    &swadm_sha2,
                )
                .await?;
                set_permissions(&path, 0o755).await?;
                tokio::fs::copy(path, bin_dir.join(filename)).await?;
            }
            Os::Illumos => {}
            Os::Mac => {
                warn!(self.log, "WARNING: Dendrite not available for Mac");
                warn!(self.log, "Network APIs will be unavailable");

                let path = bin_dir.join("dpd");
                tokio::fs::write(&path, "echo 'unsupported os' && exit 1")
                    .await?;
                set_permissions(&path, 0o755).await?;
            }
        }

        Ok(())
    }

    async fn download_maghemite_mgd(&self) -> Result<()> {
        let download_dir = self.output_dir.join("downloads");
        tokio::fs::create_dir_all(&download_dir).await?;

        let checksums_path = self.versions_dir.join("maghemite_mgd_checksums");
        let [mgd_sha2, mgd_linux_sha2] = get_values_from_file(
            ["CIDL_SHA256", "MGD_LINUX_SHA256"],
            &checksums_path,
        )
        .await?;
        let commit_path =
            self.versions_dir.join("maghemite_mg_openapi_version");
        let [commit] = get_values_from_file(["COMMIT"], &commit_path).await?;

        let repo = "oxidecomputer/maghemite";
        let base_url = format!("{BUILDOMAT_URL}/{repo}/image/{commit}");

        let filename = "mgd.tar.gz";
        let tarball_path = download_dir.join(filename);
        download_file_and_verify(
            &self.log,
            &tarball_path,
            &format!("{base_url}/{filename}"),
            ChecksumAlgorithm::Sha2,
            &mgd_sha2,
        )
        .await?;
        unpack_tarball(&self.log, &tarball_path, &download_dir).await?;

        let destination_dir = self.output_dir.join("mgd");
        let _ = tokio::fs::remove_dir_all(&destination_dir).await;
        tokio::fs::create_dir_all(&destination_dir).await?;
        copy_dir_all(
            &download_dir.join("root"),
            &destination_dir.join("root"),
        )?;

        let binary_dir = destination_dir.join("root/opt/oxide/mgd/bin");

        match os_name()? {
            Os::Linux => {
                let filename = "mgd";
                let path = download_dir.join(filename);
                download_file_and_verify(
                    &self.log,
                    &path,
                    &format!(
                        "{BUILDOMAT_URL}/{repo}/linux/{commit}/{filename}"
                    ),
                    ChecksumAlgorithm::Sha2,
                    &mgd_linux_sha2,
                )
                .await?;
                set_permissions(&path, 0o755).await?;
                tokio::fs::copy(path, binary_dir.join(filename)).await?;
            }
            _ => (),
        }

        Ok(())
    }

    async fn download_softnpu(&self) -> Result<()> {
        let destination_dir = self.output_dir.join("npuzone");
        tokio::fs::create_dir_all(&destination_dir).await?;

        let checksums_path = self.versions_dir.join("softnpu_version");
        let [commit, sha2] =
            get_values_from_file(["COMMIT", "SHA2"], &checksums_path).await?;

        let repo = "oxidecomputer/softnpu";

        let filename = "npuzone";
        let base_url = format!("{BUILDOMAT_URL}/{repo}/image/{commit}");
        let artifact_url = format!("{base_url}/{filename}");

        let path = destination_dir.join(filename);
        download_file_and_verify(
            &self.log,
            &path,
            &artifact_url,
            ChecksumAlgorithm::Sha2,
            &sha2,
        )
        .await?;
        set_permissions(&path, 0o755).await?;

        Ok(())
    }

    async fn download_transceiver_control(&self) -> Result<()> {
        let destination_dir = self.output_dir.join("transceiver-control");
        let download_dir = self.output_dir.join("downloads");
        tokio::fs::create_dir_all(&download_dir).await?;

        let [commit, sha2] = get_values_from_file(
            ["COMMIT", "CIDL_SHA256_ILLUMOS"],
            &self.versions_dir.join("transceiver_control_version"),
        )
        .await?;

        let repo = "oxidecomputer/transceiver-control";
        let base_url = format!("{BUILDOMAT_URL}/{repo}/bins/{commit}");

        let filename_gz = "xcvradm.gz";
        let filename = "xcvradm";
        let gzip_path = download_dir.join(filename_gz);
        download_file_and_verify(
            &self.log,
            &gzip_path,
            &format!("{base_url}/{filename_gz}"),
            ChecksumAlgorithm::Sha2,
            &sha2,
        )
        .await?;

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
