// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility for bundling target binaries as tarfiles.

use anyhow::{anyhow, bail, Context, Result};
use futures::stream::{self, StreamExt, TryStreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use omicron_package::{parse, BuildCommand, DeployCommand};
use omicron_zone_package::package::{Package, Progress};
use rayon::prelude::*;
use ring::digest::{Context as DigestContext, Digest, SHA256};
use serde_derive::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use structopt::StructOpt;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

/// Describes the origin of an externally-built package.
#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ExternalPackageSource {
    /// Downloads the package from the following URL:
    ///
    /// <https://buildomat.eng.oxide.computer/public/file/oxidecomputer/REPO/image/COMMIT/PACKAGE>
    Prebuilt { repo: String, commit: String, sha256: String },
    /// Expects that a package will be manually built and placed into the output
    /// directory.
    Manual,
}

/// Describes a package which originates from outside this repo.
#[derive(Deserialize, Debug)]
struct ExternalPackage {
    #[serde(flatten)]
    package: Package,

    source: ExternalPackageSource,
}

/// Describes the configuration for a set of packages.
#[derive(Deserialize, Debug)]
struct Config {
    /// Packages to be built and installed.
    #[serde(default, rename = "package")]
    packages: BTreeMap<String, Package>,

    /// Packages to be installed, but which have been created outside this
    /// repository.
    #[serde(default, rename = "external_package")]
    external_packages: BTreeMap<String, ExternalPackage>,
}

/// All packaging subcommands.
#[derive(Debug, StructOpt)]
enum SubCommand {
    #[structopt(flatten)]
    Build(BuildCommand),
    #[structopt(flatten)]
    Deploy(DeployCommand),
}

#[derive(Debug, StructOpt)]
#[structopt(name = "packaging tool")]
struct Args {
    /// The path to the build manifest TOML file.
    ///
    /// Defaults to "package-manifest.toml".
    #[structopt(
        short,
        long,
        default_value = "package-manifest.toml",
        help = "Path to package manifest toml file"
    )]
    manifest: PathBuf,

    #[structopt(subcommand)]
    subcommand: SubCommand,
}

async fn run_cargo_on_packages<I, S>(
    subcmd: &str,
    packages: I,
    release: bool,
) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let mut cmd = Command::new("cargo");
    // We rely on the rust-toolchain.toml file for toolchain information,
    // rather than specifying one within the packaging tool.
    cmd.arg(subcmd);
    for package in packages {
        cmd.arg("-p").arg(package);
    }
    if release {
        cmd.arg("--release");
    }
    let status = cmd
        .status()
        .await
        .context(format!("Failed to run command: ({:?})", cmd))?;
    if !status.success() {
        bail!("Failed to build packages");
    }

    Ok(())
}

async fn do_for_all_rust_packages(
    config: &Config,
    command: &str,
) -> Result<()> {
    // First, filter out all Rust packages from the configuration that should be
    // built, and partition them into "release" and "debug" categories.
    let (release_pkgs, debug_pkgs): (Vec<_>, _) = config
        .packages
        .iter()
        .filter_map(|(name, pkg)| {
            pkg.rust.as_ref().map(|rust_pkg| (name, rust_pkg.release))
        })
        .partition(|(_, release)| *release);

    // Execute all the release / debug packages at the same time.
    if !release_pkgs.is_empty() {
        run_cargo_on_packages(
            command,
            release_pkgs.iter().map(|(name, _)| name),
            true,
        )
        .await?;
    }
    if !debug_pkgs.is_empty() {
        run_cargo_on_packages(
            command,
            debug_pkgs.iter().map(|(name, _)| name),
            false,
        )
        .await?;
    }
    Ok(())
}

async fn do_check(config: &Config) -> Result<()> {
    do_for_all_rust_packages(config, "check").await
}

async fn do_build(config: &Config) -> Result<()> {
    do_for_all_rust_packages(config, "build").await
}

// Calculates the SHA256 digest for a file.
async fn get_sha256_digest(path: &PathBuf) -> Result<Digest> {
    let mut reader = BufReader::new(tokio::fs::File::open(&path).await?);
    let mut context = DigestContext::new(&SHA256);
    let mut buffer = [0; 1024];

    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            break;
        } else {
            context.update(&buffer[..count]);
        }
    }
    Ok(context.finish())
}

// Accesses a package which is contructed outside of Omicron.
async fn get_external_package(
    ui: &Arc<ProgressUI>,
    package_name: &String,
    external_package: &ExternalPackage,
    output_directory: &Path,
) -> Result<()> {
    let progress = ui.add_package(package_name.to_string(), 1);
    match &external_package.source {
        ExternalPackageSource::Prebuilt { repo, commit, sha256 } => {
            let expected_digest = hex::decode(&sha256)?;
            let path =
                external_package.package.get_output_path(&output_directory);

            let should_download = if path.exists() {
                // Re-download the package if the SHA doesn't match.
                progress.set_message("verifying hash".to_string());
                let digest = get_sha256_digest(&path).await?;
                digest.as_ref() != expected_digest
            } else {
                true
            };

            if should_download {
                progress.set_message("downloading prebuilt".to_string());
                let url = format!(
                    "https://buildomat.eng.oxide.computer/public/file/oxidecomputer/{}/image/{}/{}",
                    repo,
                    commit,
                    path.as_path().file_name().unwrap().to_string_lossy(),
                );
                let response = reqwest::Client::new().get(url).send().await?;
                progress.set_length(
                    response
                        .content_length()
                        .ok_or_else(|| anyhow!("Missing Content Length"))?,
                );
                let mut file = tokio::fs::File::create(path).await?;
                let mut stream = response.bytes_stream();
                let mut context = DigestContext::new(&SHA256);
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    // Update the running SHA digest
                    context.update(&chunk);
                    // Update the downloaded file
                    file.write_all(&chunk).await?;
                    // Record progress in the UI
                    progress.increment(chunk.len().try_into().unwrap());
                }

                let digest = context.finish();
                if digest.as_ref() != expected_digest {
                    bail!("Digest mismatch downloading {}", package_name);
                }
            }
        }
        ExternalPackageSource::Manual => {
            let path =
                external_package.package.get_output_path(&output_directory);
            if !path.exists() {
                bail!(
                    "The package for {} (expected at {}) does not exist.",
                    package_name,
                    path.to_string_lossy(),
                );
            }
        }
    }
    progress.finish();
    Ok(())
}

async fn do_package(config: &Config, output_directory: &Path) -> Result<()> {
    create_dir_all(&output_directory)
        .map_err(|err| anyhow!("Cannot create output directory: {}", err))?;

    let ui = ProgressUI::new();

    do_build(&config).await?;

    let ui_refs = vec![ui.clone(); config.external_packages.len()];
    let external_pkg_stream = stream::iter(&config.external_packages)
        // It's a pain to clone a value into closures - see
        // https://github.com/rust-lang/rfcs/issues/2407 - so in the meantime,
        // we explicitly create the references to the UI we need for each
        // package.
        .zip(stream::iter(ui_refs))
        // We convert the stream type to operate on Results, so we may invoke
        // "try_for_each_concurrent" more easily.
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(
            None,
            |((package_name, package), ui)| async move {
                get_external_package(
                    &ui,
                    package_name,
                    package,
                    output_directory,
                )
                .await
            },
        );

    let ui_refs = vec![ui.clone(); config.packages.len()];
    let internal_pkg_stream = stream::iter(&config.packages)
        .zip(stream::iter(ui_refs))
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(
            None,
            |((package_name, package), ui)| async move {
                let total_work = package.get_total_work();
                let progress =
                    ui.add_package(package_name.to_string(), total_work);
                progress.set_message("bundle package".to_string());
                package
                    .create_with_progress(&progress, &output_directory)
                    .await?;
                progress.finish();
                Ok(())
            },
        );

    tokio::task::spawn_blocking(move || ui.multi.join());
    tokio::try_join!(external_pkg_stream, internal_pkg_stream)?;

    Ok(())
}

fn do_install(
    config: &Config,
    artifact_dir: &Path,
    install_dir: &Path,
) -> Result<()> {
    create_dir_all(&install_dir).map_err(|err| {
        anyhow!("Cannot create installation directory: {}", err)
    })?;

    // Copy all packages to the install location in parallel.
    let packages: Vec<(&String, &Package)> = config
        .packages
        .iter()
        .chain(
            config
                .external_packages
                .iter()
                .map(|(name, epkg)| (name, &epkg.package)),
        )
        .collect();
    packages.into_par_iter().try_for_each(|(_, package)| -> Result<()> {
        let tarfile = if package.zone {
            artifact_dir.join(format!("{}.tar.gz", package.service_name))
        } else {
            artifact_dir.join(format!("{}.tar", package.service_name))
        };

        let src = tarfile.as_path();
        let dst = install_dir.join(src.strip_prefix(artifact_dir)?);
        println!(
            "Installing Service: {} -> {}",
            src.to_string_lossy(),
            dst.to_string_lossy()
        );
        std::fs::copy(&src, &dst)?;
        Ok(())
    })?;

    // Ensure we start from a clean slate - remove all packages.
    uninstall_all_packages(config);

    // Extract and install the bootstrap service, which itself extracts and
    // installs other services.
    if let Some(package) = config.packages.get("omicron-sled-agent") {
        let tar_path =
            install_dir.join(format!("{}.tar", package.service_name));
        let service_path = install_dir.join(&package.service_name);
        println!(
            "Unpacking {} to {}",
            tar_path.to_string_lossy(),
            service_path.to_string_lossy()
        );

        let tar_file = std::fs::File::open(&tar_path)?;
        let _ = std::fs::remove_dir_all(&service_path);
        std::fs::create_dir_all(&service_path)?;
        let mut archive = tar::Archive::new(tar_file);
        archive.unpack(&service_path)?;

        let manifest_path = install_dir
            .join(&package.service_name)
            .join("pkg")
            .join("manifest.xml");
        println!(
            "Installing bootstrap service from {}",
            manifest_path.to_string_lossy()
        );
        smf::Config::import().run(&manifest_path)?;
    }

    Ok(())
}

// Attempts to both disable and delete all requested packages.
fn uninstall_all_packages(config: &Config) {
    for package in config
        .packages
        .values()
        .chain(config.external_packages.values().map(|epkg| &epkg.package))
    {
        if package.zone {
            // TODO(https://github.com/oxidecomputer/omicron/issues/723):
            // At the moment, zones are entirely managed by the sled agent,
            // but could be removed here.
        } else {
            let _ = smf::Adm::new()
                .disable()
                .synchronous()
                .run(smf::AdmSelection::ByPattern(&[&package.service_name]));
            let _ = smf::Config::delete().force().run(&package.service_name);
        }
    }

    // Once all packages have been removed, also remove any locally-stored
    // configuration.
    remove_all_unless_already_removed(omicron_common::OMICRON_CONFIG_PATH)
        .unwrap();
}

fn remove_all_unless_already_removed<P: AsRef<Path>>(path: P) -> Result<()> {
    if let Err(e) = std::fs::remove_dir_all(path.as_ref()) {
        match e.kind() {
            std::io::ErrorKind::NotFound => {}
            _ => bail!(e),
        }
    }
    Ok(())
}

fn do_uninstall(
    config: &Config,
    artifact_dir: &Path,
    install_dir: &Path,
) -> Result<()> {
    println!("Uninstalling all packages");
    uninstall_all_packages(config);
    println!("Removing: {}", artifact_dir.to_string_lossy());
    remove_all_unless_already_removed(artifact_dir)?;
    println!("Removing: {}", install_dir.to_string_lossy());
    remove_all_unless_already_removed(install_dir)?;
    Ok(())
}

fn in_progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
        )
        .progress_chars("#>.")
}

fn completed_progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg:.green}")
        .progress_chars("#>.")
}

// Struct managing display of progress to UI.
struct ProgressUI {
    multi: MultiProgress,
    style: ProgressStyle,
}

struct PackageProgress {
    pb: ProgressBar,
    service_name: String,
}

impl PackageProgress {
    fn finish(&self) {
        self.pb.set_style(completed_progress_style());
        self.pb.finish_with_message(format!("{}: done", self.service_name));
        self.pb.tick();
    }

    fn set_length(&self, total: u64) {
        self.pb.set_length(total);
    }
}

impl Progress for PackageProgress {
    fn set_message(&self, message: impl Into<std::borrow::Cow<'static, str>>) {
        self.pb.set_message(format!(
            "{}: {}",
            self.service_name,
            message.into()
        ));
        self.pb.tick();
    }

    fn increment(&self, delta: u64) {
        self.pb.inc(delta);
    }
}

impl ProgressUI {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            multi: MultiProgress::new(),
            style: in_progress_style(),
        })
    }

    fn add_package(&self, service_name: String, total: u64) -> PackageProgress {
        let pb = self.multi.add(ProgressBar::new(total));
        pb.set_style(self.style.clone());
        pb.set_message(service_name.clone());
        pb.tick();
        PackageProgress { pb, service_name }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::from_args_safe().map_err(|err| anyhow!(err))?;
    let config = parse::<_, Config>(&args.manifest)?;

    // Use a CWD that is the root of the Omicron repository.
    if let Ok(manifest) = env::var("CARGO_MANIFEST_DIR") {
        let manifest_dir = PathBuf::from(manifest);
        let root = manifest_dir.parent().unwrap();
        env::set_current_dir(&root)?;
    }

    match &args.subcommand {
        SubCommand::Build(BuildCommand::Package { artifact_dir }) => {
            do_package(&config, &artifact_dir).await?;
        }
        SubCommand::Build(BuildCommand::Check) => do_check(&config).await?,
        SubCommand::Deploy(DeployCommand::Install {
            artifact_dir,
            install_dir,
        }) => {
            do_install(&config, &artifact_dir, &install_dir)?;
        }
        SubCommand::Deploy(DeployCommand::Uninstall {
            artifact_dir,
            install_dir,
        }) => {
            do_uninstall(&config, &artifact_dir, &install_dir)?;
        }
    }

    Ok(())
}
