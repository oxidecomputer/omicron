// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility for bundling target binaries as tarfiles.

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use futures::stream::{self, StreamExt, TryStreamExt};
use illumos_utils::{zfs, zone, zpool};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use omicron_package::{parse, BuildCommand, DeployCommand};
use omicron_zone_package::config::Config as PackageConfig;
use omicron_zone_package::package::{Package, PackageOutput, PackageSource};
use omicron_zone_package::progress::Progress;
use omicron_zone_package::target::Target;
use rayon::prelude::*;
use ring::digest::{Context as DigestContext, Digest, SHA256};
use sled_hardware::cleanup::cleanup_networking_resources;
use slog::debug;
use slog::o;
use slog::Drain;
use slog::Logger;
use slog::{info, warn};
use std::env;
use std::fs::create_dir_all;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

/// All packaging subcommands.
#[derive(Debug, Subcommand)]
enum SubCommand {
    #[clap(flatten)]
    Build(BuildCommand),
    #[clap(flatten)]
    Deploy(DeployCommand),
}

// Describes the default target.
//
// NOTE: This was mostly included so that the addition of the "--target"
// flag does not break anyone's existing workflow. However, picking a default
// here is an onerous prospect, and it may make more sense to require user
// involvement. If we choose to remove it, having users pick one of a few
// "build profiles" (in other words, a curated list of target strings)
// seems like a promising alternative.
const DEFAULT_TARGET: &str =
    "image_type=standard switch_variant=stub machine_type=nongimlet";

#[derive(Debug, Parser)]
#[clap(name = "packaging tool")]
struct Args {
    /// The path to the build manifest TOML file.
    ///
    /// Defaults to "package-manifest.toml".
    #[clap(
        short,
        long,
        default_value = "package-manifest.toml",
        help = "Path to package manifest toml file",
        action
    )]
    manifest: PathBuf,

    #[clap(
        short,
        long,
        help = "Key Value pairs (of the form 'KEY=VALUE') describing the package",
        default_value = DEFAULT_TARGET,
    )]
    target: Target,

    #[clap(
        short,
        long,
        help = "Skip confirmation prompt for destructive operations",
        action,
        default_value_t = false
    )]
    force: bool,

    #[clap(subcommand)]
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
        .package_config
        .packages_to_build(&config.target)
        .into_iter()
        .filter_map(|(name, pkg)| match &pkg.source {
            PackageSource::Local { rust: Some(rust_pkg), .. } => {
                Some((name, rust_pkg.release))
            }
            _ => None,
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

async fn do_dot(config: &Config) -> Result<()> {
    println!(
        "{}",
        omicron_package::dot::do_dot(&config.target, &config.package_config)?
    );
    Ok(())
}

// Calculates the SHA256 digest for a file.
async fn get_sha256_digest(path: &PathBuf) -> Result<Digest> {
    let mut reader = BufReader::new(
        tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("could not open {path:?}"))?,
    );
    let mut context = DigestContext::new(&SHA256);
    let mut buffer = [0; 1024];

    loop {
        let count = reader
            .read(&mut buffer)
            .await
            .with_context(|| format!("failed to read {path:?}"))?;
        if count == 0 {
            break;
        } else {
            context.update(&buffer[..count]);
        }
    }
    Ok(context.finish())
}

// Ensures a package exists, either by creating it or downloading it.
async fn get_package(
    target: &Target,
    ui: &Arc<ProgressUI>,
    package_name: &String,
    package: &Package,
    output_directory: &Path,
) -> Result<()> {
    let total_work = package.get_total_work_for_target(&target)?;
    let progress = ui.add_package(package_name.to_string(), total_work);
    match &package.source {
        PackageSource::Prebuilt { repo, commit, sha256 } => {
            let expected_digest = hex::decode(&sha256)?;
            let path = package.get_output_path(package_name, &output_directory);

            let should_download = if path.exists() {
                // Re-download the package if the SHA doesn't match.
                progress.set_message("verifying hash".into());
                let digest = get_sha256_digest(&path).await?;
                digest.as_ref() != expected_digest
            } else {
                true
            };

            if should_download {
                progress.set_message("downloading prebuilt".into());
                let url = format!(
                    "https://buildomat.eng.oxide.computer/public/file/oxidecomputer/{}/image/{}/{}",
                    repo,
                    commit,
                    path.as_path().file_name().unwrap().to_string_lossy(),
                );
                let response = reqwest::Client::new()
                    .get(&url)
                    .send()
                    .await
                    .with_context(|| format!("failed to get {url}"))?;
                progress.set_length(
                    response
                        .content_length()
                        .ok_or_else(|| anyhow!("Missing Content Length"))?,
                );
                let mut file = tokio::fs::File::create(&path)
                    .await
                    .with_context(|| format!("failed to create {path:?}"))?;
                let mut stream = response.bytes_stream();
                let mut context = DigestContext::new(&SHA256);
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.with_context(|| {
                        format!("failed reading response from {url}")
                    })?;
                    // Update the running SHA digest
                    context.update(&chunk);
                    // Update the downloaded file
                    file.write_all(&chunk)
                        .await
                        .with_context(|| format!("failed writing {path:?}"))?;
                    // Record progress in the UI
                    progress.increment(chunk.len().try_into().unwrap());
                }

                let digest = context.finish();
                if digest.as_ref() != expected_digest {
                    bail!("Digest mismatch downloading {package_name}: Saw {}, expected {}", hex::encode(digest.as_ref()), hex::encode(expected_digest));
                }
            }
        }
        PackageSource::Manual => {
            let path = package.get_output_path(package_name, &output_directory);
            if !path.exists() {
                bail!(
                    "The package for {} (expected at {}) does not exist.",
                    package_name,
                    path.to_string_lossy(),
                );
            }
        }
        PackageSource::Local { .. } | PackageSource::Composite { .. } => {
            progress.set_message("bundle package".into());
            package
                .create_with_progress_for_target(&progress, &target, package_name, &output_directory)
                .await
                .with_context(|| {
                    let msg = format!("failed to create {package_name} in {output_directory:?}");
                    if let Some(hint) = &package.setup_hint {
                        format!("{msg}\nHint: {hint}")
                    } else {
                        msg
                    }
                })?;
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

    // Assemble all the non-composite packages before the composite ones.
    //
    // Since there are not (yet) composite of composite packages, we can do
    // this in a simple two-stage build.
    let (base_pkgs, composite_pkgs): (Vec<_>, Vec<_>) = config
        .package_config
        .packages_to_build(&config.target)
        .into_iter()
        .partition(|(_, pkg)| {
            !matches!(pkg.source, PackageSource::Composite { .. })
        });

    let groups = [base_pkgs, composite_pkgs];

    for packages in groups {
        let ui_refs = vec![ui.clone(); packages.len()];
        let pkg_stream = stream::iter(&packages)
            .zip(stream::iter(ui_refs))
            .map(Ok::<_, anyhow::Error>)
            .try_for_each_concurrent(
                None,
                |((package_name, package), ui)| async move {
                    get_package(
                        &config.target,
                        &ui,
                        package_name,
                        package,
                        output_directory,
                    )
                    .await
                },
            );

        pkg_stream.await?;
    }

    Ok(())
}

async fn do_unpack(
    config: &Config,
    artifact_dir: &Path,
    install_dir: &Path,
) -> Result<()> {
    create_dir_all(&install_dir).map_err(|err| {
        anyhow!("Cannot create installation directory: {}", err)
    })?;

    // Copy all packages to the install location in parallel.
    let packages = config.package_config.packages_to_deploy(&config.target);

    packages.par_iter().try_for_each(
        |(package_name, package)| -> Result<()> {
            let tarfile = package.get_output_path(&package_name, artifact_dir);
            let src = tarfile.as_path();
            let dst =
                package.get_output_path(&package.service_name, install_dir);
            info!(
                &config.log,
                "Installing service";
                "src" => %src.to_string_lossy(),
                "dst" => %dst.to_string_lossy(),
            );
            std::fs::copy(&src, &dst).map_err(|err| {
                anyhow!(
                    "Failed to copy {src} to {dst}: {err}",
                    src = src.display(),
                    dst = dst.display()
                )
            })?;
            Ok(())
        },
    )?;

    if env::var("OMICRON_NO_UNINSTALL").is_err() {
        // Ensure we start from a clean slate - remove all zones & packages.
        do_uninstall(config).await?;
    }

    // Extract all global zone services.
    let global_zone_service_names =
        packages.into_iter().filter_map(|(_, p)| match p.output {
            PackageOutput::Zone { .. } => None,
            PackageOutput::Tarball => Some(&p.service_name),
        });

    for service_name in global_zone_service_names {
        let tar_path = install_dir.join(format!("{}.tar", service_name));
        let service_path = install_dir.join(service_name);
        info!(
            &config.log,
            "Unpacking service tarball";
            "tar_path" => %tar_path.to_string_lossy(),
            "service_path" => %service_path.to_string_lossy(),
        );

        let tar_file = std::fs::File::open(&tar_path)?;
        let _ = std::fs::remove_dir_all(&service_path);
        std::fs::create_dir_all(&service_path)?;
        let mut archive = tar::Archive::new(tar_file);
        archive.unpack(&service_path)?;
    }

    Ok(())
}

fn do_activate(config: &Config, install_dir: &Path) -> Result<()> {
    // Install the bootstrap service, which itself extracts and
    // installs other services.
    if let Some(package) =
        config.package_config.packages.get("omicron-sled-agent")
    {
        let manifest_path = install_dir
            .join(&package.service_name)
            .join("pkg")
            .join("manifest.xml");
        info!(
            config.log,
            "Installing boostrap service from {}",
            manifest_path.to_string_lossy()
        );

        smf::Config::import().run(&manifest_path)?;
    }

    Ok(())
}

async fn do_install(
    config: &Config,
    artifact_dir: &Path,
    install_dir: &Path,
) -> Result<()> {
    do_unpack(config, artifact_dir, install_dir).await?;
    do_activate(config, install_dir)
}

async fn uninstall_all_omicron_zones() -> Result<()> {
    const CONCURRENCY_CAP: usize = 32;
    futures::stream::iter(zone::Zones::get().await?)
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(CONCURRENCY_CAP, |zone| async move {
            zone::Zones::halt_and_remove(zone.name()).await?;
            Ok(())
        })
        .await?;
    Ok(())
}

fn get_all_omicron_datasets() -> Result<Vec<String>> {
    let mut datasets = vec![];

    // Collect all datasets within Oxide zpools.
    //
    // This includes cockroachdb, clickhouse, and crucible datasets.
    let zpools = zpool::Zpool::list()?;
    for pool in &zpools {
        let pool = pool.to_string();
        for dataset in &zfs::Zfs::list_datasets(&pool)? {
            datasets.push(format!("{pool}/{dataset}"));
        }
    }

    // Collect all datasets for Oxide zones.
    for dataset in &zfs::Zfs::list_datasets(&zfs::ZONE_ZFS_DATASET)? {
        datasets.push(format!("{}/{dataset}", zfs::ZONE_ZFS_DATASET));
    }

    Ok(datasets)
}

fn uninstall_all_omicron_datasets(config: &Config) -> Result<()> {
    let datasets = match get_all_omicron_datasets() {
        Err(e) => {
            warn!(config.log, "Failed to get omicron datasets: {}", e);
            return Ok(());
        }
        Ok(datasets) => datasets,
    };

    if datasets.is_empty() {
        return Ok(());
    }

    config.confirm(&format!(
        "About to delete the following datasets: {:#?}",
        datasets
    ))?;
    for dataset in &datasets {
        info!(config.log, "Deleting dataset: {dataset}");
        zfs::Zfs::destroy_dataset(dataset)?;
    }

    Ok(())
}

// Attempts to both disable and delete all requested packages.
fn uninstall_all_packages(config: &Config) {
    for (_, package) in config
        .package_config
        .packages_to_deploy(&config.target)
        .into_iter()
        .filter(|(_, package)| matches!(package.output, PackageOutput::Tarball))
    {
        let _ = smf::Adm::new()
            .disable()
            .synchronous()
            .run(smf::AdmSelection::ByPattern(&[&package.service_name]));
        let _ = smf::Config::delete().force().run(&package.service_name);
    }
}

fn uninstall_omicron_config() {
    // Once all packages have been removed, also remove any locally-stored
    // configuration.
    remove_all_unless_already_removed(omicron_common::OMICRON_CONFIG_PATH)
        .unwrap();
}

fn remove_file_unless_already_removed<P: AsRef<Path>>(path: P) -> Result<()> {
    if let Err(e) = std::fs::remove_file(path.as_ref()) {
        match e.kind() {
            std::io::ErrorKind::NotFound => {}
            _ => bail!(e),
        }
    }
    Ok(())
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

fn remove_all_except<P: AsRef<Path>>(
    path: P,
    to_keep: &[&str],
    log: &Logger,
) -> Result<()> {
    let dir = match path.as_ref().read_dir() {
        Ok(dir) => dir,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => bail!(e),
    };
    for entry in dir {
        let entry = entry?;
        if to_keep.contains(&&*(entry.file_name().to_string_lossy())) {
            info!(log, "Keeping: '{}'", entry.path().to_string_lossy());
        } else {
            info!(log, "Removing: '{}'", entry.path().to_string_lossy());
            if entry.metadata()?.is_dir() {
                remove_all_unless_already_removed(entry.path())?;
            } else {
                remove_file_unless_already_removed(entry.path())?;
            }
        }
    }
    Ok(())
}

async fn do_deactivate(config: &Config) -> Result<()> {
    info!(&config.log, "Removing all Omicron zones");
    uninstall_all_omicron_zones().await?;
    info!(config.log, "Uninstalling all packages");
    uninstall_all_packages(config);
    info!(config.log, "Removing networking resources");
    cleanup_networking_resources(&config.log).await?;
    Ok(())
}

async fn do_uninstall(config: &Config) -> Result<()> {
    do_deactivate(config).await?;
    info!(config.log, "Uninstalling Omicron configuration");
    uninstall_omicron_config();
    info!(config.log, "Removing datasets");
    uninstall_all_omicron_datasets(config)?;
    Ok(())
}

async fn do_clean(
    config: &Config,
    artifact_dir: &Path,
    install_dir: &Path,
) -> Result<()> {
    do_uninstall(&config).await?;
    info!(
        config.log,
        "Removing artifacts from {}",
        artifact_dir.to_string_lossy()
    );
    const ARTIFACTS_TO_KEEP: &[&str] =
        &["clickhouse", "cockroachdb", "xde", "console-assets", "downloads"];
    remove_all_except(artifact_dir, ARTIFACTS_TO_KEEP, &config.log)?;
    info!(
        config.log,
        "Removing installed objects in: {}",
        install_dir.to_string_lossy()
    );
    const INSTALLED_OBJECTS_TO_KEEP: &[&str] = &["opte"];
    remove_all_except(install_dir, INSTALLED_OBJECTS_TO_KEEP, &config.log)?;

    Ok(())
}

fn in_progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
        )
        .expect("Invalid template")
        .progress_chars("#>.")
}

fn completed_progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg:.green}")
        .expect("Invalid template")
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
    fn set_message(&self, message: std::borrow::Cow<'static, str>) {
        self.pb.set_message(format!("{}: {}", self.service_name, message));
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

struct Config {
    log: Logger,
    // Description of all possible packages.
    package_config: PackageConfig,
    // Description of the target we're trying to operate on.
    target: Target,
    // True if we should skip confirmations for destructive operations.
    force: bool,
}

impl Config {
    /// Prompts the user for input before proceeding with an operation.
    fn confirm(&self, prompt: &str) -> Result<()> {
        if self.force {
            return Ok(());
        }

        print!("{prompt}\n[yY to confirm] >> ");
        let _ = std::io::stdout().flush();

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        match input.as_str().trim() {
            "y" | "Y" => Ok(()),
            _ => bail!("Aborting"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::try_parse()?;
    let package_config = parse::<_, PackageConfig>(&args.manifest)?;

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, o!());

    debug!(log, "target: {:?}", args.target);

    let config = Config {
        log: log.clone(),
        package_config,
        target: args.target,
        force: args.force,
    };

    // Use a CWD that is the root of the Omicron repository.
    if let Ok(manifest) = env::var("CARGO_MANIFEST_DIR") {
        let manifest_dir = PathBuf::from(manifest);
        let root = manifest_dir.parent().unwrap();
        env::set_current_dir(&root)?;
    }

    match &args.subcommand {
        SubCommand::Build(BuildCommand::Dot) => {
            do_dot(&config).await?;
        }
        SubCommand::Build(BuildCommand::Package { artifact_dir }) => {
            do_package(&config, &artifact_dir).await?;
        }
        SubCommand::Build(BuildCommand::Check) => do_check(&config).await?,
        SubCommand::Deploy(DeployCommand::Install {
            artifact_dir,
            install_dir,
        }) => {
            do_install(&config, &artifact_dir, &install_dir).await?;
        }
        SubCommand::Deploy(DeployCommand::Unpack {
            artifact_dir,
            install_dir,
        }) => {
            do_unpack(&config, &artifact_dir, &install_dir).await?;
        }
        SubCommand::Deploy(DeployCommand::Activate { install_dir }) => {
            do_activate(&config, &install_dir)?;
        }
        SubCommand::Deploy(DeployCommand::Deactivate) => {
            do_deactivate(&config).await?;
        }
        SubCommand::Deploy(DeployCommand::Uninstall) => {
            do_uninstall(&config).await?;
        }
        SubCommand::Deploy(DeployCommand::Clean {
            artifact_dir,
            install_dir,
        }) => {
            do_clean(&config, &artifact_dir, &install_dir).await?;
        }
    }

    Ok(())
}
