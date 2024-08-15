// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility for bundling target binaries as tarfiles.

use anyhow::{anyhow, bail, ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand};
use futures::stream::{self, StreamExt, TryStreamExt};
use illumos_utils::{zfs, zone};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use omicron_package::target::KnownTarget;
use omicron_package::{parse, BuildCommand, DeployCommand, TargetCommand};
use omicron_zone_package::config::{Config as PackageConfig, PackageMap};
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
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::create_dir_all;
use std::io::Write;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
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

fn parse_duration_ms(arg: &str) -> Result<std::time::Duration> {
    let ms = arg.parse()?;
    Ok(std::time::Duration::from_millis(ms))
}

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
    manifest: Utf8PathBuf,

    #[clap(
        short,
        long,
        help = "The name of the build target to use for this command",
        default_value_t = ACTIVE.to_string(),
    )]
    target: String,

    /// The output directory, where artifacts should be built and staged
    #[clap(long = "artifacts", default_value = "out/")]
    artifact_dir: Utf8PathBuf,

    #[clap(
        short,
        long,
        help = "Skip confirmation prompt for destructive operations",
        action,
        default_value_t = false
    )]
    force: bool,

    #[clap(
        long,
        help = "Number of retries to use when re-attempting failed package downloads",
        action,
        default_value_t = 10
    )]
    retry_count: usize,

    #[clap(
        long,
        help = "Duration, in ms, to wait before re-attempting failed package downloads",
        action,
        value_parser = parse_duration_ms,
        default_value = "1000",
    )]
    retry_duration: std::time::Duration,

    #[clap(subcommand)]
    subcommand: SubCommand,
}

#[derive(Debug, Default)]
struct CargoPlan<'a> {
    command: &'a str,
    bins: BTreeSet<&'a String>,
    features: BTreeSet<&'a String>,
    release: bool,
}

impl<'a> CargoPlan<'a> {
    async fn run(&self, log: &Logger) -> Result<()> {
        if self.bins.is_empty() {
            return Ok(());
        }

        let mut cmd = Command::new("cargo");
        // We rely on the rust-toolchain.toml file for toolchain information,
        // rather than specifying one within the packaging tool.
        cmd.arg(self.command);
        for bin in &self.bins {
            cmd.arg("--bin").arg(bin);
        }
        if !self.features.is_empty() {
            cmd.arg("--features").arg(self.features.iter().fold(
                String::new(),
                |mut acc, s| {
                    if !acc.is_empty() {
                        acc.push(' ');
                    }
                    acc.push_str(s);
                    acc
                },
            ));
        }
        if self.release {
            cmd.arg("--release");
        }
        info!(log, "running: {:?}", cmd.as_std());
        let status = cmd
            .status()
            .await
            .context(format!("Failed to run command: ({:?})", cmd))?;
        if !status.success() {
            bail!("Failed to build packages");
        }

        Ok(())
    }
}

async fn do_for_all_rust_packages(
    config: &Config,
    command: &str,
) -> Result<()> {
    // Collect a map of all of the workspace packages
    let workspace = cargo_metadata::MetadataCommand::new().no_deps().exec()?;
    let workspace_pkgs = workspace
        .packages
        .into_iter()
        .filter_map(|package| {
            workspace
                .workspace_members
                .contains(&package.id)
                .then_some((package.name.clone(), package))
        })
        .collect::<BTreeMap<_, _>>();

    // Generate a list of all features we might want to request
    let features = config
        .target
        .0
        .iter()
        .map(|(name, value)| format!("{name}-{value}"))
        .collect::<Vec<_>>();

    // We split the packages to be built into "release" and "debug" lists
    let mut release =
        CargoPlan { command, release: true, ..Default::default() };
    let mut debug = CargoPlan { command, release: false, ..Default::default() };

    for (name, pkg) in config.packages_to_build().0 {
        // If this is a Rust package...
        if let PackageSource::Local { rust: Some(rust_pkg), .. } = &pkg.source {
            let plan = if rust_pkg.release { &mut release } else { &mut debug };
            // Get the package metadata
            let metadata = workspace_pkgs.get(name).with_context(|| {
                format!("package '{name}' is not a workspace package")
            })?;
            // Add the binaries we want to build to the plan
            let bins = metadata
                .targets
                .iter()
                .filter_map(|target| target.is_bin().then_some(&target.name))
                .collect::<BTreeSet<_>>();
            for bin in &rust_pkg.binary_names {
                ensure!(
                    bins.contains(bin),
                    "bin target '{bin}' does not belong to package '{name}'"
                );
                plan.bins.insert(bin);
            }
            // Add all features we want to request to the plan
            plan.features.extend(
                features
                    .iter()
                    .filter(|feature| metadata.features.contains_key(*feature)),
            );
        }
    }

    release.run(&config.log).await?;
    debug.run(&config.log).await?;
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

async fn do_list_outputs(
    config: &Config,
    output_directory: &Utf8Path,
    intermediate: bool,
) -> Result<()> {
    for (name, package) in config.packages_to_build().0 {
        if !intermediate
            && package.output
                == (PackageOutput::Zone { intermediate_only: true })
        {
            continue;
        }
        println!("{}", package.get_output_path(name, output_directory));
    }
    Ok(())
}

// The name reserved for the currently-in-use build target.
const ACTIVE: &str = "active";

async fn do_target(
    artifact_dir: &Utf8Path,
    name: &str,
    subcommand: &TargetCommand,
) -> Result<()> {
    let target_dir = artifact_dir.join("target");
    tokio::fs::create_dir_all(&target_dir).await.with_context(|| {
        format!("failed to create directory {}", target_dir)
    })?;
    match subcommand {
        TargetCommand::Create {
            image,
            machine,
            switch,
            rack_topology,
            clickhouse_topology,
        } => {
            let target = KnownTarget::new(
                image.clone(),
                machine.clone(),
                switch.clone(),
                rack_topology.clone(),
                clickhouse_topology
                    .clone()
                    .expect("clickhouse topology should never be NONE"),
            )?;

            let path = get_single_target(&target_dir, name).await?;
            tokio::fs::write(&path, Target::from(target).to_string())
                .await
                .with_context(|| {
                    format!("failed to write target to {}", path)
                })?;

            replace_active_link(&name, &target_dir).await?;

            println!("Created new build target '{name}' and set it as active");
        }
        TargetCommand::List => {
            let active = tokio::fs::read_link(target_dir.join(ACTIVE)).await?;
            let active = Utf8PathBuf::try_from(active)?;
            for entry in walkdir::WalkDir::new(&target_dir)
                .max_depth(1)
                .sort_by_file_name()
            {
                let entry = entry?;
                if entry.file_type().is_file() {
                    let contents =
                        tokio::fs::read_to_string(entry.path()).await?;
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    let status = if active == name {
                        "SELECTED >>> "
                    } else {
                        "             "
                    };
                    println!("{status}{name}: {contents}");
                }
            }
        }
        TargetCommand::Set => {
            let _ = get_single_target(&target_dir, name).await?;
            replace_active_link(&name, &target_dir).await?;
            println!("Set build target '{name}' as active");
        }
        TargetCommand::Delete => {
            let path = get_single_target(&target_dir, name).await?;
            tokio::fs::remove_file(&path).await?;
            println!("Removed build target '{name}'");
        }
    };
    Ok(())
}

async fn get_single_target(
    target_dir: impl AsRef<Utf8Path>,
    name: &str,
) -> Result<Utf8PathBuf> {
    if name == ACTIVE {
        bail!(
            "The name '{name}' is reserved, please try another (e.g. 'default')\n\
            Usage: '{} -t <TARGET> target ...'",
            env::current_exe().unwrap().display(),
        );
    }
    Ok(target_dir.as_ref().join(name))
}

async fn replace_active_link(
    src: impl AsRef<Utf8Path>,
    target_dir: impl AsRef<Utf8Path>,
) -> Result<()> {
    let src = src.as_ref();
    let target_dir = target_dir.as_ref();

    let dst = target_dir.join(ACTIVE);
    if !target_dir.join(src).exists() {
        bail!("Target file {} does not exist", src);
    }
    let _ = tokio::fs::remove_file(&dst).await;
    tokio::fs::symlink(src, &dst).await.with_context(|| {
        format!("failed creating symlink to {} at {}", src, dst)
    })?;
    Ok(())
}

// Calculates the SHA256 digest for a file.
async fn get_sha256_digest(path: &Utf8PathBuf) -> Result<Digest> {
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

async fn download_prebuilt(
    progress: &PackageProgress,
    package_name: &str,
    repo: &str,
    commit: &str,
    expected_digest: &Vec<u8>,
    path: &Utf8Path,
) -> Result<()> {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

    progress.set_message("downloading prebuilt".into());
    let url = format!(
        "https://buildomat.eng.oxide.computer/public/file/oxidecomputer/{}/image/{}/{}",
        repo,
        commit,
        path.file_name().unwrap(),
    );
    let client = CLIENT.get_or_init(|| {
        reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(3600))
            .tcp_keepalive(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(15))
            .build()
            .unwrap()
    });
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("failed to get {url}"))?;
    progress.increment_total(
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
        let chunk = chunk
            .with_context(|| format!("failed reading response from {url}"))?;
        // Update the running SHA digest
        context.update(&chunk);
        // Update the downloaded file
        file.write_all(&chunk)
            .await
            .with_context(|| format!("failed writing {path:?}"))?;
        // Record progress in the UI
        progress.increment_completed(chunk.len().try_into().unwrap());
    }

    let digest = context.finish();
    if digest.as_ref() == expected_digest {
        Ok(())
    } else {
        Err(anyhow!("Failed validating download of {url}").context(format!(
            "Digest mismatch on {package_name}: Saw {}, expected {}",
            hex::encode(digest.as_ref()),
            hex::encode(expected_digest)
        )))
    }
}

// Ensures a package exists, either by creating it or downloading it.
async fn ensure_package(
    config: &Config,
    ui: &Arc<ProgressUI>,
    package_name: &String,
    package: &Package,
    output_directory: &Utf8Path,
    disable_cache: bool,
) -> Result<()> {
    let target = &config.target;
    let progress = ui.add_package(package_name.to_string());
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
                let mut attempts_left = config.retry_count + 1;
                loop {
                    match download_prebuilt(
                        &progress,
                        package_name,
                        repo,
                        commit,
                        &expected_digest,
                        path.as_path(),
                    )
                    .await
                    {
                        Ok(()) => break,
                        Err(err) => {
                            attempts_left -= 1;
                            let msg = format!("Failed to download prebuilt ({attempts_left} attempts remaining)");
                            progress.set_error_message(msg.into());
                            if attempts_left == 0 {
                                return Err(err);
                            }
                            tokio::time::sleep(config.retry_duration).await;
                            progress.reset();
                        }
                    }
                }
            }
        }
        PackageSource::Manual => {
            progress.set_message("confirming manual package".into());
            let path = package.get_output_path(package_name, &output_directory);
            if !path.exists() {
                bail!(
                    "The package for {} (expected at {}) does not exist.",
                    package_name,
                    path,
                );
            }
        }
        PackageSource::Local { .. } | PackageSource::Composite { .. } => {
            progress.set_message("building package".into());

            let build_config = omicron_zone_package::package::BuildConfig {
                target,
                progress: &progress,
                cache_disabled: disable_cache,
            };
            package
                .create(package_name, &output_directory, &build_config)
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

async fn do_package(
    config: &Config,
    output_directory: &Utf8Path,
    disable_cache: bool,
) -> Result<()> {
    create_dir_all(&output_directory)
        .map_err(|err| anyhow!("Cannot create output directory: {}", err))?;

    let ui = ProgressUI::new(&config.log);

    do_build(&config).await?;

    let packages = config.packages_to_build();

    let package_iter = packages.build_order();
    for batch in package_iter {
        let ui_refs = vec![ui.clone(); batch.len()];
        let pkg_stream = stream::iter(batch)
            .zip(stream::iter(ui_refs))
            .map(Ok::<_, anyhow::Error>)
            .try_for_each_concurrent(
                None,
                |((package_name, package), ui)| async move {
                    ensure_package(
                        &config,
                        &ui,
                        package_name,
                        package,
                        output_directory,
                        disable_cache,
                    )
                    .await
                },
            );

        pkg_stream.await?;
    }

    Ok(())
}

async fn do_stamp(
    config: &Config,
    output_directory: &Utf8Path,
    package_name: &str,
    version: &semver::Version,
) -> Result<()> {
    // Find the package which should be stamped
    let (_name, package) = config
        .package_config
        .packages_to_deploy(&config.target)
        .0
        .into_iter()
        .find(|(name, _pkg)| name.as_str() == package_name)
        .ok_or_else(|| anyhow!("Package {package_name} not found"))?;

    // Stamp it
    let stamped_path =
        package.stamp(package_name, output_directory, version).await?;
    println!("Created: {}", stamped_path);
    Ok(())
}

async fn do_unpack(
    config: &Config,
    artifact_dir: &Utf8Path,
    install_dir: &Utf8Path,
) -> Result<()> {
    create_dir_all(&install_dir).map_err(|err| {
        anyhow!("Cannot create installation directory: {}", err)
    })?;

    // Copy all packages to the install location in parallel.
    let packages = config.package_config.packages_to_deploy(&config.target).0;

    packages.par_iter().try_for_each(
        |(package_name, package)| -> Result<()> {
            let tarfile = package.get_output_path(&package_name, artifact_dir);
            let src = tarfile.as_path();
            let dst =
                package.get_output_path(&package.service_name, install_dir);
            info!(
                &config.log,
                "Installing service";
                "src" => %src,
                "dst" => %dst,
            );
            std::fs::copy(&src, &dst).map_err(|err| {
                anyhow!(
                    "Failed to copy {src} to {dst}: {err}",
                    src = src,
                    dst = dst
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
            "tar_path" => %tar_path,
            "service_path" => %service_path,
        );

        let tar_file = std::fs::File::open(&tar_path)?;
        let _ = std::fs::remove_dir_all(&service_path);
        std::fs::create_dir_all(&service_path)?;
        let mut archive = tar::Archive::new(tar_file);
        archive.unpack(&service_path)?;
    }

    Ok(())
}

fn do_activate(config: &Config, install_dir: &Utf8Path) -> Result<()> {
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
            "Installing bootstrap service from {}", manifest_path
        );

        smf::Config::import().run(&manifest_path)?;
    }

    Ok(())
}

async fn do_install(
    config: &Config,
    artifact_dir: &Utf8Path,
    install_dir: &Utf8Path,
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

fn uninstall_all_omicron_datasets(config: &Config) -> Result<()> {
    let datasets = match zfs::get_all_omicron_datasets_for_delete() {
        Err(e) => {
            warn!(config.log, "Failed to get omicron datasets: {}", e);
            return Err(e);
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
        .0
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

fn remove_file_unless_already_removed<P: AsRef<Utf8Path>>(
    path: P,
) -> Result<()> {
    if let Err(e) = std::fs::remove_file(path.as_ref()) {
        match e.kind() {
            std::io::ErrorKind::NotFound => {}
            _ => bail!(e),
        }
    }
    Ok(())
}

fn remove_all_unless_already_removed<P: AsRef<Utf8Path>>(
    path: P,
) -> Result<()> {
    if let Err(e) = std::fs::remove_dir_all(path.as_ref()) {
        match e.kind() {
            std::io::ErrorKind::NotFound => {}
            _ => bail!(e),
        }
    }
    Ok(())
}

fn remove_all_except<P: AsRef<Utf8Path>>(
    path: P,
    to_keep: &[&str],
    log: &Logger,
) -> Result<()> {
    let dir = match path.as_ref().read_dir_utf8() {
        Ok(dir) => dir,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => bail!(e),
    };
    for entry in dir {
        let entry = entry?;
        if to_keep.contains(&entry.file_name()) {
            info!(log, "Keeping: '{}'", entry.path());
        } else {
            info!(log, "Removing: '{}'", entry.path());
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
    info!(config.log, "Removing datasets");
    uninstall_all_omicron_datasets(config)?;
    Ok(())
}

async fn do_clean(
    config: &Config,
    artifact_dir: &Utf8Path,
    install_dir: &Utf8Path,
) -> Result<()> {
    do_uninstall(&config).await?;
    info!(config.log, "Removing artifacts from {}", artifact_dir);
    const ARTIFACTS_TO_KEEP: &[&str] = &[
        "clickhouse",
        "cockroachdb",
        "xde",
        "console-assets",
        "downloads",
        "softnpu",
    ];
    remove_all_except(artifact_dir, ARTIFACTS_TO_KEEP, &config.log)?;
    info!(config.log, "Removing installed objects in: {}", install_dir);
    const INSTALLED_OBJECTS_TO_KEEP: &[&str] = &["opte"];
    remove_all_except(install_dir, INSTALLED_OBJECTS_TO_KEEP, &config.log)?;

    Ok(())
}

fn in_progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("[{elapsed:>3}] {bar:30.cyan/blue} {pos:>7}/{len:7} {msg}")
        .expect("Invalid template")
        .progress_chars("#>.")
}

fn completed_progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "[{elapsed:>3}] {bar:30.cyan/blue} {pos:>7}/{len:7} {msg:.green}",
        )
        .expect("Invalid template")
        .progress_chars("#>.")
}

fn error_progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "[{elapsed:>3}] {bar:30.cyan/blue} {pos:>7}/{len:7} {msg:.red}",
        )
        .expect("Invalid template")
        .progress_chars("#>.")
}

// Struct managing display of progress to UI.
struct ProgressUI {
    log: Logger,
    multi: MultiProgress,
    style: ProgressStyle,
}

impl ProgressUI {
    fn new(log: &Logger) -> Arc<Self> {
        Arc::new(Self {
            log: log.clone(),
            multi: MultiProgress::new(),
            style: in_progress_style(),
        })
    }

    fn add_package(&self, service_name: String) -> PackageProgress {
        let pb = self.multi.add(ProgressBar::new(1));
        pb.set_style(self.style.clone());
        pb.set_message(service_name.clone());
        pb.tick();
        PackageProgress::new(&self.log, pb, service_name)
    }
}

struct PackageProgress {
    log: Logger,
    pb: ProgressBar,
    service_name: String,
}

impl PackageProgress {
    fn new(log: &Logger, pb: ProgressBar, service_name: String) -> Self {
        Self {
            log: log.new(o!("package" => service_name.clone())),
            pb,
            service_name,
        }
    }

    fn finish(&self) {
        self.pb.set_style(completed_progress_style());
        self.pb.finish_with_message(format!("{}: done", self.service_name));
        self.pb.tick();
    }

    fn set_error_message(&self, message: std::borrow::Cow<'static, str>) {
        self.pb.set_style(error_progress_style());
        let message = format!("{}: {}", self.service_name, message);
        warn!(self.log, "{}", &message);
        self.pb.set_message(message);
        self.pb.tick();
    }

    fn reset(&self) {
        self.pb.reset();
    }
}

impl Progress for PackageProgress {
    fn set_message(&self, message: std::borrow::Cow<'static, str>) {
        self.pb.set_style(in_progress_style());
        let message = format!("{}: {}", self.service_name, message);
        info!(self.log, "{}", &message);
        self.pb.set_message(message);
        self.pb.tick();
    }

    fn get_log(&self) -> &Logger {
        &self.log
    }

    fn increment_total(&self, delta: u64) {
        self.pb.inc_length(delta);
    }

    fn increment_completed(&self, delta: u64) {
        self.pb.inc(delta);
    }
}

struct Config {
    log: Logger,
    // Description of all possible packages.
    package_config: PackageConfig,
    // Description of the target we're trying to operate on.
    target: Target,
    // The list of packages the user wants us to build (all, if empty)
    only: Vec<String>,
    // True if we should skip confirmations for destructive operations.
    force: bool,
    // Number of times to retry failed downloads.
    retry_count: usize,
    // Duration to wait before retrying failed downloads.
    retry_duration: std::time::Duration,
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

    /// Returns target packages to be assembled on the builder machine, limited
    /// to those specified in `only` (if set).
    fn packages_to_build(&self) -> PackageMap<'_> {
        let packages = self.package_config.packages_to_build(&self.target);
        if self.only.is_empty() {
            return packages;
        }

        let mut filtered_packages = PackageMap(BTreeMap::new());
        let mut to_walk = PackageMap(BTreeMap::new());
        // add the requested packages to `to_walk`
        for package_name in &self.only {
            to_walk.0.insert(
                package_name,
                packages.0.get(package_name).unwrap_or_else(|| {
                    panic!(
                        "Explicitly-requested package '{}' does not exist",
                        package_name
                    )
                }),
            );
        }
        // dependencies are listed by output name, so create a lookup table to
        // get a package by its output name.
        let lookup_by_output = packages
            .0
            .iter()
            .map(|(name, package)| {
                (package.get_output_file(name), (*name, *package))
            })
            .collect::<BTreeMap<_, _>>();
        // packages yet to be walked are added to `to_walk`. pop each entry and
        // add its dependencies to `to_walk`, then add the package we finished
        // walking to `filtered_packages`.
        while let Some((package_name, package)) = to_walk.0.pop_first() {
            if let PackageSource::Composite { packages } = &package.source {
                for output in packages {
                    // find the package by output name
                    let (dep_name, dep_package) =
                        lookup_by_output.get(output).unwrap_or_else(|| {
                            panic!(
                                "Could not find a package which creates '{}'",
                                output
                            )
                        });
                    if dep_name.as_str() == package_name {
                        panic!("'{}' depends on itself", package_name);
                    }
                    // if we've seen this package already, it will be in
                    // `filtered_packages`. otherwise, add it to `to_walk`.
                    if !filtered_packages.0.contains_key(dep_name) {
                        to_walk.0.insert(dep_name, dep_package);
                    }
                }
            }
            // we're done looking at this package's deps
            filtered_packages.0.insert(package_name, package);
        }
        filtered_packages
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::try_parse()?;
    let package_config = parse::<_, PackageConfig>(&args.manifest)?;

    let mut open_options = std::fs::OpenOptions::new();
    open_options.write(true).create(true).truncate(true);
    tokio::fs::create_dir_all(&args.artifact_dir).await?;
    let logpath = args.artifact_dir.join("LOG");
    let logfile = std::io::LineWriter::new(open_options.open(&logpath)?);
    eprintln!("Logging to: {}", std::fs::canonicalize(logpath)?.display());

    let drain = slog_bunyan::new(logfile).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!());

    let target_help_str = || -> String {
        format!(
            "Try calling: '{} -t default target create' to create a new build target",
            env::current_exe().unwrap().display()
        )
    };

    let get_config = || -> Result<Config> {
        let target_path = args.artifact_dir.join("target").join(&args.target);
        let raw_target =
            std::fs::read_to_string(&target_path).map_err(|e| {
                eprintln!(
                    "Failed to read build target: {}\n{}",
                    target_path,
                    target_help_str()
                );
                e
            })?;
        let target: Target = KnownTarget::from_str(&raw_target)
            .map_err(|e| {
                eprintln!(
                    "Failed to parse {} as target\n{}",
                    target_path,
                    target_help_str()
                );
                e
            })?
            .into();
        debug!(log, "target[{}]: {:?}", args.target, target);

        Ok(Config {
            log: log.clone(),
            package_config,
            target,
            only: Vec::new(),
            force: args.force,
            retry_count: args.retry_count,
            retry_duration: args.retry_duration,
        })
    };

    // Use a CWD that is the root of the Omicron repository.
    if let Ok(manifest) = env::var("CARGO_MANIFEST_DIR") {
        let manifest_dir = Utf8PathBuf::from(manifest);
        let root = manifest_dir.parent().unwrap();
        env::set_current_dir(root).with_context(|| {
            format!("failed to set current directory to {}", root)
        })?;
    }

    match args.subcommand {
        SubCommand::Build(BuildCommand::Target { subcommand }) => {
            do_target(&args.artifact_dir, &args.target, &subcommand).await?;
        }
        SubCommand::Build(BuildCommand::Dot) => {
            do_dot(&get_config()?).await?;
        }
        SubCommand::Build(BuildCommand::ListOutputs { intermediate }) => {
            do_list_outputs(&get_config()?, &args.artifact_dir, intermediate)
                .await?;
        }
        SubCommand::Build(BuildCommand::Package { disable_cache, only }) => {
            let mut config = get_config()?;
            config.only = only;
            do_package(&config, &args.artifact_dir, disable_cache).await?;
        }
        SubCommand::Build(BuildCommand::Stamp { package_name, version }) => {
            do_stamp(
                &get_config()?,
                &args.artifact_dir,
                &package_name,
                &version,
            )
            .await?;
        }
        SubCommand::Build(BuildCommand::Check) => {
            do_check(&get_config()?).await?
        }
        SubCommand::Deploy(DeployCommand::Install { install_dir }) => {
            do_install(&get_config()?, &args.artifact_dir, &install_dir)
                .await?;
        }
        SubCommand::Deploy(DeployCommand::Unpack { install_dir }) => {
            do_unpack(&get_config()?, &args.artifact_dir, &install_dir).await?;
        }
        SubCommand::Deploy(DeployCommand::Activate { install_dir }) => {
            do_activate(&get_config()?, &install_dir)?;
        }
        SubCommand::Deploy(DeployCommand::Deactivate) => {
            do_deactivate(&get_config()?).await?;
        }
        SubCommand::Deploy(DeployCommand::Uninstall) => {
            do_uninstall(&get_config()?).await?;
        }
        SubCommand::Deploy(DeployCommand::Clean { install_dir }) => {
            do_clean(&get_config()?, &args.artifact_dir, &install_dir).await?;
        }
    }

    Ok(())
}
