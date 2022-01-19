// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Utility for bundling target binaries as tarfiles.
 */

use omicron_common::packaging::sha256_digest;

use anyhow::{anyhow, bail, Context, Result};
use omicron_package::{parse, SubCommand};
use rayon::prelude::*;
use reqwest;
use serde_derive::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs::{create_dir_all, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::Command;
use structopt::StructOpt;
use tar::Builder;
use tokio::io::AsyncWriteExt;

const S3_BUCKET: &str = "https://oxide-omicron-build.s3.amazonaws.com";

// Name for the directory component where additional packaged files are stored.
const PKG: &str = "pkg";
// Name for the directory component where downloaded blobs are stored.
const BLOB: &str = "blob";

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

fn run_cargo_on_package(
    subcmd: &str,
    package: &str,
    release: bool,
) -> Result<()> {
    let mut cmd = Command::new("cargo");
    // We rely on the rust-toolchain.toml file for toolchain information,
    // rather than specifying one within the packaging tool.
    cmd.arg(subcmd).arg("-p").arg(package);
    if release {
        cmd.arg("--release");
    }
    let status =
        cmd.status().context(format!("Failed to run command: ({:?})", cmd))?;
    if !status.success() {
        bail!("Failed to build package: {}", package);
    }

    Ok(())
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Build {
    /// The package is a rust binary, which should be compiled and extracted.
    Rust,
}

#[derive(Deserialize, Debug)]
struct PackageInfo {
    // The name of the compiled binary to be used.
    // TODO: Could be extrapolated to "produced build artifacts", we don't
    // really care about the individual binary file.
    binary_name: String,
    // The name of the service name to be used on the target OS.
    // Also used as a lookup within the "smf/" directory.
    service_name: String,
    // Instructs how this package should be compiled.
    build: Build,
    // A list of blobs from the Omicron build S3 bucket which should be placed
    // within this package.
    blobs: Option<Vec<PathBuf>>,
    // If present, this service is necessary for bootstrapping, and should
    // be durably installed to the target system.
    //
    // This is typically set to None - most services are bootstrapped
    // by the bootstrap agent.
    bootstrap: Option<PathBuf>,
}

impl PackageInfo {
    fn binary_name(&self) -> &str {
        &self.binary_name
    }

    // Returns the path to the compiled binary.
    fn binary_path(&self, release: bool) -> PathBuf {
        match &self.build {
            Build::Rust => format!(
                "target/{}/{}",
                if release { "release" } else { "debug" },
                self.binary_name()
            )
            .into(),
        }
    }

    // Builds the requested package.
    fn build(&self, package_name: &str, release: bool) -> Result<()> {
        match &self.build {
            Build::Rust => run_cargo_on_package("build", package_name, release),
        }
    }

    // Checks the requested package.
    fn check(&self, package_name: &str) -> Result<()> {
        match &self.build {
            Build::Rust => run_cargo_on_package("check", package_name, false),
        }
    }
}

#[derive(Deserialize, Debug)]
struct Config {
    // Path to SMF manifest directory.
    smf: PathBuf,

    #[serde(default, rename = "package")]
    packages: BTreeMap<String, PackageInfo>,
}

async fn do_check(config: &Config) -> Result<()> {
    for (package_name, package) in &config.packages {
        println!("Checking {}", package_name);
        package.check(&package_name)?;
    }
    Ok(())
}

async fn do_package(
    config: &Config,
    output_directory: &Path,
    release: bool,
) -> Result<()> {
    // Create the output directory, if it does not already exist.
    create_dir_all(&output_directory)
        .map_err(|err| anyhow!("Cannot create output directory: {}", err))?;

    // As we emit each package bundle, capture their digests for
    // verification purposes.
    let mut digests = HashMap::<String, Vec<u8>>::new();

    for (package_name, package) in &config.packages {
        println!("Building {}", package_name);
        package.build(&package_name, release)?;

        let tarfile =
            output_directory.join(format!("{}.tar", package.service_name));
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .truncate(true)
            .create(true)
            .open(&tarfile)
            .map_err(|err| anyhow!("Cannot create tarfile: {}", err))?;

        // Create an archive filled with:
        // - The binary
        // - The corresponding SMF directory
        //
        // TODO: We could add compression here, if we'd like?
        let mut archive = Builder::new(file);
        archive.mode(tar::HeaderMode::Deterministic);

        // Add binary
        archive
            .append_path_with_name(
                package.binary_path(release),
                &package.binary_name,
            )
            .map_err(|err| {
                anyhow!("Cannot append binary to tarfile: {}", err)
            })?;

        // Add SMF directory
        let smf_path: PathBuf = format!(
            "{}/{}",
            config.smf.to_string_lossy(),
            package.service_name
        )
        .into();
        add_path_to_archive(&mut archive, &smf_path, Path::new(PKG))?;

        // Add (and possibly download) blobs
        add_blobs(&mut archive, package_name, package, output_directory)
            .await?;

        let mut file = archive
            .into_inner()
            .map_err(|err| anyhow!("Failed to finalize archive: {}", err))?;

        // Once we've created the archive, acquire a digest which can
        // later be used for verification.
        let digest = sha256_digest(&mut file)?;
        digests.insert(package.binary_name().into(), digest.as_ref().into());
    }

    let toml = toml::to_string(&digests)?;
    std::fs::write(output_directory.join("digest.toml"), &toml)?;
    Ok(())
}

// Adds all files within "path" to "archive".
//
// Within the archive, all files are renamed to "dst_prefix/<file_name>".
fn add_path_to_archive(
    archive: &mut Builder<std::fs::File>,
    path: &Path,
    dst_prefix: &Path,
) -> Result<()> {
    for entry in walkdir::WalkDir::new(&path) {
        let entry =
            entry.map_err(|err| anyhow!("Cannot access entry: {}", err))?;
        if entry.file_name().to_string_lossy().starts_with('.') {
            // Omit hidden files - we don't want to include text editor
            // artifacts in the tarfile.
            continue;
        }
        let dst = dst_prefix.join(entry.path().strip_prefix(&path)?);
        println!(
            "Archiving {} -> {}",
            entry.path().to_string_lossy(),
            dst.to_string_lossy()
        );
        archive
            .append_path_with_name(entry.path(), dst)
            .map_err(|err| anyhow!("Cannot append file to tarfile: {}", err))?;
    }
    Ok(())
}

async fn add_blobs(
    archive: &mut Builder<std::fs::File>,
    name: &String,
    package: &PackageInfo,
    output_directory: &Path,
) -> Result<()> {
    if let Some(blobs) = &package.blobs {
        let blobs_path = output_directory.join(name);
        std::fs::create_dir_all(&blobs_path)?;
        for blob in blobs {
            let blob_path = blobs_path.join(blob);
            // TODO: Check against hash, download if mismatch (i.e.,
            // corruption/update).
            if !blob_path.exists() {
                download(&blob.to_string_lossy(), &blob_path).await?;
            }
        }
        add_path_to_archive(archive, blobs_path.as_path(), Path::new(BLOB))?;
    }
    Ok(())
}

// Downloads "source" from S3_BUCKET to "destination".
async fn download(source: &str, destination: &Path) -> Result<()> {
    println!("Downloading {} to {}", source, destination.to_string_lossy());
    let response = reqwest::get(format!("{}/{}", S3_BUCKET, source)).await?;
    let mut file = tokio::fs::File::create(destination).await?;
    file.write_all(&response.bytes().await?).await?;
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

    println!(
        "Copying digest.toml from {} to {}",
        artifact_dir.to_string_lossy(),
        install_dir.to_string_lossy()
    );
    // Move the digest of expected packages.
    std::fs::copy(
        artifact_dir.join("digest.toml"),
        install_dir.join("digest.toml"),
    )?;

    // Copy all packages to the install location in parallel.
    let packages: Vec<(&String, &PackageInfo)> =
        config.packages.iter().collect();
    packages.into_par_iter().try_for_each(|(_, package)| -> Result<()> {
        let tarfile =
            artifact_dir.join(format!("{}.tar", package.service_name));
        let src = tarfile.as_path();
        let dst = install_dir.join(src.strip_prefix(artifact_dir)?);
        println!(
            "Installing {} -> {}",
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
    for (_, package) in &config.packages {
        if let Some(manifest) = &package.bootstrap {
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

            let mut manifest_path = install_dir.to_path_buf();
            manifest_path.push(&package.service_name);
            manifest_path.push(PKG);
            manifest_path.push(manifest);
            println!(
                "Installing bootstrap service from {}",
                manifest_path.to_string_lossy()
            );
            smf::Config::import().run(&manifest_path)?;
        }
    }

    Ok(())
}

// Attempts to both disable and delete all requested packages.
fn uninstall_all_packages(config: &Config) {
    for package in config.packages.values() {
        let _ = smf::Adm::new()
            .disable()
            .synchronous()
            .run(smf::AdmSelection::ByPattern(&[&package.service_name]));
        let _ = smf::Config::delete().force().run(&package.service_name);
    }
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
        SubCommand::Package { artifact_dir, release } => {
            do_package(&config, &artifact_dir, *release).await?;
        }
        SubCommand::Check => do_check(&config).await?,
        SubCommand::Install { artifact_dir, install_dir } => {
            do_install(&config, &artifact_dir, &install_dir)?;
        }
        SubCommand::Uninstall { artifact_dir, install_dir } => {
            do_uninstall(&config, &artifact_dir, &install_dir)?;
        }
    }

    Ok(())
}
