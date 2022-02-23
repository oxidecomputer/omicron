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
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

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
struct Package {
    // The name of the service name to be used on the target OS.
    // Also used as a lookup within the "smf/" directory.
    service_name: String,
    // A list of blobs from the Omicron build S3 bucket which should be placed
    // within this package.
    blobs: Option<Vec<PathBuf>>,
    // If present, this service is necessary for bootstrapping, and should
    // be durably installed to the target system.
    //
    // This is typically set to None - most services are bootstrapped
    // by the bootstrap agent.
    bootstrap: Option<PathBuf>,

    // Configuration for packages containing Rust binaries.
    rust: Option<RustPackage>,

    // Configuration for packages which are bundled into zone images.
    zone: Option<ZonePackage>,
}

#[derive(Deserialize, Debug)]
struct RustPackage {
    // The name of the compiled binary to be used.
    // TODO: Could be extrapolated to "produced build artifacts", we don't
    // really care about the individual binary file.
    binary_name: String,
}

impl RustPackage {
    fn binary_name(&self) -> &str {
        &self.binary_name
    }

    // Returns the path to the compiled binary.
    fn local_binary_path(&self, release: bool) -> PathBuf {
        format!(
            "target/{}/{}",
            if release { "release" } else { "debug" },
            self.binary_name()
        )
        .into()
    }

    // Builds the requested package.
    fn build(&self, package_name: &str, release: bool) -> Result<()> {
        run_cargo_on_package("build", package_name, release)
    }

    // Checks the requested package.
    fn check(&self, package_name: &str) -> Result<()> {
        run_cargo_on_package("check", package_name, false)
    }
}

#[derive(Deserialize, Debug)]
struct MappedPath {
    from: PathBuf,
    to: PathBuf,
}

#[derive(Deserialize, Debug)]
struct ZonePackage {
    paths: Vec<MappedPath>,
}

#[derive(Deserialize, Debug)]
struct Config {
    // Path to SMF manifest directory.
    smf: PathBuf,

    #[serde(default, rename = "package")]
    packages: BTreeMap<String, Package>,
}

async fn do_check(config: &Config) -> Result<()> {
    for (package_name, package) in &config.packages {
        if let Some(rust_pkg) = &package.rust {
            println!("Checking {}", package_name);
            rust_pkg.check(&package_name)?;
        }
    }
    Ok(())
}

async fn create_zone_package(
    config: &Config,
    package: &Package,
    output_directory: &Path,
    release: bool,
) -> Result<std::fs::File> {
    // Create a tarball which will become an Omicron-brand image
    // archive.
    let tarfile =
        output_directory.join(format!("{}.tar.gz", package.service_name));
    println!("Creating zone image: {}", tarfile.to_string_lossy());
    let file = open_tarfile(&tarfile)?;
    let gzw = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
    let mut archive = Builder::new(gzw);
    archive.mode(tar::HeaderMode::Deterministic);

    // The first file in the archive must always be a JSON file
    // which identifies the format of the rest of the archive.
    //
    // See the OMICRON1(5) man page for more detail.
    let mut root_json = tokio::fs::File::from_std(tempfile::tempfile()?);
    let contents = r#"{"v":"1","t":"layer"}"#;
    root_json.write_all(contents.as_bytes()).await?;
    root_json.seek(std::io::SeekFrom::Start(0)).await?;
    archive.append_file("oxide.json", &mut root_json.into_std().await)?;

    // All other files are contained under the "root" prefix.
    //
    // "path.from" exists on the machine running the package
    // command, "path.to" is an absolute path (remapped under
    // "root") which will again appear like an absolute path within
    // the namespace of the running Zone.
    if let Some(zone_pkg) = &package.zone {
        for path in &zone_pkg.paths {
            println!("Adding path: {:#?}", path);
            add_directory_and_parents(&mut archive, path.to.parent().unwrap())?;
            let dst = archive_path(&path.to)?;
            archive.append_dir_all(dst, &path.from)?;
        }
    }

    // Attempt to add the rust binary, if one was built.
    if let Some(rust_pkg) = &package.rust {
        println!("Adding rust binary");
        let dst =
            Path::new("/opt/oxide").join(&package.service_name).join("bin");
        add_directory_and_parents(&mut archive, &dst)?;
        let dst = archive_path(&dst)?;
        add_rust_binary(&rust_pkg, &mut archive, &dst, release)?;
    }

    // Add (and possibly download) blobs
    let blob_dst =
        Path::new("/opt/oxide").join(&package.service_name).join(BLOB);
    add_blobs(
        &mut archive,
        package,
        output_directory,
        &archive_path(&blob_dst)?,
    )
    .await?;

    // Add SMF directory
    let smf_dst =
        Path::new("/var/svc/manifest/site").join(&package.service_name);
    add_smf(&config, &package, &mut archive, &archive_path(&smf_dst)?)?;

    let file = archive
        .into_inner()
        .map_err(|err| anyhow!("Failed to finalize archive: {}", err))?;

    Ok(file.finish()?)
}

async fn create_tarball_package(
    config: &Config,
    package: &Package,
    output_directory: &Path,
    release: bool,
) -> Result<std::fs::File> {
    // Create a tarball containing the necessary executable and SMF
    // information.
    let tarfile =
        output_directory.join(format!("{}.tar", package.service_name));
    let file = open_tarfile(&tarfile)?;
    // Create an archive filled with:
    // - The binary
    // - The corresponding SMF directory
    //
    // TODO: We could add compression here, if we'd like?
    let mut archive = Builder::new(file);
    archive.mode(tar::HeaderMode::Deterministic);

    // Attempt to add the rust binary, if one was built.
    if let Some(rust_pkg) = &package.rust {
        add_rust_binary(&rust_pkg, &mut archive, Path::new(""), release)?;
    }

    // Add SMF directory
    add_smf(&config, &package, &mut archive, &Path::new(PKG))?;

    // Add (and possibly download) blobs
    add_blobs(&mut archive, package, output_directory, &Path::new(BLOB))
        .await?;

    let file = archive
        .into_inner()
        .map_err(|err| anyhow!("Failed to finalize archive: {}", err))?;

    Ok(file)
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
        if let Some(rust_pkg) = &package.rust {
            rust_pkg.build(&package_name, release)?;
        }

        let mut file = if package.zone.is_some() {
            create_zone_package(config, package, output_directory, release)
                .await?
        } else {
            create_tarball_package(config, package, output_directory, release)
                .await?
        };

        // Once we've created the archive, acquire a digest which can
        // later be used for verification.
        let digest = sha256_digest(&mut file)?;
        digests.insert(package.service_name.clone(), digest.as_ref().into());
    }

    let toml = toml::to_string(&digests)?;
    std::fs::write(output_directory.join("digest.toml"), &toml)?;
    Ok(())
}

// Adds blobs from S3 to the package.
//
// - `archive`: The archive to add the blobs into
// - `package`: The package being constructed
// - `download_directory`: The location to which the blobs should be downloaded
// - `destination_path`: The destination path of the blobs within the archive
async fn add_blobs<W: std::io::Write>(
    archive: &mut Builder<W>,
    package: &Package,
    download_directory: &Path,
    destination_path: &Path,
) -> Result<()> {
    if let Some(blobs) = &package.blobs {
        let blobs_path = download_directory.join(&package.service_name);
        println!("Downloading blobs to: {}", blobs_path.to_string_lossy());
        std::fs::create_dir_all(&blobs_path)?;
        for blob in blobs {
            let blob_path = blobs_path.join(blob);
            // TODO: Check against hash, download if mismatch (i.e.,
            // corruption/update).
            if !blob_path.exists() {
                download(&blob.to_string_lossy(), &blob_path).await?;
            }
        }
        archive.append_dir_all(&destination_path, &blobs_path)?;
    }
    Ok(())
}

// Helper to open a tarfile for reading/writing.
fn open_tarfile(tarfile: &Path) -> Result<std::fs::File> {
    OpenOptions::new()
        .write(true)
        .read(true)
        .truncate(true)
        .create(true)
        .open(&tarfile)
        .map_err(|err| anyhow!("Cannot create tarfile: {}", err))
}

// Adds the SMF directory for the service to the associated archive.
fn add_smf<W: std::io::Write>(
    config: &Config,
    package: &Package,
    archive: &mut tar::Builder<W>,
    dst: &Path,
) -> Result<()> {
    let smf_path = Path::new(&config.smf).join(&package.service_name);
    archive.append_dir_all(&dst, &smf_path)?;

    Ok(())
}

// Returns the path as it should be placed within an archive, by
// prepending "root/".
fn archive_path(path: &Path) -> Result<PathBuf> {
    let leading_slash = std::path::MAIN_SEPARATOR.to_string();
    Ok(Path::new("root").join(&path.strip_prefix(leading_slash)?))
}

// Adds all parent directories of a path to the archive.
//
// For example, if we wanted to insert the file into the archive:
//
// - /opt/oxide/foo/bar.txt
//
// We could call the following:
//
// ```
// let path = Path::new("/opt/oxide/foo/bar.txt");
// add_directory_and_parents(&mut archive, path.parent().unwrap());
// ```
//
// Which would add the following directories to the archive:
//
// - /root
// - /root/opt
// - /root/opt/oxide
// - /root/opt/oxide/foo
fn add_directory_and_parents<W: std::io::Write>(
    archive: &mut tar::Builder<W>,
    to: &Path,
) -> Result<()> {
    let mut parents: Vec<&Path> = to.ancestors().collect::<Vec<&Path>>();
    parents.reverse();

    for parent in parents {
        println!("Adding path to archive: {}", parent.to_string_lossy());
        let dst = archive_path(&parent)?;
        archive.append_dir(&dst, ".")?;
        println!("Added path to archive: {}", parent.to_string_lossy());
    }

    Ok(())
}

// Adds a rust binary to the archive.
//
// - `package`: The package being constructed
// - `archive`: The archive to which the binary should be added
// - `dst_directory`: The path where the binary should be added in the archive
// - `release`: True if the binary was built in "release" mod
fn add_rust_binary<W: std::io::Write>(
    rust_pkg: &RustPackage,
    archive: &mut tar::Builder<W>,
    dst_directory: &Path,
    release: bool,
) -> Result<()> {
    archive
        .append_path_with_name(
            rust_pkg.local_binary_path(release),
            dst_directory.join(&rust_pkg.binary_name),
        )
        .map_err(|err| anyhow!("Cannot append binary to tarfile: {}", err))?;
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
    let packages: Vec<(&String, &Package)> = config.packages.iter().collect();
    packages.into_par_iter().try_for_each(|(_, package)| -> Result<()> {
        let tarfile = match package.zone {
            Some(_) => {
                artifact_dir.join(format!("{}.tar.gz", package.service_name))
            }
            None => artifact_dir.join(format!("{}.tar", package.service_name)),
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
        if let Some(_) = &package.zone {
            // TODO: At the moment, zones are entirely managed by the sled
            // agent.
        } else {
            let _ = smf::Adm::new()
                .disable()
                .synchronous()
                .run(smf::AdmSelection::ByPattern(&[&package.service_name]));
            let _ = smf::Config::delete().force().run(&package.service_name);
        }
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
