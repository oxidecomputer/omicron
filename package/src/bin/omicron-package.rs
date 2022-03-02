// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Utility for bundling target binaries as tarfiles.
 */

use anyhow::{anyhow, bail, Context, Result};
use omicron_package::{parse, SubCommand};
use omicron_zone_package::package::Package;
use rayon::prelude::*;
use serde_derive::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::process::Command;
use structopt::StructOpt;

/// Describes the configuration for a set of packages.
#[derive(Deserialize, Debug)]
pub struct Config {
    /// Packages to be built and installed.
    #[serde(default, rename = "package")]
    pub packages: BTreeMap<String, Package>,

    /// Packages to be installed, but which have been created outside this
    /// repository.
    #[serde(default, rename = "external_package")]
    pub external_packages: BTreeMap<String, Package>,
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

async fn do_check(config: &Config) -> Result<()> {
    for (package_name, package) in &config.packages {
        if let Some(rust_pkg) = &package.rust {
            println!("Checking {}", package_name);
            run_cargo_on_package("check", &package_name, rust_pkg.release)?;
        }
    }
    Ok(())
}

async fn do_package(config: &Config, output_directory: &Path) -> Result<()> {
    create_dir_all(&output_directory)
        .map_err(|err| anyhow!("Cannot create output directory: {}", err))?;

    for (package_name, package) in &config.packages {
        if let Some(rust_pkg) = &package.rust {
            println!("Building: {}", package_name);
            run_cargo_on_package("build", package_name, rust_pkg.release)?;
        }
        package.create(&output_directory).await?;
    }

    for (package_name, package) in &config.external_packages {
        let path = package.get_output_path(&output_directory);
        if !path.exists() {
            bail!(
                "The package for {} (expected at {}) does not exist.
Omicron can build all packages that exist within the repository, but packages
from outside the repository must be created manually, and copied to the output
directory (which is currently: {}).

This is currently a shortcoming with the build system - if you have an interest
in improving the cross-repository meta-build system, please contact sean@.",
                package_name,
                path.to_string_lossy(),
                std::fs::canonicalize(output_directory)
                    .unwrap()
                    .to_string_lossy(),
            );
        }
    }

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
    let packages: Vec<(&String, &Package)> =
        config.packages.iter().chain(config.external_packages.iter()).collect();
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
    for package in
        config.packages.values().chain(config.external_packages.values())
    {
        if package.zone {
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
        SubCommand::Package { artifact_dir } => {
            do_package(&config, &artifact_dir).await?;
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
