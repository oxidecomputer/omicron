//! Common code shared between `omicron-package` and `thing-flinger` binaries.

use clap::Subcommand;
use omicron_zone_package::package::Package;
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

/// Errors which may be returned when parsing the server configuration.
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Error deserializing toml from {path}: {err}")]
    Toml { path: PathBuf, err: toml::de::Error },
    #[error("IO error: {message}: {err}")]
    Io { message: String, err: std::io::Error },
}

pub fn parse<P: AsRef<Path>, C: DeserializeOwned>(
    path: P,
) -> Result<C, ParseError> {
    let path = path.as_ref();
    let contents = std::fs::read_to_string(path).map_err(|err| {
        ParseError::Io { message: format!("failed reading {path:?}"), err }
    })?;
    let cfg = toml::from_str::<C>(&contents)
        .map_err(|err| ParseError::Toml { path: path.to_path_buf(), err })?;
    Ok(cfg)
}

/// Commands which should execute on a host building packages.
#[derive(Debug, Subcommand)]
pub enum BuildCommand {
    /// Builds the packages specified in a manifest, and places them into a target
    /// directory.
    Package {
        /// The output directory, where artifacts should be placed.
        ///
        /// Defaults to "out".
        #[clap(long = "out", default_value = "out", action)]
        artifact_dir: PathBuf,
    },
    /// Checks the packages specified in a manifest, without building.
    Check,
}

/// Commands which should execute on a host installing packages.
#[derive(Debug, Subcommand)]
pub enum DeployCommand {
    /// Installs the packages to a target machine.
    Install {
        /// The directory from which artifacts will be pulled.
        ///
        /// Should match the format from the Package subcommand.
        #[clap(long = "in", default_value = "out", action)]
        artifact_dir: PathBuf,

        /// The directory to which artifacts will be installed.
        ///
        /// Defaults to "/opt/oxide".
        #[clap(long = "out", default_value = "/opt/oxide", action)]
        install_dir: PathBuf,
    },
    /// Removes the packages from the target machine.
    Uninstall {
        /// The directory from which artifacts were be pulled.
        ///
        /// Should match the format from the Package subcommand.
        #[clap(long = "in", default_value = "out", action)]
        artifact_dir: PathBuf,

        /// The directory to which artifacts were installed.
        ///
        /// Defaults to "/opt/oxide".
        #[clap(long = "out", default_value = "/opt/oxide", action)]
        install_dir: PathBuf,
    },
}

/// Describes the origin of an externally-built package.
#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ExternalPackageSource {
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
pub struct ExternalPackage {
    #[serde(flatten)]
    pub package: Package,

    pub source: ExternalPackageSource,
}

/// Describes the configuration for a set of packages.
#[derive(Deserialize, Debug)]
pub struct Config {
    /// Packages to be built and installed.
    #[serde(default, rename = "package")]
    pub packages: BTreeMap<String, Package>,

    /// Packages to be installed, but which have been created outside this
    /// repository.
    #[serde(default, rename = "external_package")]
    pub external_packages: BTreeMap<String, ExternalPackage>,
}
