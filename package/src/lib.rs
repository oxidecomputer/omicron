//! Common code shared between `omicron-package` and `thing-flinger` binaries.

use serde::de::DeserializeOwned;
use std::path::Path;
use std::path::PathBuf;
use structopt::StructOpt;
use thiserror::Error;

/// Errors which may be returned when parsing the server configuration.
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Cannot parse toml: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub fn parse<P: AsRef<Path>, C: DeserializeOwned>(
    path: P,
) -> Result<C, ParseError> {
    let contents = std::fs::read_to_string(path.as_ref())?;
    let cfg = toml::from_str::<C>(&contents)?;
    Ok(cfg)
}

#[derive(Debug, StructOpt)]
pub enum SubCommand {
    /// Builds the packages specified in a manifest, and places them into a target
    /// directory.
    Package {
        /// The output directory, where artifacts should be placed.
        ///
        /// Defaults to "out".
        #[structopt(long = "out", default_value = "out")]
        artifact_dir: PathBuf,
    },
    /// Checks the packages specified in a manifest, without building.
    Check,
    /// Installs the packages to a target machine.
    Install {
        /// The directory from which artifacts will be pulled.
        ///
        /// Should match the format from the Package subcommand.
        #[structopt(long = "in", default_value = "out")]
        artifact_dir: PathBuf,

        /// The directory to which artifacts will be installed.
        ///
        /// Defaults to "/opt/oxide".
        #[structopt(long = "out", default_value = "/opt/oxide")]
        install_dir: PathBuf,
    },
    /// Removes the packages from the target machine.
    Uninstall {
        /// The directory from which artifacts were be pulled.
        ///
        /// Should match the format from the Package subcommand.
        #[structopt(long = "in", default_value = "out")]
        artifact_dir: PathBuf,

        /// The directory to which artifacts were installed.
        ///
        /// Defaults to "/opt/oxide".
        #[structopt(long = "out", default_value = "/opt/oxide")]
        install_dir: PathBuf,
    },
}
