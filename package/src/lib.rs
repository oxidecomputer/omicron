//! Common code shared between `omicron-package` and `thing-flinger` binaries.

use clap::Subcommand;
use serde::de::DeserializeOwned;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

pub mod dot;

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
    /// Make a `dot` graph to visualize the package tree
    Dot,
    /// Builds the packages specified in a manifest, and places them into an
    /// 'out' directory.
    Package {
        /// The output directory, where artifacts should be placed.
        ///
        /// Defaults to "out".
        #[clap(long = "out", default_value = "out", action)]
        artifact_dir: PathBuf,
    },
    /// Checks the packages specified in a manifest, without building them.
    Check,
}

/// Commands which should execute on a host installing packages.
#[derive(Debug, Subcommand)]
pub enum DeployCommand {
    /// Installs the packages and starts the sled-agent. Shortcut for `unpack`
    /// and `activate`.
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
    /// Unpacks the files created by `package` to an install directory.
    /// Issues the `uninstall` command.
    ///
    /// This command performs uninstallation by default as a safety measure,
    /// to ensure that we are not swapping packages underneath running services,
    /// which may result in unexpected behavior.
    /// The "uninstall before unpack" behavior can be disabled by setting
    /// the environment variable OMICRON_NO_UNINSTALL.
    ///
    /// `unpack` does not actually start any services, but it prepares services
    /// to be launched with the `activate` command.
    Unpack {
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
    /// Imports and starts the sled-agent illumos service
    ///
    /// The necessary packages must exist in the installation directory
    /// already; this can be done with the `unpack` command.
    Activate {
        /// The directory to which artifacts will be installed.
        ///
        /// Defaults to "/opt/oxide".
        #[clap(long = "out", default_value = "/opt/oxide", action)]
        install_dir: PathBuf,
    },
    /// Deletes all Omicron zones and stops all services.
    ///
    /// This command may be used to stop the currently executing Omicron
    /// services, such that they could be restarted later.
    Deactivate,
    /// Uninstalls packages and deletes durable Omicron storage. Issues the
    /// `deactivate` command.
    ///
    /// This command deletes all state used by Omicron services, but leaves
    /// the packages in the installation directory. This means that a later
    /// call to `activate` could re-install Omicron services.
    Uninstall,
    /// Uninstalls packages and removes them from the installation directory.
    /// Issues the `uninstall` command.
    Clean {
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
