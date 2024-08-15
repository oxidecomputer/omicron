//! Common code shared between `omicron-package` and `thing-flinger` binaries.

use camino::{Utf8Path, Utf8PathBuf};
use clap::Subcommand;
use serde::de::DeserializeOwned;
use thiserror::Error;

pub mod dot;
pub mod target;

/// Errors which may be returned when parsing the server configuration.
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Error deserializing toml from {path}: {err}")]
    Toml { path: Utf8PathBuf, err: toml::de::Error },
    #[error("IO error: {message}: {err}")]
    Io { message: String, err: std::io::Error },
}

pub fn parse<P: AsRef<Utf8Path>, C: DeserializeOwned>(
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

#[derive(Clone, Debug, Subcommand)]
pub enum TargetCommand {
    /// Creates a new build target, and sets it as "active".
    Create {
        #[clap(short, long, default_value = "standard")]
        image: crate::target::Image,

        #[clap(
            short,
            long,
            default_value_if("image", "standard", "non-gimlet")
        )]
        machine: Option<crate::target::Machine>,

        #[clap(short, long, default_value_if("image", "standard", "stub"))]
        switch: Option<crate::target::Switch>,

        #[clap(
            short,
            long,
            default_value_if("image", "trampoline", Some("single-sled")),

            // This opt is required, and clap will enforce that even with
            // `required = false`, since it's not an Option. But the
            // default_value_if only works if we set `required` to false. It's
            // jank, but it is what it is.
            // https://github.com/clap-rs/clap/issues/4086
            required = false
        )]
        /// Specify whether nexus will run in a single-sled or multi-sled
        /// environment.
        ///
        /// Set single-sled for dev purposes when you're running a single
        /// sled-agent. Set multi-sled if you're running with mulitple sleds.
        /// Currently this only affects the crucible disk allocation strategy-
        /// VM disks will require 3 distinct sleds with `multi-sled`, which will
        /// fail in a single-sled environment. `single-sled` relaxes this
        /// requirement.
        rack_topology: crate::target::RackTopology,

        #[clap(
            short,
            long,
            default_value = Some("single-node"),
            required = false
        )]
        /// Specify whether clickhouse will be deployed as a replicated cluster
        /// or single-node configuration.
        /// 
        /// Replicated cluster configuration is an experimental feature to be
        /// used only for testing.
        clickhouse_topology: Option<crate::target::ClickhouseTopology>,
    },
    /// List all existing targets
    List,
    /// Activates a build target, with a symlink named "active".
    ///
    /// This causes all subsequent commands to act on the build target
    /// if the "--target" flag is omitted.
    Set,
    /// Delete an existing target
    Delete,
}

/// Commands which should execute on a host building packages.
#[derive(Debug, Subcommand)]
pub enum BuildCommand {
    /// Define the build configuration target for subsequent commands
    Target {
        #[command(subcommand)]
        subcommand: TargetCommand,
    },
    /// Make a `dot` graph to visualize the package tree
    Dot,
    /// List the output packages for the current target
    ListOutputs {
        #[clap(long)]
        intermediate: bool,
    },
    /// Builds the packages specified in a manifest, and places them into an
    /// 'out' directory.
    Package {
        /// If true, disables the cache.
        ///
        /// By default, the cache is used.
        #[clap(short, long)]
        disable_cache: bool,
        /// Limit to building only these packages
        #[clap(long)]
        only: Vec<String>,
    },
    /// Stamps semver versions onto packages within a manifest
    Stamp {
        /// The name of the artifact to be stamped.
        package_name: String,

        /// The version to be stamped onto the package.
        version: semver::Version,
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
        /// The directory to which artifacts will be installed.
        ///
        /// Defaults to "/opt/oxide".
        #[clap(long = "out", default_value = "/opt/oxide", action)]
        install_dir: Utf8PathBuf,
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
        /// The directory to which artifacts will be installed.
        ///
        /// Defaults to "/opt/oxide".
        #[clap(long = "out", default_value = "/opt/oxide", action)]
        install_dir: Utf8PathBuf,
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
        install_dir: Utf8PathBuf,
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
        /// The directory to which artifacts were installed.
        ///
        /// Defaults to "/opt/oxide".
        #[clap(long = "out", default_value = "/opt/oxide", action)]
        install_dir: Utf8PathBuf,
    },
}
