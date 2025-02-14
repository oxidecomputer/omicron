//! Common code shared between `omicron-package` and `thing-flinger` binaries.

use camino::{Utf8Path, Utf8PathBuf};
use clap::Subcommand;
use config::MultiPresetArg;
use omicron_zone_package::config::{PackageName, PresetName};
use serde::de::DeserializeOwned;
use thiserror::Error;

pub mod cargo_plan;
pub mod config;
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
        /// The preset to use as part of the build (use `dev` for development).
        ///
        /// Presets are defined in the `target.preset` section of the config.
        /// The other configurations are layered on top of the preset.
        #[clap(short, long)]
        preset: PresetName,

        /// The image to use for the target.
        ///
        /// If specified, this configuration is layered on top of the preset.
        #[clap(short, long)]
        image: Option<crate::target::Image>,

        /// The kind of machine to build for.
        #[clap(short, long, help_heading = "Preset overrides")]
        machine: Option<crate::target::Machine>,

        /// The switch to use for the target.
        #[clap(short, long, help_heading = "Preset overrides")]
        switch: Option<crate::target::Switch>,

        #[clap(short, long, help_heading = "Preset overrides")]
        /// Specify whether nexus will run in a single-sled or multi-sled
        /// environment.
        ///
        /// Set single-sled for dev purposes when you're running a single
        /// sled-agent. Set multi-sled if you're running with multiple sleds.
        /// Currently this only affects the crucible disk allocation strategy-
        /// VM disks will require 3 distinct sleds with `multi-sled`, which will
        /// fail in a single-sled environment. `single-sled` relaxes this
        /// requirement.
        rack_topology: Option<crate::target::RackTopology>,

        #[clap(short, long, help_heading = "Preset overrides")]
        // TODO (https://github.com/oxidecomputer/omicron/issues/4148): Remove
        // once single-node functionality is removed.
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
        only: Vec<PackageName>,
    },
    /// Stamps semver versions onto packages within a manifest
    Stamp {
        /// The name of the artifact to be stamped.
        package_name: PackageName,

        /// The version to be stamped onto the package.
        version: semver::Version,
    },
    /// Show the Cargo commands that would be run to build the packages.
    ShowCargoCommands {
        /// Show cargo commands for the specified presets, or `all` for all
        /// presets.
        ///
        /// If not specified, the active target's preset is used.
        #[clap(short, long = "preset")]
        presets: Option<MultiPresetArg>,
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
