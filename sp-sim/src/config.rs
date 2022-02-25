// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//!
//! Interfaces for parsing configuration files and working with a simulated SP
//! configuration
//!

use dropshot::ConfigLogging;
use gateway_messages::IgnitionState;
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SidecarConfig {
    pub ignition_targets: Vec<IgnitionState>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum SpType {
    Sidecar(SidecarConfig),
    Gimlet,
}

/// Description of a simulated SP's components.
// TODO should reorganize this once we have more to do with components than just
// a serial console - maybe a list of components with flags for which operations
// they support (serial console, power on/off, etc)
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SpComponents {
    /// List of components with a serial console.
    pub serial_console: Vec<String>,
}

/// Configuration for a simulated SP
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    /// Type of SP to simulate.
    pub sp_type: SpType,
    /// Components to simulate.
    pub components: SpComponents,
    /// UDP listen address.
    pub bind_address: SocketAddr,
    /// UDP address of MGS.
    pub gateway_address: SocketAddr,
    /// Server-wide logging configuration.
    pub log: ConfigLogging,
}

impl Config {
    /// Load a `Config` from the given TOML file
    ///
    /// This config object can then be used to create a new simulated SP.
    // The format is described in the README. // TODO add a README
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Config, LoadError> {
        let path = path.as_ref();
        let file_contents = std::fs::read_to_string(path)
            .map_err(|e| (path.to_path_buf(), e))?;
        let config_parsed: Config = toml::from_str(&file_contents)
            .map_err(|e| (path.to_path_buf(), e))?;
        Ok(config_parsed)
    }
}

// TODO: This is copy-pasted from `gateway` and very similar to what `nexus` has
// - should we centralize on something?
#[derive(Debug, Error)]
pub enum LoadError {
    #[error("error reading \"{}\": {}", path.display(), err)]
    Io { path: PathBuf, err: std::io::Error },
    #[error("error parsing \"{}\": {}", path.display(), err)]
    Parse { path: PathBuf, err: toml::de::Error },
}

impl From<(PathBuf, std::io::Error)> for LoadError {
    fn from((path, err): (PathBuf, std::io::Error)) -> Self {
        LoadError::Io { path, err }
    }
}

impl From<(PathBuf, toml::de::Error)> for LoadError {
    fn from((path, err): (PathBuf, toml::de::Error)) -> Self {
        LoadError::Parse { path, err }
    }
}

impl std::cmp::PartialEq<std::io::Error> for LoadError {
    fn eq(&self, other: &std::io::Error) -> bool {
        if let LoadError::Io { err, .. } = self {
            err.kind() == other.kind()
        } else {
            false
        }
    }
}
