// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for parsing configuration files and working with a simulated SP
//! configuration

use dropshot::ConfigLogging;
use gateway_messages::SerialNumber;
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

/// Configuration of a simulated sidecar SP
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SidecarConfig {
    /// IPv6 multicast address to join.
    pub multicast_addr: Ipv6Addr,
    /// UDP address of the two (fake) KSZ8463 ports
    pub bind_addrs: [SocketAddr; 2],
    /// Fake serial number
    pub serial_number: SerialNumber,
}

/// Configuration of a simulated gimlet SP
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GimletConfig {
    /// IPv6 multicast address to join.
    pub multicast_addr: Ipv6Addr,
    /// UDP address of the two (fake) KSZ8463 ports
    pub bind_addrs: [SocketAddr; 2],
    /// Fake serial number
    pub serial_number: SerialNumber,
    /// Attached components
    pub components: Vec<SpComponentConfig>,
}

/// Configuration of a simulated gimlet SP
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SpComponentConfig {
    /// Name of the component
    pub name: String,
    /// Socket address we'll use to expose this component's serial console
    /// via TCP.
    pub serial_console: Option<SocketAddr>,
}

/// Configuration of a set of simulated SPs
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SimulatedSps {
    /// Simulated sidecar(s)
    pub sidecar: Vec<SidecarConfig>,
    /// Simulated gimlet(s)
    pub gimlet: Vec<GimletConfig>,
}

/// Configuration for a sp-sim
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    /// List of SPs to simulate.
    pub simulated_sps: SimulatedSps,
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
