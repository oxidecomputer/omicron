// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for parsing configuration files and working with a simulated SP
//! configuration

use dropshot::ConfigLogging;
use gateway_messages::DeviceCapabilities;
use gateway_messages::DevicePresence;
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

/// Common configuration for all flavors of SP
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SpCommonConfig {
    /// IPv6 multicast address to join.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub multicast_addr: Option<Ipv6Addr>,
    /// UDP address of the two (fake) KSZ8463 ports
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub bind_addrs: Option<[SocketAddrV6; 2]>,
    /// Fake serial number
    #[serde(with = "hex")]
    pub serial_number: Vec<u8>,
    /// 32-byte seed to create a manufacturing root certificate.
    #[serde(with = "hex")]
    pub manufacturing_root_cert_seed: [u8; 32],
    /// 32-byte seed to create a Device ID certificate.
    #[serde(with = "hex")]
    pub device_id_cert_seed: [u8; 32],
    /// Fake components.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub components: Vec<SpComponentConfig>,
}

/// Configuration of a simulated SP component
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SpComponentConfig {
    /// Must be at most [`gateway_messages::SpComponent::MAX_ID_LENGTH`] bytes
    /// long.
    pub id: String,
    pub device: String,
    pub description: String,
    pub capabilities: DeviceCapabilities,
    pub presence: DevicePresence,
    /// Socket address to emulate a serial console.
    ///
    /// Only supported for components inside a [`GimletConfig`].
    pub serial_console: Option<SocketAddrV6>,
}

/// Configuration of a simulated sidecar SP
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SidecarConfig {
    #[serde(flatten)]
    pub common: SpCommonConfig,
}

/// Configuration of a simulated gimlet SP
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GimletConfig {
    #[serde(flatten)]
    pub common: SpCommonConfig,
}

/// Configuration of a set of simulated SPs
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SimulatedSpsConfig {
    /// Simulated sidecar(s)
    pub sidecar: Vec<SidecarConfig>,
    /// Simulated gimlet(s)
    pub gimlet: Vec<GimletConfig>,
}

/// Configuration for a sp-sim
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    /// List of SPs to simulate.
    pub simulated_sps: SimulatedSpsConfig,
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

impl GimletConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, LoadError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(&path)
            .map_err(|err| LoadError::Io { path: path.into(), err })?;
        let config = toml::from_str(&contents)
            .map_err(|err| LoadError::Parse { path: path.into(), err })?;
        Ok(config)
    }
}
