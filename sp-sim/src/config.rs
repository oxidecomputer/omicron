// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for parsing configuration files and working with a simulated SP
//! configuration

use crate::sensors;
use dropshot::ConfigLogging;
use gateway_messages::DeviceCapabilities;
use gateway_messages::DevicePresence;
use nexus_types::inventory::Caboose;
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkConfig {
    /// Listen address for a fake KSZ8463 port
    Simulated { bind_addr: SocketAddrV6 },

    Real {
        bind_addr: SocketAddrV6,

        /// IPv6 multicast address
        multicast_addr: Ipv6Addr,

        /// IPv6 multicast interface to use.
        multicast_interface: String,
    },
}

impl slog::KV for NetworkConfig {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match &self {
            NetworkConfig::Simulated { bind_addr } => {
                serializer.emit_str("type".into(), "simulated")?;
                serializer
                    .emit_str("bind_addr".into(), &bind_addr.to_string())?;
            }

            NetworkConfig::Real {
                bind_addr,
                multicast_addr,
                multicast_interface,
            } => {
                serializer.emit_str("type".into(), "real")?;
                serializer
                    .emit_str("bind_addr".into(), &bind_addr.to_string())?;
                serializer.emit_str(
                    "multicast_addr".into(),
                    &multicast_addr.to_string(),
                )?;
                serializer.emit_str(
                    "multicast_interface".into(),
                    &multicast_interface,
                )?;
            }
        }

        Ok(())
    }
}

/// Configuration for every caboose in the SP
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SpCabooses {
    pub sp_slot_0: Caboose,
    pub sp_slot_1: Caboose,
    pub rot_slot_a: Caboose,
    pub rot_slot_b: Caboose,
    pub stage0: Caboose,
    pub stage0_next: Caboose,
}

/// Common configuration for all flavors of SP
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SpCommonConfig {
    /// Network config for the two (fake) KSZ8463 ports.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub network_config: Option<[NetworkConfig; 2]>,
    /// Network config for the (fake) ereport UDP ports.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ereport_network_config: Option<[NetworkConfig; 2]>,
    /// Fake serial number
    pub serial_number: String,
    /// 32-byte seed to create a manufacturing root certificate.
    #[serde(with = "hex")]
    pub manufacturing_root_cert_seed: [u8; 32],
    /// 32-byte seed to create a Device ID certificate.
    #[serde(with = "hex")]
    pub device_id_cert_seed: [u8; 32],
    /// Fake components.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub components: Vec<SpComponentConfig>,
    /// Return errors for `versioned_rot_boot_info` simulating
    /// an older RoT
    #[serde(default)]
    pub old_rot_state: bool,
    /// Simulate a RoT stage0 with no caboose
    #[serde(default)]
    pub no_stage0_caboose: bool,
    /// Fake ereport configuration
    #[serde(default)]
    pub ereport_config: EreportConfig,
    /// Configurable caboose values. If unset, these will be
    /// populated with default values
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cabooses: Option<SpCabooses>,
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

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub sensors: Vec<SensorConfig>,
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

/// Configuration for a component's sensor readings.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SensorConfig {
    #[serde(flatten)]
    pub def: sensors::SensorDef,

    #[serde(flatten)]
    pub state: sensors::SensorState,
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

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default,
)]
pub struct EreportConfig {
    #[serde(flatten, default)]
    pub restart: EreportRestart,

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub ereports: Vec<Ereport>,
}

/// Represents a single simulated restart generation of the simulated SP's
/// ereporter.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EreportRestart {
    #[serde(default = "uuid::Uuid::new_v4")]
    pub restart_id: uuid::Uuid,

    #[serde(skip_serializing_if = "toml::map::Map::is_empty", default)]
    pub metadata: toml::map::Map<String, toml::Value>,
}

impl Default for EreportRestart {
    fn default() -> Self {
        Self { restart_id: uuid::Uuid::new_v4(), metadata: Default::default() }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Ereport {
    pub task_name: String,
    pub task_gen: u32,
    pub uptime: u64,
    #[serde(flatten)]
    pub data: toml::map::Map<String, toml::Value>,
}
