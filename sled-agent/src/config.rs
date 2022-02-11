// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with sled agent configuration

use crate::common::vlan::VlanID;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;
use uuid::Uuid;

/// Configuration for a sled agent
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Unique id for the sled
    pub id: Uuid,
    /// Address of the Bootstrap Agent interface.
    pub bootstrap_address: SocketAddr,
    /// Address of Nexus instance
    pub nexus_address: SocketAddr,
    /// Configuration for the sled agent dropshot server
    pub dropshot: ConfigDropshot,
    /// Configuration for the sled agent debug log
    pub log: ConfigLogging,
    /// Optional VLAN ID to be used for tagging guest VNICs.
    pub vlan: Option<VlanID>,
    /// Optional list of zpools to be used as "discovered disks".
    ///
    /// TODO: Can we make one from the local fs, to keep the "with/without"
    /// zpool cases similar?
    pub zpools: Option<Vec<String>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)?;
        let config = toml::from_str(&contents)?;
        Ok(config)
    }
}
