// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with sled agent configuration

use crate::common::vlan::VlanID;
use crate::illumos::dladm::{self, Dladm, PhysicalLink};
use crate::illumos::zpool::ZpoolName;
use dropshot::ConfigLogging;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Configuration for a sled agent
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Unique id for the sled
    pub id: Uuid,
    /// Configuration for the sled agent debug log
    pub log: ConfigLogging,
    /// Optionally force the sled to self-identify as a scrimlet (or gimlet,
    /// if set to false).
    pub stub_scrimlet: Option<bool>,
    /// Optional VLAN ID to be used for tagging guest VNICs.
    pub vlan: Option<VlanID>,
    /// Optional list of zpools to be used as "discovered disks".
    pub zpools: Option<Vec<ZpoolName>>,

    /// The data link on which we infer the bootstrap address.
    ///
    /// If unsupplied, we default to the first physical device.
    pub data_link: Option<PhysicalLink>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config from {path}: {err}")]
    Io {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },
    #[error("Failed to parse config from {path}: {err}")]
    Parse {
        path: PathBuf,
        #[source]
        err: toml::de::Error,
    },
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(&path)
            .map_err(|err| ConfigError::Io { path: path.into(), err })?;
        let config = toml::from_str(&contents)
            .map_err(|err| ConfigError::Parse { path: path.into(), err })?;
        Ok(config)
    }

    pub fn get_link(
        &self,
    ) -> Result<PhysicalLink, dladm::FindPhysicalLinkError> {
        let link = if let Some(link) = self.data_link.clone() {
            link
        } else {
            Dladm::find_physical()?
        };
        Ok(link)
    }
}
