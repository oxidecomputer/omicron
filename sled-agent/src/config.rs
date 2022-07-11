// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with sled agent configuration

use crate::common::vlan::VlanID;
use crate::illumos::dladm::{self, Dladm, PhysicalLink};
use crate::illumos::zpool::ZpoolName;
use dropshot::ConfigLogging;
use macaddr::MacAddr6;
use serde::Deserialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use serde_with::PickFirst;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

/// Configuration for a sled agent
#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Configuration for the sled agent debug log
    pub log: ConfigLogging,
    /// Optional VLAN ID to be used for tagging guest VNICs.
    pub vlan: Option<VlanID>,
    /// Optional list of zpools to be used as "discovered disks".
    pub zpools: Option<Vec<ZpoolName>>,

    /// IP address of the Internet gateway, which is particularly
    /// relevant for external-facing services (such as Nexus).
    pub gateway_address: Option<Ipv4Addr>,

    /// MAC address of the internet gateway above. This is used to provide
    /// external connectivity into guests, by allowing OPTE to forward traffic
    /// destined for the broader network to the gateway.
    // This uses the `serde_with` crate's `serde_as` attribute, which tries
    // each of the listed serialization types (starting with the default) until
    // one succeeds. This supports deserialization from either an array of u8,
    // or the display-string representation.
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    pub gateway_mac: MacAddr6,

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
