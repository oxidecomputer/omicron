// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with sled agent configuration

use crate::common::vlan::VlanID;
use crate::illumos::dladm::{self, Dladm, PhysicalLink};
use crate::illumos::zpool::ZpoolName;
use dropshot::ConfigLogging;
use omicron_common::api::external::Ipv6Net;
use serde::Deserialize;
use std::net::{SocketAddr, SocketAddrV6};
use std::path::Path;
use uuid::Uuid;

pub const SLED_AGENT_PORT: u16 = 12345;

/// Given a subnet, return the sled agent address.
pub(crate) fn get_sled_address(subnet: Ipv6Net) -> SocketAddrV6 {
    let mut iter = subnet.iter();
    let _anycast_ip = iter.next().unwrap();
    let sled_agent_ip = iter.next().unwrap();
    SocketAddrV6::new(sled_agent_ip, SLED_AGENT_PORT, 0, 0)
}

/// Configuration for a sled agent
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Unique id for the sled
    pub id: Uuid,
    /// Address of Nexus instance
    pub nexus_address: SocketAddr,
    /// Configuration for the sled agent debug log
    pub log: ConfigLogging,
    /// Optional VLAN ID to be used for tagging guest VNICs.
    pub vlan: Option<VlanID>,
    /// Optional list of zpools to be used as "discovered disks".
    pub zpools: Option<Vec<ZpoolName>>,

    /// The data link on which to allocate VNICs.
    ///
    /// If unsupplied, we default to the first physical device.
    pub data_link: Option<PhysicalLink>,
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

    pub fn get_link(&self) -> Result<PhysicalLink, dladm::Error> {
        let link = if let Some(link) = self.data_link.clone() {
            link
        } else {
            Dladm::find_physical()?
        };
        Ok(link)
    }
}
