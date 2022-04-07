// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with RSS config.

use crate::config::ConfigError;
use crate::params::{DatasetEnsureBody, ServiceRequest};
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;

/// Configuration for the "rack setup service", which is controlled during
/// bootstrap.
///
/// The Rack Setup Service should be responsible for one-time setup actions,
/// such as CockroachDB placement and initialization.  Without operator
/// intervention, however, these actions need a way to be automated in our
/// deployment.
///
/// By injecting this (optional) configuration into the bootstrap agent, it
/// can act as a stand-in initialization service.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SetupServiceConfig {
    pub rack_subnet: std::net::Ipv6Addr,

    #[serde(default, rename = "request")]
    pub requests: Vec<SledRequest>,
}

/// A request to initialize a sled.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SledRequest {
    /// Datasets to be created.
    #[serde(default, rename = "dataset")]
    pub datasets: Vec<DatasetEnsureBody>,

    /// Services to be instantiated.
    #[serde(default, rename = "service")]
    pub services: Vec<ServiceRequest>,
}

impl SetupServiceConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)?;
        let config = toml::from_str(&contents)?;
        Ok(config)
    }

    pub fn az_subnet(&self) -> ipnetwork::Ipv6Network {
        ipnetwork::Ipv6Network::new(self.rack_subnet, 48).unwrap()
    }

    pub fn rack_subnet(&self) -> ipnetwork::Ipv6Network {
        ipnetwork::Ipv6Network::new(self.rack_subnet, 56).unwrap()
    }

    pub fn sled_subnet(&self, index: u8) -> ipnetwork::Ipv6Network {
        let mut rack_network = self.rack_subnet().network().octets();

        // To set bits distinguishing the /64 from the /56, we modify the 7th octet.
        //
        // 0001:0203:0405:0607::
        rack_network[7] = index;
        ipnetwork::Ipv6Network::new(std::net::Ipv6Addr::from(rack_network), 64).unwrap()
    }
}
