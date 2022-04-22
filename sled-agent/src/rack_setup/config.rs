// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with RSS config.

use crate::config::ConfigError;
use crate::params::{DatasetEnsureBody, ServiceRequest};
use ipnetwork::Ipv6Network;
use omicron_common::address::{AZ_PREFIX, RACK_PREFIX};
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv6Addr;
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
    pub rack_subnet: Ipv6Addr,

    #[serde(default, rename = "request")]
    pub requests: Vec<SledRequest>,
}

/// A request to initialize a sled.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SledRequest {
    /// Datasets to be created.
    #[serde(default, rename = "dataset")]
    pub datasets: Vec<DatasetEnsureBody>,

    /// Services to be instantiated.
    #[serde(default, rename = "service")]
    pub services: Vec<ServiceRequest>,

    /// DNS Services to be instantiated.
    #[serde(default, rename = "dns_service")]
    pub dns_services: Vec<ServiceRequest>,
}

fn new_network(addr: Ipv6Addr, prefix: u8) -> Ipv6Network {
    let net = Ipv6Network::new(addr, prefix).unwrap();

    // ipnetwork inputs/outputs the provided IPv6 address, unmodified by the
    // prefix. We manually mask `addr` based on `prefix` ourselves.
    Ipv6Network::new(net.network(), prefix).unwrap()
}

impl SetupServiceConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)?;
        let config = toml::from_str(&contents)?;
        Ok(config)
    }

    pub fn az_subnet(&self) -> Ipv6Network {
        new_network(self.rack_subnet, AZ_PREFIX)
    }

    /// Returns the subnet for our rack.
    pub fn rack_subnet(&self) -> Ipv6Network {
        new_network(self.rack_subnet, RACK_PREFIX)
    }

    /// Returns the subnet for the `index`-th sled in the rack.
    pub fn sled_subnet(&self, index: u8) -> Ipv6Network {
        omicron_common::address::get_64_subnet(self.rack_subnet(), index)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_subnets() {
        let cfg = SetupServiceConfig {
            rack_subnet: "fd00:1122:3344:0100::".parse().unwrap(),
            requests: vec![],
        };

        assert_eq!(
            //              Masked out in AZ Subnet
            //              vv
            "fd00:1122:3344:0000::/48".parse::<Ipv6Network>().unwrap(),
            cfg.az_subnet()
        );
        assert_eq!(
            //              Shows up from Rack Subnet
            //              vv
            "fd00:1122:3344:0100::/56".parse::<Ipv6Network>().unwrap(),
            cfg.rack_subnet()
        );
        assert_eq!(
            //                0th Sled Subnet
            //                vv
            "fd00:1122:3344:0100::/64".parse::<Ipv6Network>().unwrap(),
            cfg.sled_subnet(0)
        );
        assert_eq!(
            //                1st Sled Subnet
            //                vv
            "fd00:1122:3344:0101::/64".parse::<Ipv6Network>().unwrap(),
            cfg.sled_subnet(1)
        );
        assert_eq!(
            //                Last Sled Subnet
            //                vv
            "fd00:1122:3344:01ff::/64".parse::<Ipv6Network>().unwrap(),
            cfg.sled_subnet(255)
        );
    }
}
