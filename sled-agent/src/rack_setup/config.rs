// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with RSS config.

use crate::bootstrap::params::Gateway;
use crate::config::ConfigError;
use omicron_common::address::{
    get_64_subnet, Ipv6Subnet, AZ_PREFIX, RACK_PREFIX, SLED_PREFIX,
};
use serde::Deserialize;
use serde::Serialize;
use std::net::{IpAddr, Ipv6Addr};
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

    /// The minimum number of sleds required to unlock the rack secret.
    ///
    /// If this value is less than 2, no rack secret will be created on startup;
    /// this is the typical case for single-server test/development.
    pub rack_secret_threshold: usize,

    /// Internet gateway information.
    pub gateway: Gateway,

    /// The address on which Nexus should serve an external interface.
    // TODO(https://github.com/oxidecomputer/omicron/issues/1530): Eventually,
    // this should be pulled from a pool of addresses.
    pub nexus_external_address: IpAddr,
}

impl SetupServiceConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(&path)
            .map_err(|err| ConfigError::Io { path: path.into(), err })?;
        toml::from_str(&contents)
            .map_err(|err| ConfigError::Parse { path: path.into(), err })
    }

    pub fn az_subnet(&self) -> Ipv6Subnet<AZ_PREFIX> {
        Ipv6Subnet::<AZ_PREFIX>::new(self.rack_subnet)
    }

    /// Returns the subnet for our rack.
    pub fn rack_subnet(&self) -> Ipv6Subnet<RACK_PREFIX> {
        Ipv6Subnet::<RACK_PREFIX>::new(self.rack_subnet)
    }

    /// Returns the subnet for the `index`-th sled in the rack.
    pub fn sled_subnet(&self, index: u8) -> Ipv6Subnet<SLED_PREFIX> {
        get_64_subnet(self.rack_subnet(), index)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_subnets() {
        let cfg = SetupServiceConfig {
            rack_subnet: "fd00:1122:3344:0100::".parse().unwrap(),
            rack_secret_threshold: 0,
            gateway: Gateway { address: None, mac: macaddr::MacAddr6::nil() },
            nexus_external_address: "192.168.1.20".parse().unwrap(),
        };

        assert_eq!(
            Ipv6Subnet::<AZ_PREFIX>::new(
                //              Masked out in AZ Subnet
                //              vv
                "fd00:1122:3344:0000::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.az_subnet()
        );
        assert_eq!(
            Ipv6Subnet::<RACK_PREFIX>::new(
                //              Shows up from Rack Subnet
                //              vv
                "fd00:1122:3344:0100::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.rack_subnet()
        );
        assert_eq!(
            Ipv6Subnet::<SLED_PREFIX>::new(
                //                0th Sled Subnet
                //                vv
                "fd00:1122:3344:0100::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.sled_subnet(0)
        );
        assert_eq!(
            Ipv6Subnet::<SLED_PREFIX>::new(
                //                1st Sled Subnet
                //                vv
                "fd00:1122:3344:0101::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.sled_subnet(1)
        );
        assert_eq!(
            Ipv6Subnet::<SLED_PREFIX>::new(
                //                Last Sled Subnet
                //                vv
                "fd00:1122:3344:01ff::".parse::<Ipv6Addr>().unwrap(),
            ),
            cfg.sled_subnet(255)
        );
    }
}
