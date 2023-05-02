// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with RSS config.

use crate::config::ConfigError;
use omicron_common::address::{
    get_64_subnet, Ipv6Subnet, AZ_PREFIX, RACK_PREFIX, SLED_PREFIX,
};
use std::path::Path;

pub use crate::bootstrap::params::RackInitializeRequest as SetupServiceConfig;

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
    use crate::bootstrap::params::{Gateway, RecoverySiloConfig};
    use omicron_common::address::IpRange;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_subnets() {
        let cfg = SetupServiceConfig {
            rack_subnet: "fd00:1122:3344:0100::".parse().unwrap(),
            rack_secret_threshold: 0,
            gateway: Some(Gateway {
                address: None,
                mac: macaddr::MacAddr6::nil().into(),
            }),
            ntp_servers: vec![String::from("test.pool.example.com")],
            dns_servers: vec![String::from("1.1.1.1")],
            external_dns_zone_name: String::from("oxide.test"),
            internal_services_ip_pool_ranges: vec![IpRange::from(IpAddr::V4(
                Ipv4Addr::new(129, 168, 1, 20),
            ))],
            recovery_silo: RecoverySiloConfig {
                silo_name: "test-silo".parse().unwrap(),
                user_name: "dummy".parse().unwrap(),
                // This is a hash for the password "oxide".  It doesn't matter,
                // though; it's not used.
                user_password_hash: "$argon2id$v=19$m=98304,t=13,p=1$\
                    RUlWc0ZxaHo0WFdrN0N6ZQ$S8p52j85GPvMhR/\
                    ek3GL0el/oProgTwWpHJZ8lsQQoY"
                    .parse()
                    .unwrap(),
            },
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
