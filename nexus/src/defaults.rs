// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Default values for data in the Nexus API, when not provided explicitly in a request.

use ipnetwork::Ipv4Network;
use ipnetwork::Ipv6Network;
use lazy_static::lazy_static;
use omicron_common::api::external;
use omicron_common::api::external::Ipv4Net;
use omicron_common::api::external::Ipv6Net;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

/// Minimum prefix size supported in IPv4 VPC Subnets.
///
/// NOTE: This is the minimum _prefix_, which sets the maximum subnet size.
pub const MIN_VPC_IPV4_SUBNET_PREFIX: u8 = 8;

/// Maximum prefix size supported in IPv4 VPC Subnets.
///
/// NOTE: This is the maximum _prefix_, which sets the minimum subnet size.
pub const MAX_VPC_IPV4_SUBNET_PREFIX: u8 = 26;

/// The default prefix size for a automatically-generated IPv6 address range for
/// VPC Subnets
pub const DEFAULT_VPC_SUBNET_IPV6_PREFIX: u8 = 64;

lazy_static! {
    /// The default IPv4 subnet range assigned to the default VPC Subnet, when
    /// the VPC is created, if one is not provided in the request. See
    /// <https://rfd.shared.oxide.computer/rfd/0021> for details.
    pub static ref DEFAULT_VPC_SUBNET_IPV4_BLOCK: external::Ipv4Net =
        Ipv4Net(Ipv4Network::new(Ipv4Addr::new(172, 30, 0, 0), 22).unwrap());
}

lazy_static! {
    pub static ref DEFAULT_FIREWALL_RULES: external::VpcFirewallRuleUpdateParams =
        serde_json::from_str(r#"{
            "allow-internal-inbound": {
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": { "hosts": [ { "type": "vpc", "value": "default" } ] },
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound traffic to all instances within the VPC if originated within the VPC"
            },
            "allow-ssh": {
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": { "ports": [ "22" ], "protocols": [ "TCP" ] },
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound TCP connections on port 22 from anywhere"
            },
            "allow-icmp": {
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": { "protocols": [ "ICMP" ] },
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound ICMP traffic from anywhere"
            },
            "allow-rdp": {
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": { "ports": [ "3389" ], "protocols": [ "TCP" ] },
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound TCP connections on port 3389 from anywhere"
            }
        }"#).unwrap();
}

pub fn random_unique_local_ipv6() -> Result<Ipv6Net, external::Error> {
    use rand::Rng;
    let mut bytes = [0u8; 16];
    bytes[0] = 0xfd;
    rand::thread_rng().try_fill(&mut bytes[1..6]).map_err(|_| {
        external::Error::internal_error(
            "Unable to allocate random IPv6 address range",
        )
    })?;
    Ok(Ipv6Net(Ipv6Network::new(Ipv6Addr::from(bytes), 48).unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_unique_local_ipv6() {
        let network = random_unique_local_ipv6().unwrap();
        assert!(network.is_unique_local());
        let octets = network.network().octets();
        assert!(octets[6..].iter().all(|x| *x == 0));
    }
}
