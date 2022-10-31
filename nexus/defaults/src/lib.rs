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

/// The number of reserved addresses at the beginning of a subnet range.
pub const NUM_INITIAL_RESERVED_IP_ADDRESSES: usize = 5;

/// The name provided for a default primary network interface for a guest
/// instance.
pub const DEFAULT_PRIMARY_NIC_NAME: &str = "net0";

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
            "rules": [
                {
                    "name": "allow-internal-inbound",
                    "status": "enabled",
                    "direction": "inbound",
                    "targets": [ { "type": "vpc", "value": "default" } ],
                    "filters": { "hosts": [ { "type": "vpc", "value": "default" } ] },
                    "action": "allow",
                    "priority": 65534,
                    "description": "allow inbound traffic to all instances within the VPC if originated within the VPC"
                },
                {
                    "name": "allow-ssh",
                    "status": "enabled",
                    "direction": "inbound",
                    "targets": [ { "type": "vpc", "value": "default" } ],
                    "filters": { "ports": [ "22" ], "protocols": [ "TCP" ] },
                    "action": "allow",
                    "priority": 65534,
                    "description": "allow inbound TCP connections on port 22 from anywhere"
                },
                {
                    "name": "allow-icmp",
                    "status": "enabled",
                    "direction": "inbound",
                    "targets": [ { "type": "vpc", "value": "default" } ],
                    "filters": { "protocols": [ "ICMP" ] },
                    "action": "allow",
                    "priority": 65534,
                    "description": "allow inbound ICMP traffic from anywhere"
                }
            ]
        }"#).unwrap();
}

/// Generate a random VPC IPv6 prefix, in the range `fd00::/48`.
pub fn random_vpc_ipv6_prefix() -> Result<Ipv6Net, external::Error> {
    use rand::Rng;
    let mut bytes = [0u8; 16];
    bytes[0] = 0xfd;
    rand::thread_rng().try_fill(&mut bytes[1..6]).map_err(|_| {
        external::Error::internal_error(
            "Unable to allocate random IPv6 address range",
        )
    })?;
    Ok(Ipv6Net(
        Ipv6Network::new(
            Ipv6Addr::from(bytes),
            Ipv6Net::VPC_IPV6_PREFIX_LENGTH,
        )
        .unwrap(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_vpc_ipv6_prefix() {
        let network = random_vpc_ipv6_prefix().unwrap();
        assert!(network.is_vpc_prefix());
        let octets = network.network().octets();
        assert!(octets[6..].iter().all(|x| *x == 0));
    }
}
