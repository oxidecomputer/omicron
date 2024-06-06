// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Default values for data in the Nexus API, when not provided explicitly in a request.

use omicron_common::api::external;
use once_cell::sync::Lazy;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

/// The name provided for a default primary network interface for a guest
/// instance.
pub const DEFAULT_PRIMARY_NIC_NAME: &str = "net0";

/// The default IPv4 subnet range assigned to the default VPC Subnet, when
/// the VPC is created, if one is not provided in the request. See
/// <https://rfd.shared.oxide.computer/rfd/0021> for details.
pub static DEFAULT_VPC_SUBNET_IPV4_BLOCK: Lazy<Ipv4Net> =
    Lazy::new(|| Ipv4Net::new(Ipv4Addr::new(172, 30, 0, 0), 22).unwrap());

pub static DEFAULT_FIREWALL_RULES: Lazy<external::VpcFirewallRuleUpdateParams> =
    Lazy::new(|| {
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
    }"#).unwrap()
    });

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
    Ok(Ipv6Net::new(
        Ipv6Addr::from(bytes),
        omicron_common::address::VPC_IPV6_PREFIX_LENGTH,
    )
    .unwrap())
}

#[cfg(test)]
mod tests {
    use omicron_common::api::external::Ipv6NetExt;

    use super::*;

    #[test]
    fn test_random_vpc_ipv6_prefix() {
        let network = random_vpc_ipv6_prefix().unwrap();
        assert!(network.is_vpc_prefix());
        let octets = network.prefix().octets();
        assert!(octets[6..].iter().all(|x| *x == 0));
    }
}
