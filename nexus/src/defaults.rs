// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Default values for data in the Nexus API, when not provided explicitly in a request.

use ipnetwork::Ipv6Network;
use lazy_static::lazy_static;
use omicron_common::api::external;
use omicron_common::api::external::Ipv6Net;
use std::net::Ipv6Addr;

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
    let mut bytes = [0u16; 8];
    bytes[0] = 0xfd00;
    rand::thread_rng().try_fill(&mut bytes[1..4]).map_err(|_| {
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
        let network = random_unique_local_ipv6().unwrap().0;
        assert_eq!(network.prefix(), 48);
        let segments = network.network().segments();
        assert_eq!(segments[0], 0xfd00);
        assert!(segments[4..].iter().all(|x| *x == 0));
    }
}
