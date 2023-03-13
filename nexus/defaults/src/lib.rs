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

/// Returns the default (instance) firewall rules for a VPC with the given name.
pub fn firewall_rules(vpc_name: &str) -> external::VpcFirewallRuleUpdateParams {
    serde_json::from_value(serde_json::json!({
        "rules": [
            {
                "name": "allow-internal-inbound",
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": vpc_name } ],
                "filters": { "hosts": [ { "type": "vpc", "value": vpc_name } ] },
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound traffic to all instances within the VPC if originated within the VPC"
            },
            {
                "name": "allow-ssh",
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": vpc_name } ],
                "filters": { "ports": [ "22" ], "protocols": [ "TCP" ] },
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound TCP connections on port 22 from anywhere"
            },
            {
                "name": "allow-icmp",
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": vpc_name } ],
                "filters": { "protocols": [ "ICMP" ] },
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound ICMP traffic from anywhere"
            }
        ]
    })).unwrap()
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

// Some hardcoded values for the Nexus service itself
pub mod nexus_service {
    use super::*;
    use omicron_common::api::external::MacAddr;
    use omicron_common::api::external::Vni;
    use omicron_common::api::internal::sled_agent::NetworkInterface;
    use std::net::IpAddr;

    /// The MAC address assigned to the OPTE port for the Nexus service.
    ///
    /// Chosen from the "system" portion of the virtual MAC address space.
    pub const EXTERNAL_NIC_MAC: MacAddr =
        MacAddr(macaddr::MacAddr6::new(0xA8, 0x40, 0x25, 0xFF, 0x00, 0x01));

    lazy_static! {
        /// The VNI assigned to the Nexus service VPC.
        ///
        /// Chosen from the Oxide-reserved range.
        pub static ref VNI: Vni = 100.try_into().unwrap();

        /// The IP address assigned to the OPTE port for the Nexus service.
        ///
        /// This is the private IP nexus will bind to within its service zone.
        /// OPTE will map to this from whatever external IP Nexus is configured
        /// to use.
        /// Use first non-reserved IP in the subnet, see RFD 21, section 2.2,
        ///
        /// TODO-completeness: IPv6
        pub static ref EXTERNAL_NIC_PRIVATE_IP: IpAddr =
            DEFAULT_VPC_SUBNET_IPV4_BLOCK.iter().nth(5).unwrap().into();

        /// Information used to construct the OPTE port for the Nexus service.
        pub static ref EXTERNAL_NIC: NetworkInterface = NetworkInterface {
            name: "nexus".parse().unwrap(),
            ip: *EXTERNAL_NIC_PRIVATE_IP,
            mac: EXTERNAL_NIC_MAC,
            subnet: (*DEFAULT_VPC_SUBNET_IPV4_BLOCK).into(),
            vni: *VNI,
            // Irrelevant for service NICs
            primary: false,
            // Only a single port currently
            slot: 0,
        };
    }

    /// Returns the default (nexus) firewall rules for a VPC with the given name.
    pub fn firewall_rules(
        vpc_name: &str,
    ) -> external::VpcFirewallRuleUpdateParams {
        serde_json::from_value(serde_json::json!({
            "rules": [
                {
                    "name": "allow-external-inbound",
                    "status": "enabled",
                    "direction": "inbound",
                    "targets": [ { "type": "vpc", "value": vpc_name } ],
                    "filters": { "ports": [ "80", "443" ], "protocols": [ "TCP" ] },
                    "action": "allow",
                    "priority": 65534,
                    "description": "allow inbound connections for HTTP and HTTPS from anywhere"
                }
                ]
        })).unwrap()
    }
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
