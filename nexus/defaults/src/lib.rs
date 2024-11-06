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
use std::num::NonZeroU32;

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

/// Capacity limits for one table on the Tofino ASIC.
///
/// The ASIC uses a set of match-action tables to do its job, such as for
/// routing packets out of the rack to customer networks. These tables have a
/// size fixed by the P4 program loaded onto the ASIC at runtime.
///
/// The source for these values is `dendrite/p4/constants.p4`, which defines the
/// _desired_ size for each table. However, the actual available size of each
/// table may be slightly less, as some tables include default entries. The
/// concrete values for these sizes that we actually get are defined in
/// `dendrite/dpd-client/tests/integration_tests/table_tests.rs`, which are
/// determined empirically after loading the P4 program.
#[derive(Clone, Copy, Debug)]
pub struct AsicTableCapacity {
    // The total capacity of the ASIC table.
    total_capacity: NonZeroU32,
    // The number of entries reserved for the Oxide control plane.
    n_reserved: u32,
    // The number of entries left that can be consumed by API resources.
    n_allocatable: u32,
}

impl AsicTableCapacity {
    // Construct a new table size.
    //
    // # Panics
    //
    // This panics if `n_reserved` is greater than `size`, or if `size` is zero.
    const fn new(size: u32, n_reserved: u32) -> Self {
        assert!(size > 0, "`size` must be non-zero");
        assert!(size >= n_reserved, "`size` must be >= `n_reserved");
        Self {
            total_capacity: unsafe { NonZeroU32::new_unchecked(size) },
            n_reserved,
            n_allocatable: size - n_reserved,
        }
    }

    /// Return the total capacity of the table.
    pub const fn total_capacity(&self) -> NonZeroU32 {
        self.total_capacity
    }

    /// Return the number of entries reserved for the Oxide control plane.
    pub const fn n_reserved(&self) -> u32 {
        self.n_reserved
    }

    /// Return the number of entries that can be allocated and consumed by API
    /// resources.
    pub const fn n_allocatable(&self) -> u32 {
        self.n_allocatable
    }
}

/// Total size limit on the IPv4 routing table.
pub static IPV4_ROUTING_TABLE: AsicTableCapacity =
    AsicTableCapacity::new(4091, 0);

/// Total size limit on the IPv6 routing table.
pub static IPV6_ROUTING_TABLE: AsicTableCapacity =
    AsicTableCapacity::new(1023, 0);

/// Total size limit on the IPv4 address table.
pub static IPV4_ADDRESS_TABLE: AsicTableCapacity =
    AsicTableCapacity::new(511, 0);

/// Total size limit on the IPv6 address table.
pub static IPV6_ADDRESS_TABLE: AsicTableCapacity =
    AsicTableCapacity::new(511, 0);

/// Total size limit on the IPv4 NAT table.
pub static IPV4_NAT_TABLE: AsicTableCapacity = AsicTableCapacity::new(1024, 16);

/// Total size limit on the IPv6 NAT table.
pub static IPV6_NAT_TABLE: AsicTableCapacity = AsicTableCapacity::new(1024, 16);

/// Total size limit on the IPv4 ARP table.
pub static IPV4_ARP_TABLE: AsicTableCapacity = AsicTableCapacity::new(512, 0);

/// Total size limit on the IPv6 neighbor (NDP) table.
pub static IPV6_NEIGHBOR_TABLE: AsicTableCapacity =
    AsicTableCapacity::new(512, 0);

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
