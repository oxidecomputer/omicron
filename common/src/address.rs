// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common IP addressing functionality.
//!
//! This addressing functionality is shared by both initialization services
//! and Nexus, who need to agree upon addressing schemes.

use crate::api::external::{self, Error};
use crate::policy::INTERNAL_DNS_REDUNDANCY;
use ipnetwork::Ipv6Network;
use oxnet::{Ipv4Net, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV6},
    sync::LazyLock,
};

pub const AZ_PREFIX: u8 = 48;
pub const RACK_PREFIX: u8 = 56;
pub const SLED_PREFIX: u8 = 64;

// Multicast constants

/// IPv4 Source-Specific Multicast (SSM) subnet as defined in RFC 4607:
/// <https://tools.ietf.org/html/rfc4607>.
pub const IPV4_SSM_SUBNET: oxnet::Ipv4Net =
    oxnet::Ipv4Net::new_unchecked(Ipv4Addr::new(232, 0, 0, 0), 8);

/// IPv6 Source-Specific Multicast (SSM) flag field value as defined in RFC 4607:
/// <https://tools.ietf.org/html/rfc4607>.
/// This is the flags nibble (high nibble of second byte) for FF3x::/32 addresses.
pub const IPV6_SSM_FLAG_FIELD: u8 = 3;

/// maximum possible value for a tcp or udp port
pub const MAX_PORT: u16 = u16::MAX;

/// minimum possible value for a tcp or udp port
pub const MIN_PORT: u16 = u16::MIN;

pub const DNS_PORT: u16 = 53;
pub const DNS_HTTP_PORT: u16 = 5353;
pub const SLED_AGENT_PORT: u16 = 12345;
pub const REPO_DEPOT_PORT: u16 = 12348;

pub const COCKROACH_PORT: u16 = 32221;
pub const COCKROACH_ADMIN_PORT: u16 = 32222;
pub const CRUCIBLE_PORT: u16 = 32345;
pub const CLICKHOUSE_HTTP_PORT: u16 = 8123;
pub const CLICKHOUSE_INTERSERVER_PORT: u16 = 9009;
pub const CLICKHOUSE_TCP_PORT: u16 = 9000;
pub const CLICKHOUSE_KEEPER_TCP_PORT: u16 = 9181;
pub const CLICKHOUSE_KEEPER_RAFT_PORT: u16 = 9234;
pub const CLICKHOUSE_ADMIN_PORT: u16 = 8888;
pub const OXIMETER_PORT: u16 = 12223;
pub const DENDRITE_PORT: u16 = 12224;
pub const LLDP_PORT: u16 = 12230;
pub const MGD_PORT: u16 = 4676;
pub const DDMD_PORT: u16 = 8000;
pub const MGS_PORT: u16 = 12225;
pub const WICKETD_PORT: u16 = 12226;
pub const BOOTSTRAP_ARTIFACT_PORT: u16 = 12227;
pub const CRUCIBLE_PANTRY_PORT: u16 = 17000;
pub const TFPORTD_PORT: u16 = 12231;
pub const NEXUS_INTERNAL_PORT: u16 = 12221;
pub const NEXUS_LOCKSTEP_PORT: u16 = 12232;

/// The port on which Nexus exposes its external API on the underlay network.
///
/// This is used by the `wicketd` Nexus proxy to allow external API access via
/// the rack's tech port.
pub const NEXUS_TECHPORT_EXTERNAL_PORT: u16 = 12228;

/// The port on which `wicketd` runs a Nexus external API proxy on the tech port
/// interface(s).
pub const WICKETD_NEXUS_PROXY_PORT: u16 = 12229;

/// The port on which NTP runs
pub const NTP_PORT: u16 = 123;

/// The port on which the NTP admin service exposes an HTTP interface
pub const NTP_ADMIN_PORT: u16 = 10123;

/// The length for all VPC IPv6 prefixes
pub const VPC_IPV6_PREFIX_LENGTH: u8 = 48;

/// The prefix length for all VPC subnets
pub const VPC_SUBNET_IPV6_PREFIX_LENGTH: u8 = 64;

// The number of ports available to an SNAT IP.
// Note that for static NAT, this value isn't used, and all ports are available.
//
// NOTE: This must be a power of 2. We're expecting to provide the Tofino with a
// port mask, e.g., a 16-bit mask such as `0b01...`, where those dots are any 14
// bits. This signifies the port range `[16384, 32768)`. Such a port mask only
// works when the port-ranges are limited to powers of 2, not arbitrary ranges.
//
// Also NOTE: This is not going to work if we modify this value across different
// versions of Nexus. Currently, we're considering a port range free simply by
// checking if the _first_ address in a range is free. However, we'll need to
// instead to check if a candidate port range has any overlap with an existing
// port range, which is more complicated. That's deferred until we actually have
// that situation (which may be as soon as allocating ephemeral IPs).
pub const NUM_SOURCE_NAT_PORTS: u16 = 1 << 14;

// Services that require external connectivity are given an OPTE port
// with a "Service VNIC" record. Like a "Guest VNIC", a service is
// placed within a VPC (a built-in services VPC), along with a VPC subnet.
// But unlike guest instances which are created at runtime by Nexus, these
// services are created by RSS early on. So, we have some fixed values
// used to bootstrap service OPTE ports. Each service kind uses a distinct
// VPC subnet which RSS will allocate addresses from for those services.
// The specific values aren't deployment-specific as they are virtualized
// within OPTE.

/// The IPv6 prefix assigned to the built-in services VPC.
// The specific prefix here was randomly chosen from the expected VPC
// prefix range (`fd00::/48`). See `random_vpc_ipv6_prefix`.
// Furthermore, all the below *_OPTE_IPV6_SUBNET constants are
// /64's within this prefix.
pub static SERVICE_VPC_IPV6_PREFIX: LazyLock<Ipv6Net> = LazyLock::new(|| {
    Ipv6Net::new(
        Ipv6Addr::new(0xfd77, 0xe9d2, 0x9cd9, 0, 0, 0, 0, 0),
        VPC_IPV6_PREFIX_LENGTH,
    )
    .unwrap()
});

/// The IPv4 subnet for External DNS OPTE ports.
pub static DNS_OPTE_IPV4_SUBNET: LazyLock<Ipv4Net> =
    LazyLock::new(|| Ipv4Net::new(Ipv4Addr::new(172, 30, 1, 0), 24).unwrap());

/// The IPv6 subnet for External DNS OPTE ports.
pub static DNS_OPTE_IPV6_SUBNET: LazyLock<Ipv6Net> = LazyLock::new(|| {
    Ipv6Net::new(
        Ipv6Addr::new(0xfd77, 0xe9d2, 0x9cd9, 1, 0, 0, 0, 0),
        VPC_SUBNET_IPV6_PREFIX_LENGTH,
    )
    .unwrap()
});

/// The IPv4 subnet for Nexus OPTE ports.
pub static NEXUS_OPTE_IPV4_SUBNET: LazyLock<Ipv4Net> =
    LazyLock::new(|| Ipv4Net::new(Ipv4Addr::new(172, 30, 2, 0), 24).unwrap());

/// The IPv6 subnet for Nexus OPTE ports.
pub static NEXUS_OPTE_IPV6_SUBNET: LazyLock<Ipv6Net> = LazyLock::new(|| {
    Ipv6Net::new(
        Ipv6Addr::new(0xfd77, 0xe9d2, 0x9cd9, 2, 0, 0, 0, 0),
        VPC_SUBNET_IPV6_PREFIX_LENGTH,
    )
    .unwrap()
});

/// The IPv4 subnet for Boundary NTP OPTE ports.
pub static NTP_OPTE_IPV4_SUBNET: LazyLock<Ipv4Net> =
    LazyLock::new(|| Ipv4Net::new(Ipv4Addr::new(172, 30, 3, 0), 24).unwrap());

/// The IPv6 subnet for Boundary NTP OPTE ports.
pub static NTP_OPTE_IPV6_SUBNET: LazyLock<Ipv6Net> = LazyLock::new(|| {
    Ipv6Net::new(
        Ipv6Addr::new(0xfd77, 0xe9d2, 0x9cd9, 3, 0, 0, 0, 0),
        VPC_SUBNET_IPV6_PREFIX_LENGTH,
    )
    .unwrap()
});

// Anycast is a mechanism in which a single IP address is shared by multiple
// devices, and the destination is located based on routing distance.
//
// This is covered by RFC 4291 in much more detail:
// <https://datatracker.ietf.org/doc/html/rfc4291#section-2.6>
//
// Anycast addresses are always the "zeroeth" address within a subnet.  We
// always explicitly skip these addresses within our network.
const _ANYCAST_ADDRESS_INDEX: usize = 0;
const DNS_ADDRESS_INDEX: usize = 1;
const GZ_ADDRESS_INDEX: usize = 2;

/// The maximum number of addresses per sled reserved for RSS.
pub const RSS_RESERVED_ADDRESSES: u16 = 32;

// The maximum number of addresses per sled reserved for control plane services.
pub const CP_SERVICES_RESERVED_ADDRESSES: u16 = 0xFFFF;

// Number of addresses reserved (by the Nexus deployment planner) for allocation
// by the sled itself.  This is currently used for the first two addresses of
// the sled subnet, which are used for the sled global zone and the switch zone,
// if any.  Note that RSS does not honor this yet (in fact, per the above
// RSS_RESERVED_ADDRESSES, it will _only_ choose from this range).  And
// historically, systems did not have this reservation at all.  So it's not safe
// to assume that addresses in this subnet are available.
pub const SLED_RESERVED_ADDRESSES: u16 = 32;

/// Wraps an [`Ipv6Net`] with a compile-time prefix length.
#[derive(
    Debug,
    Clone,
    Copy,
    JsonSchema,
    Serialize,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
#[schemars(rename = "Ipv6Subnet")]
pub struct Ipv6Subnet<const N: u8> {
    net: Ipv6Net,
}

impl<const N: u8> Ipv6Subnet<N> {
    pub fn new(addr: Ipv6Addr) -> Self {
        // Create a network with the compile-time prefix length.
        let net = Ipv6Net::new(addr, N).unwrap();
        // Ensure the address is set to within-prefix only components.
        let net = Ipv6Net::new(net.prefix(), N).unwrap();
        Self { net }
    }

    /// Returns the underlying network.
    pub fn net(&self) -> Ipv6Net {
        self.net
    }
}

impl<const N: u8> From<Ipv6Network> for Ipv6Subnet<N> {
    fn from(net: Ipv6Network) -> Self {
        // Ensure the address is set to within-prefix only components.
        let net = Ipv6Net::new(net.network(), N).unwrap();
        Self { net }
    }
}

// We need a custom Deserialize to ensure that the subnet is what we expect.
impl<'de, const N: u8> Deserialize<'de> for Ipv6Subnet<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Inner {
            net: Ipv6Net,
        }

        let Inner { net } = Inner::deserialize(deserializer)?;
        if net.width() == N {
            Ok(Self { net })
        } else {
            Err(<D::Error as serde::de::Error>::custom(format!(
                "expected prefix {} but found {}",
                N,
                net.width(),
            )))
        }
    }
}

/// Represents a subnet which may be used for contacting DNS services.
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct DnsSubnet {
    subnet: Ipv6Subnet<SLED_PREFIX>,
}

impl DnsSubnet {
    pub fn new(subnet: Ipv6Subnet<SLED_PREFIX>) -> Self {
        Self { subnet }
    }

    /// Makes a new DNS subnet from the high-order bits of an address.
    pub fn from_addr(addr: Ipv6Addr) -> Self {
        Self::new(Ipv6Subnet::new(addr))
    }

    /// Returns the DNS subnet.
    pub fn subnet(&self) -> Ipv6Subnet<SLED_PREFIX> {
        self.subnet
    }

    /// Returns the reserved rack subnet that contains this DNS subnet.
    pub fn rack_subnet(&self) -> ReservedRackSubnet {
        ReservedRackSubnet::from_subnet(self.subnet)
    }

    /// Returns the DNS server address within the subnet.
    ///
    /// This is the first address within the subnet.
    pub fn dns_address(&self) -> Ipv6Addr {
        self.subnet.net().nth(DNS_ADDRESS_INDEX as u128).unwrap()
    }

    /// Returns the address which the Global Zone should create
    /// to be able to contact the DNS server.
    ///
    /// This is the second address within the subnet.
    pub fn gz_address(&self) -> Ipv6Addr {
        self.subnet.net().nth(GZ_ADDRESS_INDEX as u128).unwrap()
    }
}

/// A wrapper around an IPv6 network, indicating it is a "reserved" rack
/// subnet which can be used for AZ-wide services.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReservedRackSubnet(pub Ipv6Subnet<RACK_PREFIX>);

impl ReservedRackSubnet {
    /// Returns the subnet for the reserved rack subnet.
    pub fn new(subnet: Ipv6Subnet<AZ_PREFIX>) -> Self {
        ReservedRackSubnet(Ipv6Subnet::<RACK_PREFIX>::new(subnet.net().addr()))
    }

    /// Infer the reserved rack subnet from a sled/AZ/DNS subnet.
    pub fn from_subnet<const N: u8>(subnet: Ipv6Subnet<N>) -> Self {
        Self::new(Ipv6Subnet::<AZ_PREFIX>::new(subnet.net().addr()))
    }

    /// Returns the `index`th DNS subnet from this reserved rack subnet.
    pub fn get_dns_subnet(&self, index: u8) -> DnsSubnet {
        DnsSubnet::new(get_64_subnet(self.0, index))
    }

    /// Returns the DNS addresses from this reserved rack subnet.
    ///
    /// These addresses will come from the first [`INTERNAL_DNS_REDUNDANCY`]
    /// `/64s` of the [`RACK_PREFIX`] subnet.
    pub fn get_dns_subnets(&self) -> Vec<DnsSubnet> {
        (0..INTERNAL_DNS_REDUNDANCY)
            .map(|idx| self.get_dns_subnet(u8::try_from(idx + 1).unwrap()))
            .collect()
    }
}

/// Return the list of DNS servers for the rack, given any address in the AZ
/// subnet
pub fn get_internal_dns_server_addresses(addr: Ipv6Addr) -> Vec<IpAddr> {
    let az_subnet = Ipv6Subnet::<AZ_PREFIX>::new(addr);
    ReservedRackSubnet::new(az_subnet)
        .get_dns_subnets()
        .iter()
        .map(|dns_subnet| IpAddr::from(dns_subnet.dns_address()))
        .collect()
}

const SLED_AGENT_ADDRESS_INDEX: usize = 1;
const SWITCH_ZONE_ADDRESS_INDEX: usize = 2;

/// Return the sled agent address for a subnet.
///
/// This address will come from the first address of the [`SLED_PREFIX`] subnet.
pub fn get_sled_address(sled_subnet: Ipv6Subnet<SLED_PREFIX>) -> SocketAddrV6 {
    let sled_agent_ip =
        sled_subnet.net().nth(SLED_AGENT_ADDRESS_INDEX as u128).unwrap();
    SocketAddrV6::new(sled_agent_ip, SLED_AGENT_PORT, 0, 0)
}

/// Return the switch zone address for a subnet.
///
/// This address will come from the second address of the [`SLED_PREFIX`] subnet.
pub fn get_switch_zone_address(
    sled_subnet: Ipv6Subnet<SLED_PREFIX>,
) -> Ipv6Addr {
    sled_subnet.net().nth(SWITCH_ZONE_ADDRESS_INDEX as u128).unwrap()
}

/// Returns a sled subnet within a rack subnet.
///
/// The subnet at index == 0 is used for rack-local services.
pub fn get_64_subnet(
    rack_subnet: Ipv6Subnet<RACK_PREFIX>,
    index: u8,
) -> Ipv6Subnet<SLED_PREFIX> {
    let mut rack_network = rack_subnet.net().addr().octets();

    // To set bits distinguishing the /64 from the /56, we modify the 7th octet.
    rack_network[7] = index;
    Ipv6Subnet::<SLED_PREFIX>::new(Ipv6Addr::from(rack_network))
}

/// The IP address version.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IpVersion {
    V4,
    V6,
}

impl IpVersion {
    pub const fn v4() -> IpVersion {
        IpVersion::V4
    }
}

impl std::fmt::Display for IpVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::V4 => write!(f, "v4"),
            Self::V6 => write!(f, "v6"),
        }
    }
}

/// An IP Range is a contiguous range of IP addresses, usually within an IP
/// Pool.
///
/// The first address in the range is guaranteed to be no greater than the last
/// address.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, Ord, PartialOrd,
)]
#[serde(untagged)]
pub enum IpRange {
    V4(Ipv4Range),
    V6(Ipv6Range),
}

// NOTE: We don't derive JsonSchema. That's intended so that we can use an
// untagged enum for `IpRange`, and use this method to annotate schemars output
// for client-generators (e.g., progenitor) to use in generating a better
// client.
impl JsonSchema for IpRange {
    fn schema_name() -> String {
        "IpRange".to_string()
    }

    fn json_schema(
        gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            subschemas: Some(Box::new(schemars::schema::SubschemaValidation {
                one_of: Some(vec![
                    external::label_schema(
                        "v4",
                        gen.subschema_for::<Ipv4Range>(),
                    ),
                    external::label_schema(
                        "v6",
                        gen.subschema_for::<Ipv6Range>(),
                    ),
                ]),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

impl IpRange {
    pub fn contains(&self, addr: IpAddr) -> bool {
        match (self, addr) {
            (IpRange::V4(r), IpAddr::V4(addr)) => r.contains(addr),
            (IpRange::V6(r), IpAddr::V6(addr)) => r.contains(addr),
            (IpRange::V6(_), IpAddr::V4(_))
            | (IpRange::V4(_), IpAddr::V6(_)) => false,
        }
    }

    pub fn first_address(&self) -> IpAddr {
        match self {
            IpRange::V4(inner) => IpAddr::from(inner.first),
            IpRange::V6(inner) => IpAddr::from(inner.first),
        }
    }

    pub fn last_address(&self) -> IpAddr {
        match self {
            IpRange::V4(inner) => IpAddr::from(inner.last),
            IpRange::V6(inner) => IpAddr::from(inner.last),
        }
    }

    pub fn iter(&self) -> IpRangeIter {
        match self {
            IpRange::V4(ip4) => IpRangeIter::V4(ip4.iter()),
            IpRange::V6(ip6) => IpRangeIter::V6(ip6.iter()),
        }
    }

    // Has to be u128 to accommodate IPv6
    pub fn len(&self) -> u128 {
        match self {
            IpRange::V4(ip4) => u128::from(ip4.len()),
            IpRange::V6(ip6) => ip6.len(),
        }
    }

    /// Return true if this is an IPv4 range, and false for IPv6.
    pub fn is_ipv4(&self) -> bool {
        matches!(self, IpRange::V4(_))
    }

    /// Return true if this is an IPv6 range, and false for IPv4.
    pub fn is_ipv6(&self) -> bool {
        matches!(self, IpRange::V6(_))
    }

    /// Return the IP version of this range.
    pub fn version(&self) -> IpVersion {
        match self {
            IpRange::V4(_) => IpVersion::V4,
            IpRange::V6(_) => IpVersion::V6,
        }
    }
}

impl From<IpAddr> for IpRange {
    fn from(addr: IpAddr) -> Self {
        match addr {
            IpAddr::V4(addr) => IpRange::V4(Ipv4Range::from(addr)),
            IpAddr::V6(addr) => IpRange::V6(Ipv6Range::from(addr)),
        }
    }
}

impl TryFrom<(IpAddr, IpAddr)> for IpRange {
    type Error = String;

    fn try_from(pair: (IpAddr, IpAddr)) -> Result<Self, Self::Error> {
        match (pair.0, pair.1) {
            (IpAddr::V4(a), IpAddr::V4(b)) => Self::try_from((a, b)),
            (IpAddr::V6(a), IpAddr::V6(b)) => Self::try_from((a, b)),
            (IpAddr::V4(_), IpAddr::V6(_)) | (IpAddr::V6(_), IpAddr::V4(_)) => {
                Err("IP address ranges cannot mix IPv4 and IPv6".to_string())
            }
        }
    }
}

impl TryFrom<(Ipv4Addr, Ipv4Addr)> for IpRange {
    type Error = String;

    fn try_from(pair: (Ipv4Addr, Ipv4Addr)) -> Result<Self, Self::Error> {
        Ipv4Range::new(pair.0, pair.1).map(IpRange::V4)
    }
}

impl TryFrom<(Ipv6Addr, Ipv6Addr)> for IpRange {
    type Error = String;

    fn try_from(pair: (Ipv6Addr, Ipv6Addr)) -> Result<Self, Self::Error> {
        Ipv6Range::new(pair.0, pair.1).map(IpRange::V6)
    }
}

impl From<Ipv4Range> for IpRange {
    fn from(value: Ipv4Range) -> Self {
        Self::V4(value)
    }
}

impl From<Ipv6Range> for IpRange {
    fn from(value: Ipv6Range) -> Self {
        Self::V6(value)
    }
}

/// A non-decreasing IPv4 address range, inclusive of both ends.
///
/// The first address must be less than or equal to the last address.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialOrd,
    Ord,
)]
#[serde(try_from = "AnyIpv4Range")]
pub struct Ipv4Range {
    pub first: Ipv4Addr,
    pub last: Ipv4Addr,
}

impl Ipv4Range {
    pub fn new(first: Ipv4Addr, last: Ipv4Addr) -> Result<Self, String> {
        if first <= last {
            Ok(Self { first, last })
        } else {
            Err(String::from("IP address ranges must be non-decreasing"))
        }
    }

    pub fn contains(&self, addr: Ipv4Addr) -> bool {
        self.first <= addr && addr <= self.last
    }

    pub fn first_address(&self) -> Ipv4Addr {
        self.first
    }

    pub fn last_address(&self) -> Ipv4Addr {
        self.last
    }

    pub fn iter(&self) -> Ipv4RangeIter {
        Ipv4RangeIter { next: Some(self.first.into()), last: self.last.into() }
    }

    pub fn len(&self) -> u32 {
        let start_num = u32::from(self.first);
        let end_num = u32::from(self.last);
        end_num - start_num + 1
    }
}

impl From<Ipv4Addr> for Ipv4Range {
    fn from(addr: Ipv4Addr) -> Self {
        Self { first: addr, last: addr }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct AnyIpv4Range {
    first: Ipv4Addr,
    last: Ipv4Addr,
}

impl TryFrom<AnyIpv4Range> for Ipv4Range {
    type Error = Error;
    fn try_from(r: AnyIpv4Range) -> Result<Self, Self::Error> {
        Ipv4Range::new(r.first, r.last)
            .map_err(|msg| Error::invalid_request(msg.as_str()))
    }
}

/// A non-decreasing IPv6 address range, inclusive of both ends.
///
/// The first address must be less than or equal to the last address.
#[derive(
    PartialOrd,
    Ord,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(try_from = "AnyIpv6Range")]
pub struct Ipv6Range {
    pub first: Ipv6Addr,
    pub last: Ipv6Addr,
}

impl Ipv6Range {
    pub fn new(first: Ipv6Addr, last: Ipv6Addr) -> Result<Self, String> {
        if first <= last {
            Ok(Self { first, last })
        } else {
            Err(String::from("IP address ranges must be non-decreasing"))
        }
    }

    pub fn contains(&self, addr: Ipv6Addr) -> bool {
        self.first <= addr && addr <= self.last
    }

    pub fn first_address(&self) -> Ipv6Addr {
        self.first
    }

    pub fn last_address(&self) -> Ipv6Addr {
        self.last
    }

    pub fn iter(&self) -> Ipv6RangeIter {
        Ipv6RangeIter { next: Some(self.first.into()), last: self.last.into() }
    }

    pub fn len(&self) -> u128 {
        let start_num = u128::from(self.first);
        let end_num = u128::from(self.last);
        end_num - start_num + 1
    }
}

impl From<Ipv6Addr> for Ipv6Range {
    fn from(addr: Ipv6Addr) -> Self {
        Self { first: addr, last: addr }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct AnyIpv6Range {
    first: Ipv6Addr,
    last: Ipv6Addr,
}

impl TryFrom<AnyIpv6Range> for Ipv6Range {
    type Error = Error;
    fn try_from(r: AnyIpv6Range) -> Result<Self, Self::Error> {
        Ipv6Range::new(r.first, r.last)
            .map_err(|msg| Error::invalid_request(msg.as_str()))
    }
}

pub struct Ipv4RangeIter {
    next: Option<u32>,
    last: u32,
}

impl Iterator for Ipv4RangeIter {
    type Item = Ipv4Addr;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.next?;
        if next < self.last {
            self.next = Some(next + 1);
        } else {
            self.next = None;
        }
        Some(next.into())
    }
}

pub struct Ipv6RangeIter {
    next: Option<u128>,
    last: u128,
}

impl Iterator for Ipv6RangeIter {
    type Item = Ipv6Addr;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.next?;
        if next < self.last {
            self.next = Some(next + 1);
        } else {
            self.next = None;
        }
        Some(next.into())
    }
}

pub enum IpRangeIter {
    V4(Ipv4RangeIter),
    V6(Ipv6RangeIter),
}

impl Iterator for IpRangeIter {
    type Item = IpAddr;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::V4(iter) => iter.next().map(IpAddr::V4),
            Self::V6(iter) => iter.next().map(IpAddr::V6),
        }
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_dns_subnets() {
        let subnet = Ipv6Subnet::<AZ_PREFIX>::new(
            "fd00:1122:3344:0100::".parse::<Ipv6Addr>().unwrap(),
        );
        let rack_subnet = ReservedRackSubnet::new(subnet);

        assert_eq!(
            //              Note that these bits (indicating the rack) are zero.
            //              vv
            "fd00:1122:3344:0000::/56".parse::<Ipv6Net>().unwrap(),
            rack_subnet.0.net(),
        );

        // Observe the first DNS subnet within this reserved rack subnet.
        let dns_subnets = rack_subnet.get_dns_subnets();
        assert_eq!(INTERNAL_DNS_REDUNDANCY, dns_subnets.len());

        // The DNS address and GZ address should be only differing by one.
        assert_eq!(
            "fd00:1122:3344:0001::1".parse::<Ipv6Addr>().unwrap(),
            dns_subnets[0].dns_address(),
        );
        assert_eq!(
            "fd00:1122:3344:0001::2".parse::<Ipv6Addr>().unwrap(),
            dns_subnets[0].gz_address(),
        );
    }

    #[test]
    fn test_sled_address() {
        let subnet = Ipv6Subnet::<SLED_PREFIX>::new(
            "fd00:1122:3344:0101::".parse::<Ipv6Addr>().unwrap(),
        );
        assert_eq!(
            "[fd00:1122:3344:0101::1]:12345".parse::<SocketAddrV6>().unwrap(),
            get_sled_address(subnet)
        );

        let subnet = Ipv6Subnet::<SLED_PREFIX>::new(
            "fd00:1122:3344:0308::".parse::<Ipv6Addr>().unwrap(),
        );
        assert_eq!(
            "[fd00:1122:3344:0308::1]:12345".parse::<SocketAddrV6>().unwrap(),
            get_sled_address(subnet)
        );
    }

    #[test]
    fn test_ip_range_checks_non_decreasing() {
        let lo = Ipv4Addr::new(10, 0, 0, 1);
        let hi = Ipv4Addr::new(10, 0, 0, 3);
        assert!(Ipv4Range::new(lo, hi).is_ok());
        assert!(Ipv4Range::new(lo, lo).is_ok());
        assert!(Ipv4Range::new(hi, lo).is_err());

        let lo = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1);
        let hi = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 3);
        assert!(Ipv6Range::new(lo, hi).is_ok());
        assert!(Ipv6Range::new(lo, lo).is_ok());
        assert!(Ipv6Range::new(hi, lo).is_err());
    }

    #[test]
    fn test_ip_range_enum_deserialization() {
        let data = r#"{"first": "10.0.0.1", "last": "10.0.0.3"}"#;
        let expected = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(10, 0, 0, 1),
                Ipv4Addr::new(10, 0, 0, 3),
            )
            .unwrap(),
        );
        assert_eq!(expected, serde_json::from_str(data).unwrap());

        let data = r#"{"first": "fd00::", "last": "fd00::3"}"#;
        let expected = IpRange::V6(
            Ipv6Range::new(
                Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0),
                Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 3),
            )
            .unwrap(),
        );
        assert_eq!(expected, serde_json::from_str(data).unwrap());

        let data = r#"{"first": "fd00::3", "last": "fd00::"}"#;
        assert!(
            serde_json::from_str::<IpRange>(data).is_err(),
            "Expected an error deserializing an IP range with first address \
            greater than last address",
        );
    }

    #[test]
    fn test_ip_range_try_from() {
        let lo = Ipv4Addr::new(10, 0, 0, 1);
        let hi = Ipv4Addr::new(10, 0, 0, 3);
        assert!(IpRange::try_from((lo, hi)).is_ok());
        assert!(IpRange::try_from((hi, lo)).is_err());

        let lo = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1);
        let hi = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 3);
        assert!(IpRange::try_from((lo, hi)).is_ok());
        assert!(IpRange::try_from((hi, lo)).is_err());
    }

    #[test]
    fn test_ip_range_iter() {
        let lo = Ipv4Addr::new(10, 0, 0, 1);
        let hi = Ipv4Addr::new(10, 0, 0, 3);
        let range = IpRange::try_from((lo, hi)).unwrap();
        let ips = range.iter().collect::<Vec<_>>();
        assert_eq!(
            ips,
            vec![
                Ipv4Addr::new(10, 0, 0, 1),
                Ipv4Addr::new(10, 0, 0, 2),
                Ipv4Addr::new(10, 0, 0, 3),
            ]
        );

        let lo = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1);
        let hi = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 3);
        let range = IpRange::try_from((lo, hi)).unwrap();
        let ips = range.iter().collect::<Vec<_>>();
        assert_eq!(
            ips,
            vec![
                Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1),
                Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 2),
                Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 3),
            ]
        );
    }

    #[test]
    fn test_ip_range_length() {
        let lo = Ipv4Addr::new(10, 0, 0, 1);
        let hi = Ipv4Addr::new(10, 0, 0, 3);
        let range = IpRange::try_from((lo, hi)).unwrap();
        assert_eq!(range.len(), 3);

        let lo = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1);
        let hi = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 1, 3);
        let range = IpRange::try_from((lo, hi)).unwrap();
        assert_eq!(range.len(), 2u128.pow(16) + 3);
    }

    #[test]
    fn test_ipv6_subnet_deserialize() {
        let value = json!({
            "net": "ff12::3456/64"
        });

        assert!(
            serde_json::from_value::<Ipv6Subnet<64>>(value.clone()).is_ok()
        );
        assert!(serde_json::from_value::<Ipv6Subnet<56>>(value).is_err());
    }
}
