// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::anyhow;
use daft::Diffable;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::TriHashItem;
use iddqd::TriHashMap;
use iddqd::tri_upcast;
use omicron_common::api::external::IpVersion;
use omicron_common::api::external::MacAddr;
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_uuid_kinds::ExternalIpUuid;
use omicron_uuid_kinds::GenericUuid as _;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::VnicUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::inventory::ExternalDnsAddrs;
use sled_agent_types::inventory::NexusExternalIps;
use sled_agent_types::inventory::SourceNatConfigGeneric;
use sled_agent_types::inventory::SourceNatConfigV4;
use sled_agent_types::inventory::SourceNatConfigV6;
use sled_agent_types::inventory::ZoneExternalAddrsError;
use sled_agent_types::inventory::ZoneSnatConfig;
use sled_agent_types::inventory::check_external_ip_count;
use std::collections::HashSet;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use thiserror::Error;
use uuid::Uuid;

/// Tracker and validator for network resources allocated to Omicron-managed
/// zones.
///
/// ## Implementation notes
///
/// `OmicronZoneNetworkResources` consists of two 1:1:1 "trijective" maps:
///
/// 1. Providing a unique map for Omicron zone IDs, external IP IDs, and
///    external IPs.
/// 2. Providing a unique map for Omicron zone IDs, vNIC IDs, and vNICs.
///
/// One question that arises: should there instead be a single 1:1:1:1:1 map?
/// In other words, is there a 1:1 mapping between external IPs and vNICs as
/// well? The answer is "generally yes", but:
///
/// - They're not stored in the database that way, and it's possible that
///   there's some divergence.
/// - We currently don't plan to get any utility out of asserting the 1:1:1:1:1
///   map. The main planned use of this is for expunged zone garbage collection
///   -- while that benefits from trijective maps tremendously, there's no
///   additional value in asserting a unique mapping between external IPs and
///   vNICs.
///
/// So we use two separate maps for now. But a single map is always a
/// possibility in the future, if required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmicronZoneNetworkResources {
    /// external IPs allocated to Omicron zones
    omicron_zone_external_ips: TriHashMap<OmicronZoneExternalIpEntry>,

    /// vNICs allocated to Omicron zones
    omicron_zone_nics: TriHashMap<OmicronZoneNicEntry>,
}

impl OmicronZoneNetworkResources {
    pub fn new() -> Self {
        Self {
            omicron_zone_external_ips: TriHashMap::new(),
            omicron_zone_nics: TriHashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.omicron_zone_external_ips.is_empty()
            && self.omicron_zone_nics.is_empty()
    }

    pub fn omicron_zone_external_ips(
        &self,
    ) -> impl Iterator<Item = OmicronZoneExternalIpEntry> + '_ {
        self.omicron_zone_external_ips.iter().copied()
    }

    pub fn omicron_zone_nics(
        &self,
    ) -> impl Iterator<Item = OmicronZoneNicEntry> + '_ {
        self.omicron_zone_nics.iter().copied()
    }

    // When adding a new external IP, check that if we already have a NIC for
    // this zone, it has a private IP of the same version as the external IP.
    fn check_existing_nic_supports_external_ip(
        &self,
        zone_id: &OmicronZoneUuid,
        ip: &OmicronZoneExternalIp,
    ) -> Result<(), AddNetworkResourceError> {
        if let Some(OmicronZoneNicEntry { nic, .. }) =
            self.omicron_zone_nics.get1(zone_id)
        {
            return self.check_external_and_private_ips_are_consistent(
                zone_id,
                ip.ip(),
                nic,
            );
        }
        Ok(())
    }

    // When adding a new NIC, check that if we already have an external IP for
    // this zone, it has a private IP of the same version as the external IP.
    fn check_nic_supports_existing_external_ip(
        &self,
        zone_id: &OmicronZoneUuid,
        nic: &OmicronZoneNic,
    ) -> Result<(), AddNetworkResourceError> {
        if let Some(OmicronZoneExternalIpEntry { ip, .. }) =
            self.omicron_zone_external_ips.get1(zone_id)
        {
            return self.check_external_and_private_ips_are_consistent(
                zone_id,
                ip.ip(),
                nic,
            );
        }
        Ok(())
    }

    fn check_external_and_private_ips_are_consistent(
        &self,
        zone_id: &OmicronZoneUuid,
        external_ip: IpAddr,
        nic: &OmicronZoneNic,
    ) -> Result<(), AddNetworkResourceError> {
        if external_ip.is_ipv4() && nic.ip.ipv4().is_none() {
            return Err(AddNetworkResourceError::NoPrivateIpForExternalIp {
                zone_id: *zone_id,
                nic_id: nic.id,
                external_ip,
            });
        }
        if external_ip.is_ipv6() && nic.ip.ipv6().is_none() {
            return Err(AddNetworkResourceError::NoPrivateIpForExternalIp {
                zone_id: *zone_id,
                nic_id: nic.id,
                external_ip,
            });
        }
        Ok(())
    }

    pub fn add_external_ip(
        &mut self,
        zone_id: OmicronZoneUuid,
        ip: OmicronZoneExternalIp,
    ) -> Result<(), AddNetworkResourceError> {
        self.check_existing_nic_supports_external_ip(&zone_id, &ip)?;
        let entry = OmicronZoneExternalIpEntry { zone_id, ip };
        self.omicron_zone_external_ips.insert_unique(entry).map_err(|err| {
            AddNetworkResourceError::DuplicateOmicronZoneExternalIp {
                zone_id,
                ip,
                err: anyhow!(err.into_owned()),
            }
        })
    }

    pub fn add_nic(
        &mut self,
        zone_id: OmicronZoneUuid,
        nic: OmicronZoneNic,
    ) -> Result<(), AddNetworkResourceError> {
        self.check_nic_supports_existing_external_ip(&zone_id, &nic)?;
        let entry = OmicronZoneNicEntry { zone_id, nic };
        self.omicron_zone_nics.insert_unique(entry).map_err(|err| {
            AddNetworkResourceError::DuplicateOmicronZoneNic {
                zone_id,
                nic,
                err: anyhow!(err.into_owned()),
            }
        })
    }

    pub fn get_external_ip_by_zone_id(
        &self,
        zone_id: OmicronZoneUuid,
    ) -> Option<&OmicronZoneExternalIpEntry> {
        self.omicron_zone_external_ips.get1(&zone_id)
    }

    pub fn get_external_ip_by_external_ip_id(
        &self,
        ip: ExternalIpUuid,
    ) -> Option<&OmicronZoneExternalIpEntry> {
        self.omicron_zone_external_ips.get2(&ip)
    }

    pub fn get_external_ip_by_ip(
        &self,
        ip: OmicronZoneExternalIpKey,
    ) -> Option<&OmicronZoneExternalIpEntry> {
        self.omicron_zone_external_ips.get3(&ip)
    }

    pub fn get_nic_by_zone_id(
        &self,
        zone_id: OmicronZoneUuid,
    ) -> Option<&OmicronZoneNicEntry> {
        self.omicron_zone_nics.get1(&zone_id)
    }

    pub fn get_nic_by_vnic_id(
        &self,
        vnic_id: VnicUuid,
    ) -> Option<&OmicronZoneNicEntry> {
        self.omicron_zone_nics.get2(&vnic_id)
    }

    pub fn get_nic_by_mac(&self, mac: MacAddr) -> Option<&OmicronZoneNicEntry> {
        self.omicron_zone_nics.get3(&mac)
    }
}

/// External IP variants possible for Omicron-managed zones.
#[derive(
    Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize,
)]
pub enum OmicronZoneExternalIp {
    Floating(OmicronZoneExternalFloatingIp),
    Snat(OmicronZoneExternalSnatIp),
    // We may eventually want `Ephemeral(_)` too (arguably Nexus could be
    // ephemeral?), but for now we only have Floating and Snat uses.
}

impl OmicronZoneExternalIp {
    pub fn id(&self) -> ExternalIpUuid {
        match self {
            OmicronZoneExternalIp::Floating(ext) => ext.id,
            OmicronZoneExternalIp::Snat(ext) => ext.id,
        }
    }

    pub fn ip(&self) -> IpAddr {
        match self {
            OmicronZoneExternalIp::Floating(ext) => ext.ip,
            OmicronZoneExternalIp::Snat(ext) => ext.snat_cfg.ip,
        }
    }

    pub fn ip_key(&self) -> OmicronZoneExternalIpKey {
        match self {
            OmicronZoneExternalIp::Floating(ip) => {
                OmicronZoneExternalIpKey::Floating(ip.ip)
            }
            OmicronZoneExternalIp::Snat(snat) => {
                OmicronZoneExternalIpKey::Snat(snat.snat_cfg)
            }
        }
    }

    /// Return the IP version of the contained address.
    pub fn ip_version(&self) -> IpVersion {
        match self.ip() {
            IpAddr::V4(_) => IpVersion::V4,
            IpAddr::V6(_) => IpVersion::V6,
        }
    }
}

/// An IP-based key suitable for uniquely identifying an
/// [`OmicronZoneExternalIp`].
///
/// We can't use the IP itself to uniquely identify an external IP because SNAT
/// IPs can have overlapping addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OmicronZoneExternalIpKey {
    Floating(IpAddr),
    Snat(SourceNatConfigGeneric),
}

/// Floating external IP allocated to an Omicron-managed zone.
///
/// This is a slimmer `nexus_db_model::ExternalIp` that only stores the fields
/// necessary for blueprint planning.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    JsonSchema,
    Serialize,
    Deserialize,
    Diffable,
)]
pub struct OmicronZoneExternalFloatingIp {
    pub id: ExternalIpUuid,
    pub ip: IpAddr,
}

impl IdOrdItem for OmicronZoneExternalFloatingIp {
    type Key<'a> = IpAddr;

    fn key(&self) -> Self::Key<'_> {
        self.ip
    }

    iddqd::id_upcast!();
}

/// Floating external address with port allocated to an Omicron-managed zone.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    JsonSchema,
    Serialize,
    Deserialize,
    Diffable,
)]
pub struct OmicronZoneExternalFloatingAddr {
    pub id: ExternalIpUuid,
    pub addr: SocketAddr,
}

impl IdOrdItem for OmicronZoneExternalFloatingAddr {
    type Key<'a> = IpAddr;

    fn key(&self) -> Self::Key<'_> {
        self.addr.ip()
    }

    iddqd::id_upcast!();
}

impl OmicronZoneExternalFloatingAddr {
    pub fn into_ip(self) -> OmicronZoneExternalFloatingIp {
        OmicronZoneExternalFloatingIp { id: self.id, ip: self.addr.ip() }
    }
}

/// SNAT (outbound) external IP allocated to an Omicron-managed zone.
///
/// This is a slimmer `nexus_db_model::ExternalIp` that only stores the fields
/// necessary for blueprint planning.
#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    JsonSchema,
    Serialize,
    Deserialize,
    Diffable,
)]
pub struct OmicronZoneExternalSnatIp {
    pub id: ExternalIpUuid,
    pub snat_cfg: SourceNatConfigGeneric,
}

/// An IPv4 SNAT external IP allocated to an Omicron-managed zone.
///
/// The family-typed analog of [`OmicronZoneExternalSnatIp`], used in the
/// variants of [`OmicronZoneExternalSnat`] so the enum can't hold an address of
/// the wrong family.
#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    JsonSchema,
    Serialize,
    Deserialize,
    Diffable,
)]
pub struct OmicronZoneExternalSnatIpv4 {
    pub id: ExternalIpUuid,
    pub snat_cfg: SourceNatConfigV4,
}

impl From<OmicronZoneExternalSnatIpv4> for OmicronZoneExternalSnatIp {
    fn from(value: OmicronZoneExternalSnatIpv4) -> Self {
        OmicronZoneExternalSnatIp {
            id: value.id,
            snat_cfg: value.snat_cfg.into(),
        }
    }
}

/// An IPv6 SNAT external IP allocated to an Omicron-managed zone.
///
/// The family-typed analog of [`OmicronZoneExternalSnatIp`], used in the
/// variants of [`OmicronZoneExternalSnat`] so the enum can't hold an address of
/// the wrong family.
#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    JsonSchema,
    Serialize,
    Deserialize,
    Diffable,
)]
pub struct OmicronZoneExternalSnatIpv6 {
    pub id: ExternalIpUuid,
    pub snat_cfg: SourceNatConfigV6,
}

impl From<OmicronZoneExternalSnatIpv6> for OmicronZoneExternalSnatIp {
    fn from(value: OmicronZoneExternalSnatIpv6) -> Self {
        OmicronZoneExternalSnatIp {
            id: value.id,
            snat_cfg: value.snat_cfg.into(),
        }
    }
}

/// A set of `OmicronZoneExternalFloatingIp`s allocated to a single zone.
///
/// The set of IPs is always non-empty, and there are no duplicate IP addresses.
/// Also, the size is bounded above by `MAX_ZONE_EXTERNAL_IPS`.
///
/// NOTE: This is the reconfigurator analog of the inventory `NexusExternalIps`
/// type.
#[derive(
    Debug, Clone, PartialEq, Eq, JsonSchema, Serialize, Deserialize, Diffable,
)]
#[daft(leaf)]
#[serde(
    try_from = "IdOrdMap<OmicronZoneExternalFloatingIp>",
    into = "IdOrdMap<OmicronZoneExternalFloatingIp>"
)]
pub struct OmicronZoneExternalFloatingIps(
    #[schemars(length(
        min = 1,
        max = "sled_agent_types::inventory::MAX_ZONE_EXTERNAL_IPS"
    ))]
    IdOrdMap<OmicronZoneExternalFloatingIp>,
);

impl std::cmp::PartialOrd for OmicronZoneExternalFloatingIps {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for OmicronZoneExternalFloatingIps {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.iter().cmp(other.0.iter())
    }
}

fn check_external_ip_ids<'a>(
    ids: impl Iterator<Item = &'a Uuid>,
) -> Result<(), ZoneExternalAddrsError> {
    let mut seen_ids = HashSet::new();
    for id in ids {
        if !seen_ids.insert(id) {
            return Err(ZoneExternalAddrsError::DuplicateIds { id: *id });
        }
    }
    Ok(())
}

impl OmicronZoneExternalFloatingIps {
    /// Construct from a set of external IPs, validating the count and that all
    /// IPs and IDs are unique.
    pub fn new(
        ips: IdOrdMap<OmicronZoneExternalFloatingIp>,
    ) -> Result<Self, ZoneExternalAddrsError> {
        check_external_ip_count(ips.len())?;
        check_external_ip_ids(ips.iter().map(|ip| ip.id.as_untyped_uuid()))?;
        Ok(Self(ips))
    }

    /// Construct from a single external IP.
    pub fn from_single(ip: OmicronZoneExternalFloatingIp) -> Self {
        Self(IdOrdMap::from_iter_unique([ip]).unwrap())
    }

    /// Iterate over the external IPs.
    pub fn iter(&self) -> impl Iterator<Item = &OmicronZoneExternalFloatingIp> {
        self.0.iter()
    }
}

impl From<OmicronZoneExternalFloatingIps> for NexusExternalIps {
    fn from(value: OmicronZoneExternalFloatingIps) -> Self {
        // Failing should be impossible. Both types share invariants:
        //
        // - There's at least one IP
        // - There are no more than `MAX_ZONE_EXTERNAL_IPS` IPs
        // - All the IP addresses are unique.
        //
        // The only difference is between this type and `NexusExternalIps` is
        // that this one carries the UUId for each IP address as well.
        NexusExternalIps::new(value.0.into_iter().map(|ip| ip.ip).collect())
            .expect("both types enforce the same invariants")
    }
}

impl TryFrom<IdOrdMap<OmicronZoneExternalFloatingIp>>
    for OmicronZoneExternalFloatingIps
{
    type Error = ZoneExternalAddrsError;

    fn try_from(
        value: IdOrdMap<OmicronZoneExternalFloatingIp>,
    ) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<OmicronZoneExternalFloatingIps>
    for IdOrdMap<OmicronZoneExternalFloatingIp>
{
    fn from(ips: OmicronZoneExternalFloatingIps) -> Self {
        ips.0
    }
}

/// A set of `OmicronZoneExternalFloatingAddrs`s allocated to a single zone.
///
/// The set of IPs is always non-empty, and there are no duplicate IP addresses.
/// Also, the size is bounded above by `MAX_ZONE_EXTERNAL_IPS`.
///
/// NOTE: This is the reconfigurator analog of the inventory `ExternalDnsAddrs`
/// type.
#[derive(
    Debug, Clone, Eq, PartialEq, JsonSchema, Serialize, Deserialize, Diffable,
)]
#[daft(leaf)]
#[serde(
    try_from = "IdOrdMap<OmicronZoneExternalFloatingAddr>",
    into = "IdOrdMap<OmicronZoneExternalFloatingAddr>"
)]
pub struct OmicronZoneExternalFloatingAddrs(
    #[schemars(length(
        min = 1,
        max = "sled_agent_types::inventory::MAX_ZONE_EXTERNAL_IPS"
    ))]
    IdOrdMap<OmicronZoneExternalFloatingAddr>,
);

impl std::cmp::PartialOrd for OmicronZoneExternalFloatingAddrs {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for OmicronZoneExternalFloatingAddrs {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.iter().cmp(other.0.iter())
    }
}

impl OmicronZoneExternalFloatingAddrs {
    /// Construct from a set of external addresses, validating the count and
    /// that all IPs and IDs are unique.
    pub fn new(
        addrs: IdOrdMap<OmicronZoneExternalFloatingAddr>,
    ) -> Result<Self, ZoneExternalAddrsError> {
        check_external_ip_count(addrs.len())?;
        check_external_ip_ids(
            addrs.iter().map(|addr| addr.id.as_untyped_uuid()),
        )?;
        Ok(Self(addrs))
    }

    /// Construct from a single external address.
    pub fn from_single(addr: OmicronZoneExternalFloatingAddr) -> Self {
        Self(IdOrdMap::from_iter_unique([addr]).unwrap())
    }

    /// Iterate over the external addresses.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = &OmicronZoneExternalFloatingAddr> {
        self.0.iter()
    }
}

impl From<OmicronZoneExternalFloatingAddrs> for ExternalDnsAddrs {
    fn from(value: OmicronZoneExternalFloatingAddrs) -> Self {
        // Failing should be impossible. Both types share invariants:
        //
        // - There's at least one IP
        // - There are no more than `MAX_ZONE_EXTERNAL_IPS` IPs
        // - All the IP addresses are unique, ignoring the port numbers.
        //
        // The only difference between this type and `ExternalDnsAddrs` is that
        // this one carries the UUID for each IP address as well.
        ExternalDnsAddrs::new(
            value.0.into_iter().map(|addr| addr.addr).collect(),
        )
        .expect("both types enforce the same invariants")
    }
}

impl TryFrom<IdOrdMap<OmicronZoneExternalFloatingAddr>>
    for OmicronZoneExternalFloatingAddrs
{
    type Error = ZoneExternalAddrsError;

    fn try_from(
        value: IdOrdMap<OmicronZoneExternalFloatingAddr>,
    ) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<OmicronZoneExternalFloatingAddrs>
    for IdOrdMap<OmicronZoneExternalFloatingAddr>
{
    fn from(addrs: OmicronZoneExternalFloatingAddrs) -> Self {
        addrs.0
    }
}

/// SNAT configuration for a boundary NTP zone in a blueprint.
///
/// Boundary NTP reaches upstream servers via source NAT and needs a source
/// address per IP version it wants to reach them on: at most one per family,
/// and at least one overall. This is the blueprint-layer analog of the
/// sled-agent wire type `ZoneSnatConfig`, but each entry additionally carries
/// its allocated `ExternalIpUuid`.
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    JsonSchema,
    Serialize,
    Deserialize,
    Diffable,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OmicronZoneExternalSnat {
    Ipv4Only(OmicronZoneExternalSnatIpv4),
    Ipv6Only(OmicronZoneExternalSnatIpv6),
    DualStack {
        ipv4: OmicronZoneExternalSnatIpv4,
        ipv6: OmicronZoneExternalSnatIpv6,
    },
}

impl OmicronZoneExternalSnat {
    /// Construct from a single SNAT IP, inferring the family from its address.
    pub fn from_single(snat: OmicronZoneExternalSnatIp) -> Self {
        match snat.snat_cfg.ip {
            IpAddr::V4(_) => {
                let snat_cfg = snat
                    .snat_cfg
                    .try_as_ipv4()
                    .expect("just matched an IPv4 address");
                OmicronZoneExternalSnat::Ipv4Only(OmicronZoneExternalSnatIpv4 {
                    id: snat.id,
                    snat_cfg,
                })
            }
            IpAddr::V6(_) => {
                let snat_cfg = snat
                    .snat_cfg
                    .try_as_ipv6()
                    .expect("just matched an IPv6 address");
                OmicronZoneExternalSnat::Ipv6Only(OmicronZoneExternalSnatIpv6 {
                    id: snat.id,
                    snat_cfg,
                })
            }
        }
    }

    /// Build from a set of SNAT IPs: at most one per IP family, and at least
    /// one overall.
    pub fn from_ips(
        ips: impl IntoIterator<Item = OmicronZoneExternalSnatIp>,
    ) -> Result<Self, ZoneExternalSnatError> {
        let mut v4: Option<OmicronZoneExternalSnatIpv4> = None;
        let mut v6: Option<OmicronZoneExternalSnatIpv6> = None;
        for ip in ips {
            match ip.snat_cfg.ip {
                IpAddr::V4(_) => {
                    let snat_cfg = ip
                        .snat_cfg
                        .try_as_ipv4()
                        .expect("just matched an IPv4 address");
                    let entry =
                        OmicronZoneExternalSnatIpv4 { id: ip.id, snat_cfg };
                    if v4.replace(entry).is_some() {
                        return Err(ZoneExternalSnatError::DuplicateIpv4);
                    }
                }
                IpAddr::V6(_) => {
                    let snat_cfg = ip
                        .snat_cfg
                        .try_as_ipv6()
                        .expect("just matched an IPv6 address");
                    let entry =
                        OmicronZoneExternalSnatIpv6 { id: ip.id, snat_cfg };
                    if v6.replace(entry).is_some() {
                        return Err(ZoneExternalSnatError::DuplicateIpv6);
                    }
                }
            }
        }
        match (v4, v6) {
            (Some(ipv4), None) => Ok(OmicronZoneExternalSnat::Ipv4Only(ipv4)),
            (None, Some(ipv6)) => Ok(OmicronZoneExternalSnat::Ipv6Only(ipv6)),
            (Some(ipv4), Some(ipv6)) => {
                Ok(OmicronZoneExternalSnat::DualStack { ipv4, ipv6 })
            }
            (None, None) => Err(ZoneExternalSnatError::Empty),
        }
    }

    /// Iterate over the SNAT IPs (one per family), widened to the
    /// family-agnostic [`OmicronZoneExternalSnatIp`].
    pub fn iter(&self) -> impl Iterator<Item = OmicronZoneExternalSnatIp> {
        let (first, second) = match *self {
            OmicronZoneExternalSnat::Ipv4Only(v4) => (v4.into(), None),
            OmicronZoneExternalSnat::Ipv6Only(v6) => (v6.into(), None),
            OmicronZoneExternalSnat::DualStack { ipv4, ipv6 } => {
                (ipv4.into(), Some(ipv6.into()))
            }
        };
        std::iter::once(first).chain(second)
    }
}

impl From<OmicronZoneExternalSnat> for ZoneSnatConfig {
    /// Convert to the sled-agent wire [`ZoneSnatConfig`], dropping the
    /// allocation IDs (which sled-agent does not need).
    fn from(snat: OmicronZoneExternalSnat) -> Self {
        match snat {
            OmicronZoneExternalSnat::Ipv4Only(v4) => {
                ZoneSnatConfig::Ipv4Only(v4.snat_cfg)
            }
            OmicronZoneExternalSnat::Ipv6Only(v6) => {
                ZoneSnatConfig::Ipv6Only(v6.snat_cfg)
            }
            OmicronZoneExternalSnat::DualStack { ipv4, ipv6 } => {
                ZoneSnatConfig::DualStack {
                    ipv4: ipv4.snat_cfg,
                    ipv6: ipv6.snat_cfg,
                }
            }
        }
    }
}

/// Errors building an [`OmicronZoneExternalSnat`] from a set of SNAT IPs.
#[derive(Clone, Copy, Debug, Error)]
pub enum ZoneExternalSnatError {
    #[error("must provide at least one SNAT address")]
    Empty,
    #[error("multiple IPv4 SNAT addresses provided")]
    DuplicateIpv4,
    #[error("multiple IPv6 SNAT addresses provided")]
    DuplicateIpv6,
}

/// The private IP address(es) of an Omicron zone's network interface.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum OmicronZoneNicIp {
    /// The interface has only an IPv4 address.
    Ipv4Only(Ipv4Addr),
    /// The interface has only an IPv6 address.
    Ipv6Only(Ipv6Addr),
    /// The interface is dual-stack.
    DualStack { v4: Ipv4Addr, v6: Ipv6Addr },
}

impl OmicronZoneNicIp {
    /// Return the IPv4 address, if this configuration has one.
    pub fn ipv4(&self) -> Option<Ipv4Addr> {
        match self {
            OmicronZoneNicIp::Ipv4Only(v4)
            | OmicronZoneNicIp::DualStack { v4, .. } => Some(*v4),
            OmicronZoneNicIp::Ipv6Only(_) => None,
        }
    }

    /// Return the IPv6 address, if this configuration has one.
    pub fn ipv6(&self) -> Option<Ipv6Addr> {
        match self {
            OmicronZoneNicIp::Ipv6Only(v6)
            | OmicronZoneNicIp::DualStack { v6, .. } => Some(*v6),
            OmicronZoneNicIp::Ipv4Only(_) => None,
        }
    }

    /// Return true if this is a dual-stack configuration.
    pub fn is_dual_stack(&self) -> bool {
        matches!(self, OmicronZoneNicIp::DualStack { .. })
    }

    /// Iterate over all addresses: one for a single-stack NIC, two for a
    /// dual-stack NIC (IPv4 first).
    pub fn addrs(&self) -> impl Iterator<Item = IpAddr> {
        self.ipv4()
            .map(IpAddr::V4)
            .into_iter()
            .chain(self.ipv6().map(IpAddr::V6))
    }
}

impl From<&PrivateIpConfig> for OmicronZoneNicIp {
    fn from(cfg: &PrivateIpConfig) -> Self {
        match cfg {
            PrivateIpConfig::V4(v4) => OmicronZoneNicIp::Ipv4Only(*v4.ip()),
            PrivateIpConfig::V6(v6) => OmicronZoneNicIp::Ipv6Only(*v6.ip()),
            PrivateIpConfig::DualStack { v4, v6 } => {
                OmicronZoneNicIp::DualStack { v4: *v4.ip(), v6: *v6.ip() }
            }
        }
    }
}

impl std::fmt::Display for OmicronZoneNicIp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OmicronZoneNicIp::Ipv4Only(v4) => write!(f, "{v4}"),
            OmicronZoneNicIp::Ipv6Only(v6) => write!(f, "{v6}"),
            OmicronZoneNicIp::DualStack { v4, v6 } => {
                write!(f, "{v4} | {v6}")
            }
        }
    }
}

/// Network interface allocated to an Omicron-managed zone.
///
/// This is a slimmer `nexus_db_model::ServiceNetworkInterface` that only stores
/// the fields necessary for blueprint planning.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct OmicronZoneNic {
    pub id: VnicUuid,
    pub mac: MacAddr,
    pub ip: OmicronZoneNicIp,
    pub slot: u8,
    pub primary: bool,
}

/// A pair of an Omicron zone ID and an external IP.
///
/// Part of [`OmicronZoneNetworkResources`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct OmicronZoneExternalIpEntry {
    pub zone_id: OmicronZoneUuid,
    pub ip: OmicronZoneExternalIp,
}

/// Specification for the tri-map of Omicron zone external IPs.
impl TriHashItem for OmicronZoneExternalIpEntry {
    type K1<'a> = OmicronZoneUuid;
    type K2<'a> = ExternalIpUuid;

    // Note: cannot use IpAddr here, because SNAT IPs can overlap as long as
    // their port blocks are disjoint.
    type K3<'a> = OmicronZoneExternalIpKey;

    fn key1(&self) -> Self::K1<'_> {
        self.zone_id
    }

    fn key2(&self) -> Self::K2<'_> {
        self.ip.id()
    }

    fn key3(&self) -> Self::K3<'_> {
        self.ip.ip_key()
    }

    tri_upcast!();
}

/// A pair of an Omicron zone ID and a network interface.
///
/// Part of [`OmicronZoneNetworkResources`].
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize,
)]
pub struct OmicronZoneNicEntry {
    pub zone_id: OmicronZoneUuid,
    pub nic: OmicronZoneNic,
}

impl TriHashItem for OmicronZoneNicEntry {
    type K1<'a> = OmicronZoneUuid;
    type K2<'a> = VnicUuid;
    type K3<'a> = MacAddr;

    fn key1(&self) -> Self::K1<'_> {
        self.zone_id
    }

    fn key2(&self) -> Self::K2<'_> {
        self.nic.id
    }

    fn key3(&self) -> Self::K3<'_> {
        self.nic.mac
    }

    tri_upcast!();
}

#[derive(Debug, Error)]
pub enum AddNetworkResourceError {
    #[error(
        "associating Omicron zone {zone_id} with {ip:?} failed due to duplicates"
    )]
    DuplicateOmicronZoneExternalIp {
        zone_id: OmicronZoneUuid,
        ip: OmicronZoneExternalIp,
        #[source]
        err: anyhow::Error,
    },
    #[error(
        "associating Omicron zone {zone_id} with {nic:?} failed due to duplicates"
    )]
    DuplicateOmicronZoneNic {
        zone_id: OmicronZoneUuid,
        nic: OmicronZoneNic,
        #[source]
        err: anyhow::Error,
    },
    #[error(
        "the Omicron zone {zone_id} with NIC {nic_id} has no private IP \
        address for the external IP {external_ip}"
    )]
    NoPrivateIpForExternalIp {
        zone_id: OmicronZoneUuid,
        nic_id: VnicUuid,
        external_ip: IpAddr,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_common::api::internal::shared::PrivateIpv4Config;
    use omicron_common::api::internal::shared::PrivateIpv6Config;
    use proptest::prelude::*;
    use sled_agent_types::inventory::MAX_ZONE_EXTERNAL_IPS;

    fn v4_config() -> PrivateIpv4Config {
        PrivateIpv4Config::new(
            "172.30.2.5".parse().unwrap(),
            "172.30.2.0/24".parse().unwrap(),
        )
        .unwrap()
    }

    fn v6_config() -> PrivateIpv6Config {
        PrivateIpv6Config::new(
            "fd00:1122:3344:100::5".parse().unwrap(),
            "fd00:1122:3344:100::/64".parse().unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn from_private_ip_config_and_accessors() {
        let v4: Ipv4Addr = "172.30.2.5".parse().unwrap();
        let v6: Ipv6Addr = "fd00:1122:3344:100::5".parse().unwrap();

        let v4_only = OmicronZoneNicIp::from(&PrivateIpConfig::V4(v4_config()));
        assert_eq!(v4_only, OmicronZoneNicIp::Ipv4Only(v4));
        assert_eq!(v4_only.ipv4(), Some(v4));
        assert_eq!(v4_only.ipv6(), None);
        assert!(!v4_only.is_dual_stack());
        assert_eq!(v4_only.addrs().collect::<Vec<_>>(), vec![IpAddr::V4(v4)]);

        let v6_only = OmicronZoneNicIp::from(&PrivateIpConfig::V6(v6_config()));
        assert_eq!(v6_only, OmicronZoneNicIp::Ipv6Only(v6));
        assert_eq!(v6_only.ipv4(), None);
        assert_eq!(v6_only.ipv6(), Some(v6));
        assert!(!v6_only.is_dual_stack());
        assert_eq!(v6_only.addrs().collect::<Vec<_>>(), vec![IpAddr::V6(v6)]);

        let dual = OmicronZoneNicIp::from(&PrivateIpConfig::DualStack {
            v4: v4_config(),
            v6: v6_config(),
        });
        assert_eq!(dual, OmicronZoneNicIp::DualStack { v4, v6 });
        assert_eq!(dual.ipv4(), Some(v4));
        assert_eq!(dual.ipv6(), Some(v6));
        assert!(dual.is_dual_stack());
        // blippy relies on `addrs()` yielding both addresses (IPv4 first) so it
        // can check each against the known OPTE subnets.
        assert_eq!(
            dual.addrs().collect::<Vec<_>>(),
            vec![IpAddr::V4(v4), IpAddr::V6(v6)],
        );
    }

    #[test]
    fn display() {
        let v4: Ipv4Addr = "172.30.2.5".parse().unwrap();
        let v6: Ipv6Addr = "fd00:1122:3344:100::5".parse().unwrap();
        assert_eq!(OmicronZoneNicIp::Ipv4Only(v4).to_string(), "172.30.2.5");
        assert_eq!(
            OmicronZoneNicIp::Ipv6Only(v6).to_string(),
            "fd00:1122:3344:100::5",
        );
        assert_eq!(
            OmicronZoneNicIp::DualStack { v4, v6 }.to_string(),
            "172.30.2.5 | fd00:1122:3344:100::5",
        );
    }

    fn zone_nic(ip: OmicronZoneNicIp) -> OmicronZoneNic {
        OmicronZoneNic {
            id: VnicUuid::new_v4(),
            mac: "a8:40:25:ff:00:01".parse().unwrap(),
            ip,
            slot: 0,
            primary: true,
        }
    }

    fn floating_external_ip(ip: IpAddr) -> OmicronZoneExternalIp {
        OmicronZoneExternalIp::Floating(OmicronZoneExternalFloatingIp {
            id: ExternalIpUuid::new_v4(),
            ip,
        })
    }

    fn v4_only_nic_ip() -> OmicronZoneNicIp {
        OmicronZoneNicIp::Ipv4Only("172.30.2.5".parse().unwrap())
    }

    fn v6_only_nic_ip() -> OmicronZoneNicIp {
        OmicronZoneNicIp::Ipv6Only("fd00:1122:3344:100::5".parse().unwrap())
    }

    fn dual_stack_nic_ip() -> OmicronZoneNicIp {
        OmicronZoneNicIp::DualStack {
            v4: "172.30.2.5".parse().unwrap(),
            v6: "fd00:1122:3344:100::5".parse().unwrap(),
        }
    }

    fn v4_external_ip() -> OmicronZoneExternalIp {
        floating_external_ip("192.0.2.1".parse().unwrap())
    }

    fn v6_external_ip() -> OmicronZoneExternalIp {
        floating_external_ip("2001:db8::1".parse().unwrap())
    }

    #[track_caller]
    fn assert_consistency_result(
        result: Result<(), AddNetworkResourceError>,
        expect_ok: bool,
        direction: &str,
    ) {
        match (expect_ok, result) {
            (true, Ok(())) => {}
            (
                false,
                Err(AddNetworkResourceError::NoPrivateIpForExternalIp {
                    ..
                }),
            ) => {}
            (true, Err(err)) => {
                panic!("expected success ({direction}), got error: {err}")
            }
            (false, Ok(())) => {
                panic!("expected failure ({direction}), but it succeeded")
            }
            (false, Err(err)) => panic!(
                "expected NoPrivateIpForExternalIp ({direction}), got: {err}"
            ),
        }
    }

    // Associate `external` and a NIC carrying `nic_ip` on the same zone and
    // assert the outcome is `expect_ok`, no matter which one we add first.
    #[track_caller]
    fn assert_external_ip_nic_consistency(
        external: OmicronZoneExternalIp,
        nic_ip: OmicronZoneNicIp,
        expect_ok: bool,
    ) {
        // External IP first, then NIC.
        {
            let mut resources = OmicronZoneNetworkResources::new();
            let zone_id = OmicronZoneUuid::new_v4();
            resources
                .add_external_ip(zone_id, external)
                .expect("adding the external IP first always succeeds");
            assert_consistency_result(
                resources.add_nic(zone_id, zone_nic(nic_ip)),
                expect_ok,
                "add NIC after external IP",
            );
        }
        // NIC first, then external IP.
        {
            let mut resources = OmicronZoneNetworkResources::new();
            let zone_id = OmicronZoneUuid::new_v4();
            resources
                .add_nic(zone_id, zone_nic(nic_ip))
                .expect("adding the NIC first always succeeds");
            assert_consistency_result(
                resources.add_external_ip(zone_id, external),
                expect_ok,
                "add external IP after NIC",
            );
        }
    }

    #[test]
    fn external_ip_consistent_with_nic_passes() {
        assert_external_ip_nic_consistency(
            v4_external_ip(),
            v4_only_nic_ip(),
            true,
        );
        assert_external_ip_nic_consistency(
            v4_external_ip(),
            dual_stack_nic_ip(),
            true,
        );
        assert_external_ip_nic_consistency(
            v6_external_ip(),
            v6_only_nic_ip(),
            true,
        );
        assert_external_ip_nic_consistency(
            v6_external_ip(),
            dual_stack_nic_ip(),
            true,
        );
    }

    #[test]
    fn external_ip_without_matching_nic_family_fails() {
        assert_external_ip_nic_consistency(
            v4_external_ip(),
            v6_only_nic_ip(),
            false,
        );
        assert_external_ip_nic_consistency(
            v6_external_ip(),
            v4_only_nic_ip(),
            false,
        );
    }

    #[test]
    fn adding_a_resource_without_the_other_passes() {
        // An external IP of either family is fine when the zone has no NIC yet.
        for external in [v4_external_ip(), v6_external_ip()] {
            let mut resources = OmicronZoneNetworkResources::new();
            let zone_id = OmicronZoneUuid::new_v4();
            resources
                .add_external_ip(zone_id, external)
                .expect("external IP with no NIC should be accepted");
        }
        // A NIC of either family, or dual-stack, is fine when the zone has no
        // external IP yet.
        for nic_ip in [v4_only_nic_ip(), v6_only_nic_ip(), dual_stack_nic_ip()]
        {
            let mut resources = OmicronZoneNetworkResources::new();
            let zone_id = OmicronZoneUuid::new_v4();
            resources
                .add_nic(zone_id, zone_nic(nic_ip))
                .expect("NIC with no external IP should be accepted");
        }
    }

    #[test]
    fn omicron_zone_external_floating_ips_reject_duplicate_ip_on_deserialize() {
        let json = r#"[
            {"id":"bf8c8086-cb70-4b33-82a1-ce749fcdd8de","ip":"192.0.2.1"},
            {"id":"d0c6f5fc-7414-46d7-8992-f553d3fc303f","ip":"192.0.2.1"}
        ]"#;
        let result: Result<OmicronZoneExternalFloatingIps, _> =
            serde_json::from_str(json);
        assert!(
            result.is_err(),
            "a duplicate IP should fail to deserialize, got {result:?}",
        );
    }

    #[test]
    fn omicron_zone_external_floating_ips_reject_bad_count() {
        let empty =
            OmicronZoneExternalFloatingIps::new(IdOrdMap::new()).unwrap_err();
        assert!(
            matches!(empty, ZoneExternalAddrsError::Empty),
            "got {empty:?}",
        );

        let too_many =
            IdOrdMap::from_iter_unique((0..=MAX_ZONE_EXTERNAL_IPS).map(|i| {
                OmicronZoneExternalFloatingIp {
                    id: ExternalIpUuid::new_v4(),
                    ip: IpAddr::V4(Ipv4Addr::new(192, 0, 2, i as u8)),
                }
            }))
            .expect("distinct IPs build a valid map");
        let err = OmicronZoneExternalFloatingIps::new(too_many).unwrap_err();
        assert!(
            matches!(err, ZoneExternalAddrsError::TooMany { .. }),
            "got {err:?}",
        );
    }

    // Get an IP address from the pool, my mapping the index to an IP. We have
    // both IPv4 and IPv6.
    fn pool_ip(index: usize) -> IpAddr {
        if index.is_multiple_of(2) {
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, index as u8))
        } else {
            IpAddr::V6(Ipv6Addr::new(
                0x2001,
                0xdb8,
                0,
                0,
                0,
                0,
                0,
                index as u16,
            ))
        }
    }

    fn arbitrary_floating_ips()
    -> impl Strategy<Value = Vec<OmicronZoneExternalFloatingIp>> {
        let element = (any::<ExternalIpUuid>(), 0..MAX_ZONE_EXTERNAL_IPS)
            .prop_map(|(id, index)| OmicronZoneExternalFloatingIp {
                id,
                ip: pool_ip(index),
            });
        proptest::collection::vec(element, 0..MAX_ZONE_EXTERNAL_IPS)
    }

    fn arbitrary_floating_addrs()
    -> impl Strategy<Value = Vec<OmicronZoneExternalFloatingAddr>> {
        // Ports are irrelevant to the constructors, so just draw randomly.
        let element =
            (any::<ExternalIpUuid>(), 0..MAX_ZONE_EXTERNAL_IPS, any::<u16>())
                .prop_map(|(id, index, port)| {
                    OmicronZoneExternalFloatingAddr {
                        id,
                        addr: SocketAddr::new(pool_ip(index), port),
                    }
                });
        proptest::collection::vec(element, 0..MAX_ZONE_EXTERNAL_IPS)
    }

    proptest! {
        /// We should always be able to convert the blueprint
        /// `OmicronZoneExternalFloatingIps` into the inventory type
        /// `NexusExternalIps`. That's technically a panicking conversion, and
        /// there's some cross-crate coupling here, so use a proptest to make
        /// sure it fails loudly if anything changes.
        #[test]
        fn floating_ips_always_convert_to_inventory(
            ips in arbitrary_floating_ips(),
        ) {
            let map = IdOrdMap::from_iter(ips);
            let blueprint = OmicronZoneExternalFloatingIps::new(map.clone());
            let inventory =
                NexusExternalIps::new(map.iter().map(|ip| ip.ip).collect());

            prop_assert_eq!(blueprint.is_ok(), inventory.is_ok());

            if let (Ok(blueprint), Ok(inventory)) = (blueprint, inventory) {
                prop_assert_eq!(inventory, blueprint.into());
            }
        }

        /// The same test for `OmicronZoneExternalFloatingAddrs` and the
        /// inventory `ExternalDnsAddrs`.
        #[test]
        fn floating_addrs_always_convert_to_inventory(
            addrs in arbitrary_floating_addrs(),
        ) {
            let map = IdOrdMap::from_iter(addrs);
            let blueprint =
                OmicronZoneExternalFloatingAddrs::new(map.clone());
            let inventory = ExternalDnsAddrs::new(
                map.iter().map(|addr| addr.addr).collect(),
            );

            prop_assert_eq!(blueprint.is_ok(), inventory.is_ok());

            if let (Ok(blueprint), Ok(inventory)) = (blueprint, inventory) {
                prop_assert_eq!(inventory, blueprint.into());
            }
        }
    }
}
