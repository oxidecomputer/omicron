// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::anyhow;
use daft::Diffable;
use iddqd::TriHashItem;
use iddqd::TriHashMap;
use iddqd::tri_upcast;
use omicron_common::api::external::MacAddr;
use omicron_common::api::internal::shared::SourceNatConfig;
use omicron_uuid_kinds::ExternalIpUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::VnicUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;
use std::net::SocketAddr;
use thiserror::Error;

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

    pub fn add_external_ip(
        &mut self,
        zone_id: OmicronZoneUuid,
        ip: OmicronZoneExternalIp,
    ) -> Result<(), AddNetworkResourceError> {
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
    Debug,
    Clone,
    Copy,
    Hash,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
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
}

/// An IP-based key suitable for uniquely identifying an
/// [`OmicronZoneExternalIp`].
///
/// We can't use the IP itself to uniquely identify an external IP because SNAT
/// IPs can have overlapping addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OmicronZoneExternalIpKey {
    Floating(IpAddr),
    Snat(SourceNatConfig),
}

/// Floating external IP allocated to an Omicron-managed zone.
///
/// This is a slimmer `nexus_db_model::ExternalIp` that only stores the fields
/// necessary for blueprint planning, and requires that the zone have a single
/// IP.
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
pub struct OmicronZoneExternalFloatingIp {
    pub id: ExternalIpUuid,
    pub ip: IpAddr,
}

/// Floating external address with port allocated to an Omicron-managed zone.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    JsonSchema,
    Serialize,
    Deserialize,
    Diffable,
)]
pub struct OmicronZoneExternalFloatingAddr {
    pub id: ExternalIpUuid,
    pub addr: SocketAddr,
}

impl OmicronZoneExternalFloatingAddr {
    pub fn into_ip(self) -> OmicronZoneExternalFloatingIp {
        OmicronZoneExternalFloatingIp { id: self.id, ip: self.addr.ip() }
    }
}

/// SNAT (outbound) external IP allocated to an Omicron-managed zone.
///
/// This is a slimmer `nexus_db_model::ExternalIp` that only stores the fields
/// necessary for blueprint planning, and requires that the zone have a single
/// IP.
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
    pub snat_cfg: SourceNatConfig,
}

/// Network interface allocated to an Omicron-managed zone.
///
/// This is a slimmer `nexus_db_model::ServiceNetworkInterface` that only stores
/// the fields necessary for blueprint planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmicronZoneNic {
    pub id: VnicUuid,
    pub mac: MacAddr,
    pub ip: IpAddr,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
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
}
