// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::anyhow;
use daft::Diffable;
use iddqd::TriHashItem;
use iddqd::TriHashMap;
use iddqd::tri_upcast;
use omicron_common::api::external::IpVersion;
use omicron_common::api::external::MacAddr;
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_uuid_kinds::ExternalIpUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::VnicUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::inventory::SourceNatConfigGeneric;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
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
    pub snat_cfg: SourceNatConfigGeneric,
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
}
