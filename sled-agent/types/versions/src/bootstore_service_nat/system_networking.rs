// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for system-level networking.
//!
//! Changes in this version:
//!
//! * New types added:
//!   * [`ServiceZoneNatKind`], a description of the zone-type-specific details
//!     for service zone NAT entries
//!   * [`ServiceZoneNatEntry`], holding all the information needed to set up
//!     the NAT entry for a service zone
//!   * [`ServiceZoneNatEntries`], a map of all the zone NAT entries for all
//!     services on the system. This is a newtype wrapper around an `IdOrdMap`
//!     that enforces some constraints that should always be true for real
//!     systems.
//! * Old types changed and renamed:
//!   * [`SystemNetworkingConfig`] replaces
//!     [`crate::v30::early_networking::EarlyNetworkConfigBody`]. It adds the
//!     new field [`SystemNetworkingConfig::service_zone_nat_entries`].
//! * Types changed:
//!   * [`WriteNetworkConfigRequest::body`] is now a [`SystemNetworkingConfig`]

use crate::v1::inventory::ZoneKind;
use crate::v11::inventory::SourceNatConfigGeneric;
use crate::v30;
use crate::v30::early_networking::RackNetworkConfig;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::Vni;
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map::Entry;
use std::net::IpAddr;
use std::net::Ipv6Addr;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub enum ServiceZoneNatKind {
    BoundaryNtp { snat_cfg: SourceNatConfigGeneric },
    ExternalDns { external_ip: IpAddr },
    Nexus { external_ip: IpAddr },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub struct ServiceZoneNatEntry {
    pub zone_id: OmicronZoneUuid,
    pub sled_underlay_ip: Ipv6Addr,
    pub nic_mac: MacAddr,
    pub vni: Vni,
    pub kind: ServiceZoneNatKind,
}

impl IdOrdItem for ServiceZoneNatEntry {
    type Key<'a> = OmicronZoneUuid;

    fn key(&self) -> Self::Key<'_> {
        self.zone_id
    }

    iddqd::id_upcast!();
}

/// Description of all NAT entries for control plane services.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(
    try_from = "IdOrdMap<ServiceZoneNatEntry>",
    into = "IdOrdMap<ServiceZoneNatEntry>"
)]
pub struct ServiceZoneNatEntries(pub(crate) IdOrdMap<ServiceZoneNatEntry>);

#[derive(Debug, thiserror::Error)]
pub enum ServiceZoneNatEntriesError {
    #[error(
        "ServiceZoneNatEntries requires at least one entry for \
         zone types {missing_zone_types:?}"
    )]
    MissingZoneTypes { missing_zone_types: BTreeSet<ZoneKind> },

    #[error(
        "ServiceZoneNatEntries requires non-overlapping entries: \
         zone {zone_1_id} IP {ip} ports {zone_1_lo}-{zone_1_hi} \
         conflicts with \
         zone {zone_2_id} IP {ip} ports {zone_2_lo}-{zone_2_hi}"
    )]
    OverlappingNatEntry {
        ip: IpAddr,
        zone_1_id: OmicronZoneUuid,
        zone_1_lo: u16,
        zone_1_hi: u16,
        zone_2_id: OmicronZoneUuid,
        zone_2_lo: u16,
        zone_2_hi: u16,
    },
}

// These conversions are here instead of `crate::impls` because they're part of
// our `Serialize` / `Deserialize` implementations.
//
// This `TryFrom` impl enforces requirements we expect of typical
// `ServiceZoneNatEntries` on real systems:
//
// 1. At least one entry for each zone type that has external connectivity
//    (boundary NTP, Nexus, external DNS)
// 2. No overlapping NAT entries
//
// Both of these are enforced on construction to ensure we don't accidentally
// persist data to the bootstore that would cause the rack to become unreachable
// or fail to cold boot (e.g., NAT entries with no boundary NTP entry would
// fail to time sync if we cold booted; NAT entries with no Nexus entry would
// remove all access to the external API). Overlapping NAT entries are
// nonsensical - we can't map the same IP+port range to two different zones.
//
// It's possible in the future we might want to relax this; e.g., if RSS wanted
// to create a `ServiceZoneNatEntries` describing an "early RSS" step that only
// had boundary NTP zones without Nexus or external DNS. But for now, we always
// expect both of the above conditions to remain true, so we enforce them as a
// part of this newtype.
impl TryFrom<IdOrdMap<ServiceZoneNatEntry>> for ServiceZoneNatEntries {
    type Error = ServiceZoneNatEntriesError;

    fn try_from(
        entries: IdOrdMap<ServiceZoneNatEntry>,
    ) -> Result<Self, Self::Error> {
        let mut have_boundary_ntp = false;
        let mut have_external_dns = false;
        let mut have_nexus = false;

        let mut entries_by_ip: BTreeMap<
            IpAddr,
            Vec<(u16, u16, OmicronZoneUuid)>,
        > = BTreeMap::new();

        for entry in &entries {
            match &entry.kind {
                ServiceZoneNatKind::BoundaryNtp { .. } => {
                    have_boundary_ntp = true;
                }
                ServiceZoneNatKind::ExternalDns { .. } => {
                    have_external_dns = true;
                }
                ServiceZoneNatKind::Nexus { .. } => {
                    have_nexus = true;
                }
            }

            let ip = entry.kind.external_ip();
            let (port_lo, port_hi) = entry.kind.nat_port_range();
            match entries_by_ip.entry(ip) {
                Entry::Vacant(vacant) => {
                    vacant.insert(vec![(port_lo, port_hi, entry.zone_id)]);
                }
                Entry::Occupied(mut occupied) => {
                    for (prev_lo, prev_hi, prev_id) in occupied.get() {
                        // We already have a NAT entry for this same external
                        // IP; that's only okay if the port ranges don't
                        // overlap.
                        if *prev_hi >= port_lo && *prev_lo <= port_hi {
                            return Err(ServiceZoneNatEntriesError::OverlappingNatEntry {
                                ip,
                                zone_1_id: *prev_id,
                                zone_1_lo: *prev_lo,
                                zone_1_hi: *prev_hi,
                                zone_2_id: entry.zone_id,
                                zone_2_lo: port_lo,
                                zone_2_hi: port_hi,
                            });
                        }
                    }
                    occupied.get_mut().push((port_lo, port_hi, entry.zone_id));
                }
            }
        }

        let mut missing_zone_types = BTreeSet::new();
        if !have_boundary_ntp {
            missing_zone_types.insert(ZoneKind::BoundaryNtp);
        }
        if !have_external_dns {
            missing_zone_types.insert(ZoneKind::ExternalDns);
        }
        if !have_nexus {
            missing_zone_types.insert(ZoneKind::Nexus);
        }

        if missing_zone_types.is_empty() {
            Ok(Self(entries))
        } else {
            Err(ServiceZoneNatEntriesError::MissingZoneTypes {
                missing_zone_types,
            })
        }
    }
}

impl From<ServiceZoneNatEntries> for IdOrdMap<ServiceZoneNatEntry> {
    fn from(value: ServiceZoneNatEntries) -> Self {
        value.0
    }
}

/// All configuration needed to set up system-level networking.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SystemNetworkingConfig {
    pub rack_network_config: RackNetworkConfig,

    /// Set of all Omicron service zone NAT entries.
    //
    // This field is optional for two reasons:
    //
    // 1. RSS has to initially populate a `SystemNetworkingConfig` with no NAT
    //    information to start all the sled-agents. Once they all start, it
    //    computes a service plan, at which point it can fill this field in.
    // 2. Backwards compatibility: prior versions of this type did not store
    //    this information at all, and we must be able to cleanly handle that at
    //    runtime.
    //
    // In the future, if we can find a way to relax RSS, we can eventually make
    // this field non-optional (once we're confident all deployed systems are
    // past the release we start populating this field).
    pub service_zone_nat_entries: Option<ServiceZoneNatEntries>,
}

impl SystemNetworkingConfig {
    pub const SCHEMA_VERSION: u32 = 5;
}

impl From<SystemNetworkingConfig>
    for v30::early_networking::EarlyNetworkConfigBody
{
    fn from(value: SystemNetworkingConfig) -> Self {
        Self { rack_network_config: value.rack_network_config }
    }
}

impl From<v30::early_networking::EarlyNetworkConfigBody>
    for SystemNetworkingConfig
{
    fn from(value: v30::early_networking::EarlyNetworkConfigBody) -> Self {
        Self {
            rack_network_config: value.rack_network_config,
            service_zone_nat_entries: None,
        }
    }
}

/// Structure for requests from Nexus to sled-agent to write a new
/// [`SystemNetworkingConfig`] into the replicated bootstore.
///
/// [`WriteNetworkConfigRequest`] INTENTIONALLY does not have a `From`
/// implementation from prior API versions. It is critically important that
/// sled-agent not attempt to rewrite old [`SystemNetworkingConfig`] types to
/// the latest version. For more about this, see the comments on the relevant
/// endpoint in `sled-agent-api`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct WriteNetworkConfigRequest {
    pub generation: u64,
    pub body: SystemNetworkingConfig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_strategy::proptest;

    #[proptest]
    fn proptest_service_nat_entries_try_from(
        entries: IdOrdMap<ServiceZoneNatEntry>,
    ) {
        match ServiceZoneNatEntries::try_from(entries.clone()) {
            Ok(converted) => {
                // We kept all the entries.
                assert_eq!(converted.0, entries);

                // We have at least 1 zone of each type.
                assert!(
                    entries
                        .iter()
                        .find(|e| matches!(
                            e.kind,
                            ServiceZoneNatKind::BoundaryNtp { .. }
                        ))
                        .is_some(),
                );
                assert!(
                    entries
                        .iter()
                        .find(|e| matches!(
                            e.kind,
                            ServiceZoneNatKind::ExternalDns { .. }
                        ))
                        .is_some(),
                );
                assert!(
                    entries
                        .iter()
                        .find(|e| matches!(
                            e.kind,
                            ServiceZoneNatKind::Nexus { .. }
                        ))
                        .is_some(),
                );

                // None of the NAT entries overlap.
                let mut nat_full_ip = BTreeSet::new();
                let mut snat_ip: BTreeMap<_, Vec<_>> = BTreeMap::new();

                for e in entries.iter() {
                    match &e.kind {
                        ServiceZoneNatKind::BoundaryNtp { snat_cfg } => {
                            snat_ip
                                .entry(snat_cfg.ip)
                                .or_default()
                                .push(snat_cfg.port_range_raw());
                        }
                        ServiceZoneNatKind::ExternalDns { external_ip }
                        | ServiceZoneNatKind::Nexus { external_ip } => {
                            // Every full IP should be unique.
                            assert!(nat_full_ip.insert(*external_ip));
                        }
                    }
                }

                // There should be no IPs present in both "full IPs" and "SNAT
                // IPs".
                for ip in snat_ip.keys() {
                    assert!(!nat_full_ip.contains(&ip));
                }

                // For any SNAT IP with multiple entries, there should be no
                // overlap in the port ranges.
                for port_ranges in snat_ip.values() {
                    for i in 0..port_ranges.len() {
                        for j in i + 1..port_ranges.len() {
                            let (r1_lo, r1_hi) = port_ranges[i];
                            let (r2_lo, r2_hi) = port_ranges[j];
                            assert!(!(r1_lo <= r2_hi && r2_lo <= r1_hi));
                        }
                    }
                }
            }
            Err(ServiceZoneNatEntriesError::MissingZoneTypes {
                missing_zone_types,
            }) => {
                // This error should always have at least one missing type...
                assert!(!missing_zone_types.is_empty());

                // ...and we should actually be missing any type it includes.
                for t in missing_zone_types {
                    match t {
                        ZoneKind::BoundaryNtp => {
                            assert_eq!(
                                entries.iter().find(|e| matches!(
                                    e.kind,
                                    ServiceZoneNatKind::BoundaryNtp { .. }
                                )),
                                None
                            );
                        }
                        ZoneKind::ExternalDns => {
                            assert_eq!(
                                entries.iter().find(|e| matches!(
                                    e.kind,
                                    ServiceZoneNatKind::ExternalDns { .. }
                                )),
                                None
                            );
                        }
                        ZoneKind::Nexus => {
                            assert_eq!(
                                entries.iter().find(|e| matches!(
                                    e.kind,
                                    ServiceZoneNatKind::Nexus { .. }
                                )),
                                None
                            );
                        }
                        _ => panic!("unexpected missing zone type {t:?}"),
                    }
                }
            }
            Err(ServiceZoneNatEntriesError::OverlappingNatEntry {
                ip,
                zone_1_id,
                zone_1_lo,
                zone_1_hi,
                zone_2_id,
                zone_2_lo,
                zone_2_hi,
            }) => {
                // Check that the error faithfully reported the two entries'
                // fields.
                let e1 =
                    entries.iter().find(|e| e.zone_id == zone_1_id).unwrap();
                let e2 =
                    entries.iter().find(|e| e.zone_id == zone_2_id).unwrap();
                assert_eq!(ip, e1.kind.external_ip());
                assert_eq!(zone_1_lo, e1.kind.nat_port_range().0);
                assert_eq!(zone_1_hi, e1.kind.nat_port_range().1);
                assert_eq!(ip, e2.kind.external_ip());
                assert_eq!(zone_2_lo, e2.kind.nat_port_range().0);
                assert_eq!(zone_2_hi, e2.kind.nat_port_range().1);

                // Check that the port ranges actually overlap.
                assert!(zone_1_lo <= zone_2_hi && zone_2_lo <= zone_1_hi);
            }
        }
    }
}
