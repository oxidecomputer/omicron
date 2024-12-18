// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::checks;
use crate::report::BlippyReport;
use crate::report::BlippyReportSortKey;
use core::fmt;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::inventory::ZpoolName;
use omicron_common::address::DnsSubnet;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::MacAddr;
use omicron_common::disk::DatasetKind;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::collections::BTreeSet;
use std::net::IpAddr;

#[derive(Debug, Clone)]
pub struct Note<'a> {
    pub component: Component,
    pub severity: Severity,
    pub kind: Kind<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// Indicator of a serious problem that means the blueprint is invalid.
    ///
    /// Many common blueprint use cases are likely to fail in some way if
    /// performed with a blueprint reporting a `Fatal` note:
    ///
    /// * Uploading the blueprint to Nexus
    /// * Attempting to execute the blueprint
    /// * Attempting to generate a new child blueprint
    Fatal,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Fatal => write!(f, "FATAL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Component {
    Sled(SledUuid),
}

impl fmt::Display for Component {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Component::Sled(id) => write!(f, "sled {id}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind<'a> {
    /// Two running zones have the same underlay IP address.
    DuplicateUnderlayIp {
        zone1: &'a BlueprintZoneConfig,
        zone2: &'a BlueprintZoneConfig,
    },
    /// A sled has two zones that are not members of the same sled subnet.
    SledWithMixedUnderlaySubnets {
        zone1: &'a BlueprintZoneConfig,
        zone2: &'a BlueprintZoneConfig,
    },
    /// Two sleds are using the same sled subnet.
    ConflictingSledSubnets {
        other_sled: SledUuid,
        subnet: Ipv6Subnet<SLED_PREFIX>,
    },
    /// An internal DNS zone has an IP that is not one of the expected rack DNS
    /// subnets.
    InternalDnsZoneBadSubnet {
        zone: &'a BlueprintZoneConfig,
        rack_dns_subnets: BTreeSet<DnsSubnet>,
    },
    /// Two running zones have the same external IP address.
    DuplicateExternalIp {
        zone1: &'a BlueprintZoneConfig,
        zone2: &'a BlueprintZoneConfig,
        ip: IpAddr,
    },
    /// Two running zones' NICs have the same IP address.
    DuplicateNicIp {
        zone1: &'a BlueprintZoneConfig,
        zone2: &'a BlueprintZoneConfig,
        ip: IpAddr,
    },
    /// Two running zones' NICs have the same MAC address.
    DuplicateNicMac {
        zone1: &'a BlueprintZoneConfig,
        zone2: &'a BlueprintZoneConfig,
        mac: MacAddr,
    },
    /// Two zones with the same durable dataset kind are on the same zpool.
    ZoneDurableDatasetCollision {
        zone1: &'a BlueprintZoneConfig,
        zone2: &'a BlueprintZoneConfig,
        zpool: ZpoolName,
    },
    /// Two zones with the same filesystem dataset kind are on the same zpool.
    ZpoolFilesystemDatasetCollision {
        zone1: &'a BlueprintZoneConfig,
        zone2: &'a BlueprintZoneConfig,
        zpool: ZpoolName,
    },
    /// One zpool has two datasets of the same kind.
    ZpoolWithDuplicateDatasetKinds {
        dataset1: &'a BlueprintDatasetConfig,
        dataset2: &'a BlueprintDatasetConfig,
        zpool: ZpoolUuid,
    },
    /// A zpool is missing its Debug dataset.
    ZpoolMissingDebugDataset { zpool: ZpoolUuid },
    /// A zpool is missing its Zone Root dataset.
    ZpoolMissingZoneRootDataset { zpool: ZpoolUuid },
    /// A zone's filesystem dataset is missing from `blueprint_datasets`.
    ZoneMissingFilesystemDataset { zone: &'a BlueprintZoneConfig },
    /// A zone's durable dataset is missing from `blueprint_datasets`.
    ZoneMissingDurableDataset { zone: &'a BlueprintZoneConfig },
    /// A zone's durable dataset and transient root dataset are on different
    /// zpools.
    ZoneWithDatasetsOnDifferentZpools {
        zone: &'a BlueprintZoneConfig,
        durable_zpool: ZpoolName,
        transient_zpool: ZpoolName,
    },
    /// A sled is missing entries in `Blueprint::blueprint_datasets`.
    ///
    /// `why` indicates why we expected this sled to have an entry.
    SledMissingDatasets { why: &'static str },
    /// A sled is missing entries in `Blueprint::blueprint_disks`.
    ///
    /// `why` indicates why we expected this sled to have an entry.
    SledMissingDisks { why: &'static str },
    /// A dataset is present but not referenced by any in-service zone or disk.
    OrphanedDataset { dataset: &'a BlueprintDatasetConfig },
    /// A dataset claims to be on a zpool that does not exist.
    DatasetOnNonexistentZpool { dataset: &'a BlueprintDatasetConfig },
}

impl fmt::Display for Kind<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Kind::DuplicateUnderlayIp { zone1, zone2 } => {
                write!(
                    f,
                    "duplicate underlay IP {} ({:?} {} and {:?} {})",
                    zone1.underlay_ip(),
                    zone1.zone_type.kind(),
                    zone1.id,
                    zone2.zone_type.kind(),
                    zone2.id,
                )
            }
            Kind::SledWithMixedUnderlaySubnets { zone1, zone2 } => {
                write!(
                    f,
                    "zones have underlay IPs on two different sled subnets: \
                     {:?} {} ({}) and {:?} {} ({})",
                    zone1.zone_type.kind(),
                    zone1.id,
                    zone1.underlay_ip(),
                    zone2.zone_type.kind(),
                    zone2.id,
                    zone2.underlay_ip(),
                )
            }
            Kind::ConflictingSledSubnets { other_sled, subnet } => {
                write!(
                    f,
                    "duplicate sled subnet {} with sled {other_sled}",
                    subnet.net()
                )
            }
            Kind::InternalDnsZoneBadSubnet { zone, rack_dns_subnets } => {
                write!(
                    f,
                    "internal DNS zone {} underlay IP {} is not \
                     one of the reserved rack DNS subnets ({:?})",
                    zone.id,
                    zone.underlay_ip(),
                    rack_dns_subnets
                )
            }
            Kind::DuplicateExternalIp { zone1, zone2, ip } => {
                write!(
                    f,
                    "duplicate external IP {ip} ({:?} {} and {:?} {})",
                    zone1.zone_type.kind(),
                    zone1.id,
                    zone2.zone_type.kind(),
                    zone2.id,
                )
            }
            Kind::DuplicateNicIp { zone1, zone2, ip } => {
                write!(
                    f,
                    "duplicate NIC IP {ip} ({:?} {} and {:?} {})",
                    zone1.zone_type.kind(),
                    zone1.id,
                    zone2.zone_type.kind(),
                    zone2.id,
                )
            }
            Kind::DuplicateNicMac { zone1, zone2, mac } => {
                write!(
                    f,
                    "duplicate NIC MAC {mac} ({:?} {} and {:?} {})",
                    zone1.zone_type.kind(),
                    zone1.id,
                    zone2.zone_type.kind(),
                    zone2.id,
                )
            }
            Kind::ZoneDurableDatasetCollision { zone1, zone2, zpool } => {
                write!(
                    f,
                    "zpool {zpool} has two zone datasets of the same kind \
                     ({:?} {} and {:?} {})",
                    zone1.zone_type.kind(),
                    zone1.id,
                    zone2.zone_type.kind(),
                    zone2.id,
                )
            }
            Kind::ZpoolFilesystemDatasetCollision { zone1, zone2, zpool } => {
                write!(
                    f,
                    "zpool {zpool} has two zone filesystems of the same kind \
                     ({:?} {} and {:?} {})",
                    zone1.zone_type.kind(),
                    zone1.id,
                    zone2.zone_type.kind(),
                    zone2.id,
                )
            }
            Kind::ZpoolWithDuplicateDatasetKinds {
                dataset1,
                dataset2,
                zpool,
            } => {
                write!(
                    f,
                    "two datasets of the same kind on zpool {zpool} \
                     ({:?} {} and {:?} {})",
                    dataset1.kind, dataset1.id, dataset2.kind, dataset2.id,
                )
            }
            Kind::ZpoolMissingDebugDataset { zpool } => {
                write!(f, "zpool {zpool} is missing its Debug dataset")
            }
            Kind::ZpoolMissingZoneRootDataset { zpool } => {
                write!(f, "zpool {zpool} is missing its Zone Root dataset")
            }
            Kind::ZoneMissingFilesystemDataset { zone } => {
                write!(
                    f,
                    "in-service zone's filesytem dataset is missing: {:?} {}",
                    zone.zone_type.kind(),
                    zone.id,
                )
            }
            Kind::ZoneMissingDurableDataset { zone } => {
                write!(
                    f,
                    "in-service zone's durable dataset is missing: {:?} {}",
                    zone.zone_type.kind(),
                    zone.id,
                )
            }
            Kind::ZoneWithDatasetsOnDifferentZpools {
                zone,
                durable_zpool,
                transient_zpool,
            } => {
                write!(
                    f,
                    "zone {:?} {} has its durable dataset on \
                     zpool {durable_zpool} but its root dataset on \
                     zpool {transient_zpool}",
                    zone.zone_type.kind(),
                    zone.id,
                )
            }
            Kind::SledMissingDatasets { why } => {
                write!(f, "missing entry in blueprint_datasets ({why})")
            }
            Kind::SledMissingDisks { why } => {
                write!(f, "missing entry in blueprint_disks ({why})")
            }
            Kind::OrphanedDataset { dataset } => {
                let parent = match dataset.kind {
                    DatasetKind::Cockroach
                    | DatasetKind::Crucible
                    | DatasetKind::Clickhouse
                    | DatasetKind::ClickhouseKeeper
                    | DatasetKind::ClickhouseServer
                    | DatasetKind::ExternalDns
                    | DatasetKind::InternalDns
                    | DatasetKind::TransientZone { .. } => "zone",
                    DatasetKind::TransientZoneRoot
                    | DatasetKind::Debug
                    | DatasetKind::Update => "disk",
                };
                write!(
                    f,
                    "in-service dataset ({:?} {}) with no associated {parent}",
                    dataset.kind, dataset.id
                )
            }
            Kind::DatasetOnNonexistentZpool { dataset } => {
                write!(
                    f,
                    "in-service dataset ({:?} {}) on non-existent zpool {}",
                    dataset.kind, dataset.id, dataset.pool
                )
            }
        }
    }
}

impl Note<'_> {
    pub fn display(&self, sort_key: BlippyReportSortKey) -> NoteDisplay<'_> {
        NoteDisplay { note: self, sort_key }
    }
}

#[derive(Debug)]
pub struct NoteDisplay<'a> {
    note: &'a Note<'a>,
    sort_key: BlippyReportSortKey,
}

impl fmt::Display for NoteDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.sort_key {
            BlippyReportSortKey::Component => {
                write!(
                    f,
                    "{}: {} note: {}",
                    self.note.component, self.note.severity, self.note.kind
                )
            }
            BlippyReportSortKey::Severity => {
                write!(
                    f,
                    "{} note: {}: {}",
                    self.note.severity, self.note.component, self.note.kind
                )
            }
        }
    }
}

#[derive(Debug)]
pub struct Blippy<'a> {
    blueprint: &'a Blueprint,
    notes: Vec<Note<'a>>,
}

impl<'a> Blippy<'a> {
    pub fn new(blueprint: &'a Blueprint) -> Self {
        let mut slf = Self { blueprint, notes: Vec::new() };
        checks::perform_all_blueprint_only_checks(&mut slf);
        slf
    }

    pub fn blueprint(&self) -> &'a Blueprint {
        self.blueprint
    }

    pub(crate) fn push_note(
        &mut self,
        component: Component,
        severity: Severity,
        kind: Kind<'a>,
    ) {
        self.notes.push(Note { component, severity, kind });
    }

    pub fn into_report(
        self,
        sort_key: BlippyReportSortKey,
    ) -> BlippyReport<'a> {
        BlippyReport::new(self.blueprint, self.notes, sort_key)
    }
}
