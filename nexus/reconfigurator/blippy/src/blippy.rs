// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::checks;
use crate::report::BlippyReport;
use crate::report::BlippyReportSortKey;
use core::fmt;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::inventory::ZpoolName;
use omicron_common::address::DnsSubnet;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Generation;
use omicron_common::api::external::MacAddr;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::net::SocketAddrV6;
use tufaceous_artifact::ArtifactHash;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    pub severity: Severity,
    pub kind: Kind,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// Indicates an issue with a blueprint that should be corrected by a future
    /// planning run.
    BackwardsCompatibility,
    /// Indicates a serious problem that means the blueprint is invalid.
    Fatal,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::BackwardsCompatibility => write!(f, "BACKCOMPAT"),
            Severity::Fatal => write!(f, "FATAL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    Sled { sled_id: SledUuid, kind: SledKind },
}

impl Kind {
    pub fn display_component(&self) -> impl fmt::Display + '_ {
        enum Component<'a> {
            Sled(&'a SledUuid),
        }

        impl fmt::Display for Component<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    Component::Sled(id) => write!(f, "sled {id}"),
                }
            }
        }

        match self {
            Kind::Sled { sled_id, .. } => Component::Sled(sled_id),
        }
    }

    pub fn display_subkind(&self) -> impl fmt::Display + '_ {
        enum Subkind<'a> {
            Sled(&'a SledKind),
        }

        impl fmt::Display for Subkind<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    Subkind::Sled(kind) => write!(f, "{kind}"),
                }
            }
        }

        match self {
            Kind::Sled { kind, .. } => Subkind::Sled(kind),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SledKind {
    /// Two running zones have the same underlay IP address.
    DuplicateUnderlayIp {
        zone1: BlueprintZoneConfig,
        zone2: BlueprintZoneConfig,
    },
    /// A sled has two zones that are not members of the same sled subnet.
    SledWithMixedUnderlaySubnets {
        zone1: BlueprintZoneConfig,
        zone2: BlueprintZoneConfig,
    },
    /// Two sleds are using the same sled subnet.
    ConflictingSledSubnets {
        other_sled: SledUuid,
        subnet: Ipv6Subnet<SLED_PREFIX>,
    },
    /// An internal DNS zone has an IP that is not one of the expected rack DNS
    /// subnets.
    InternalDnsZoneBadSubnet {
        zone: BlueprintZoneConfig,
        rack_dns_subnets: BTreeSet<DnsSubnet>,
    },
    /// Two running zones have the same external IP address.
    DuplicateExternalIp {
        zone1: BlueprintZoneConfig,
        zone2: BlueprintZoneConfig,
        ip: IpAddr,
    },
    /// Two running zones' NICs have the same IP address.
    DuplicateNicIp {
        zone1: BlueprintZoneConfig,
        zone2: BlueprintZoneConfig,
        ip: IpAddr,
    },
    /// Two running zones' NICs have the same MAC address.
    DuplicateNicMac {
        zone1: BlueprintZoneConfig,
        zone2: BlueprintZoneConfig,
        mac: MacAddr,
    },
    /// Two zones with the same durable dataset kind are on the same zpool.
    ZoneDurableDatasetCollision {
        zone1: BlueprintZoneConfig,
        zone2: BlueprintZoneConfig,
        zpool: ZpoolName,
    },
    /// Two zones with the same filesystem dataset kind are on the same zpool.
    ZoneFilesystemDatasetCollision {
        zone1: BlueprintZoneConfig,
        zone2: BlueprintZoneConfig,
        zpool: ZpoolName,
    },
    /// One zpool has two datasets of the same kind.
    ZpoolWithDuplicateDatasetKinds {
        dataset1: BlueprintDatasetConfig,
        dataset2: BlueprintDatasetConfig,
        zpool: ZpoolUuid,
    },
    /// A zpool is missing its Debug dataset.
    ZpoolMissingDebugDataset { zpool: ZpoolUuid },
    /// A zpool is missing its Zone Root dataset.
    ZpoolMissingZoneRootDataset { zpool: ZpoolUuid },
    /// A zone's filesystem dataset is missing from `blueprint_datasets`.
    ZoneMissingFilesystemDataset { zone: BlueprintZoneConfig },
    /// A zone's durable dataset is missing from `blueprint_datasets`.
    ZoneMissingDurableDataset { zone: BlueprintZoneConfig },
    /// A zone's durable dataset and transient root dataset are on different
    /// zpools.
    ZoneWithDatasetsOnDifferentZpools {
        zone: BlueprintZoneConfig,
        durable_zpool: ZpoolName,
        transient_zpool: ZpoolName,
    },
    /// A dataset is present but not referenced by any in-service zone or disk.
    OrphanedDataset { dataset: BlueprintDatasetConfig },
    /// A dataset claims to be on a zpool that does not exist.
    DatasetOnNonexistentZpool { dataset: BlueprintDatasetConfig },
    /// A Crucible dataset does not have its `address` set to its corresponding
    /// Crucible zone.
    CrucibleDatasetWithIncorrectAddress {
        dataset: BlueprintDatasetConfig,
        expected_address: SocketAddrV6,
    },
    /// A non-Crucible dataset has an address.
    NonCrucibleDatasetWithAddress {
        dataset: BlueprintDatasetConfig,
        address: SocketAddrV6,
    },
    MupdateOverrideWithArtifactZone {
        mupdate_override_id: MupdateOverrideUuid,
        zone: BlueprintZoneConfig,
        version: BlueprintArtifactVersion,
        hash: ArtifactHash,
    },
    MupdateOverrideWithHostPhase2Artifact {
        mupdate_override_id: MupdateOverrideUuid,
        slot: M2Slot,
        version: BlueprintArtifactVersion,
        hash: ArtifactHash,
    },
    /// Nexus zones with the same generation have different image sources.
    NexusZoneGenerationImageSourceMismatch {
        zone1: BlueprintZoneConfig,
        zone2: BlueprintZoneConfig,
        generation: Generation,
    },
}

impl fmt::Display for SledKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SledKind::DuplicateUnderlayIp { zone1, zone2 } => {
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
            SledKind::SledWithMixedUnderlaySubnets { zone1, zone2 } => {
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
            SledKind::ConflictingSledSubnets { other_sled, subnet } => {
                write!(
                    f,
                    "duplicate sled subnet {} with sled {other_sled}",
                    subnet.net()
                )
            }
            SledKind::InternalDnsZoneBadSubnet { zone, rack_dns_subnets } => {
                write!(
                    f,
                    "internal DNS zone {} underlay IP {} is not \
                     one of the reserved rack DNS subnets ({:?})",
                    zone.id,
                    zone.underlay_ip(),
                    rack_dns_subnets
                )
            }
            SledKind::DuplicateExternalIp { zone1, zone2, ip } => {
                write!(
                    f,
                    "duplicate external IP {ip} ({:?} {} and {:?} {})",
                    zone1.zone_type.kind(),
                    zone1.id,
                    zone2.zone_type.kind(),
                    zone2.id,
                )
            }
            SledKind::DuplicateNicIp { zone1, zone2, ip } => {
                write!(
                    f,
                    "duplicate NIC IP {ip} ({:?} {} and {:?} {})",
                    zone1.zone_type.kind(),
                    zone1.id,
                    zone2.zone_type.kind(),
                    zone2.id,
                )
            }
            SledKind::DuplicateNicMac { zone1, zone2, mac } => {
                write!(
                    f,
                    "duplicate NIC MAC {mac} ({:?} {} and {:?} {})",
                    zone1.zone_type.kind(),
                    zone1.id,
                    zone2.zone_type.kind(),
                    zone2.id,
                )
            }
            SledKind::ZoneDurableDatasetCollision { zone1, zone2, zpool } => {
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
            SledKind::ZoneFilesystemDatasetCollision {
                zone1,
                zone2,
                zpool,
            } => {
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
            SledKind::ZpoolWithDuplicateDatasetKinds {
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
            SledKind::ZpoolMissingDebugDataset { zpool } => {
                write!(f, "zpool {zpool} is missing its Debug dataset")
            }
            SledKind::ZpoolMissingZoneRootDataset { zpool } => {
                write!(f, "zpool {zpool} is missing its Zone Root dataset")
            }
            SledKind::ZoneMissingFilesystemDataset { zone } => {
                write!(
                    f,
                    "in-service zone's filesytem dataset is missing: {:?} {}",
                    zone.zone_type.kind(),
                    zone.id,
                )
            }
            SledKind::ZoneMissingDurableDataset { zone } => {
                write!(
                    f,
                    "in-service zone's durable dataset is missing: {:?} {}",
                    zone.zone_type.kind(),
                    zone.id,
                )
            }
            SledKind::ZoneWithDatasetsOnDifferentZpools {
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
            SledKind::OrphanedDataset { dataset } => {
                let parent = match dataset.kind {
                    DatasetKind::Cockroach
                    | DatasetKind::Crucible
                    | DatasetKind::Clickhouse
                    | DatasetKind::ClickhouseKeeper
                    | DatasetKind::ClickhouseServer
                    | DatasetKind::ExternalDns
                    | DatasetKind::InternalDns
                    | DatasetKind::TransientZone { .. } => "zone",
                    DatasetKind::TransientZoneRoot | DatasetKind::Debug => {
                        "disk"
                    }
                };
                write!(
                    f,
                    "in-service dataset ({:?} {}) with no associated {parent}",
                    dataset.kind, dataset.id
                )
            }
            SledKind::DatasetOnNonexistentZpool { dataset } => {
                write!(
                    f,
                    "in-service dataset ({:?} {}) on non-existent zpool {}",
                    dataset.kind, dataset.id, dataset.pool
                )
            }
            SledKind::CrucibleDatasetWithIncorrectAddress {
                dataset,
                expected_address,
            } => {
                write!(
                    f,
                    "Crucible dataset {} has bad address {:?} (expected {})",
                    dataset.id, dataset.address, expected_address,
                )
            }
            SledKind::NonCrucibleDatasetWithAddress { dataset, address } => {
                write!(
                    f,
                    "non-Crucible dataset ({:?} {}) has an address: {} \
                     (only Crucible datasets should have addresses)",
                    dataset.kind, dataset.id, address,
                )
            }
            SledKind::MupdateOverrideWithArtifactZone {
                mupdate_override_id,
                zone,
                version,
                hash,
            } => {
                write!(
                    f,
                    "sled has remove_mupdate_override set ({mupdate_override_id}), \
                     but zone {} image source is set to Artifact (version {version}, \
                     hash {hash})",
                    zone.id,
                )
            }
            SledKind::MupdateOverrideWithHostPhase2Artifact {
                mupdate_override_id,
                slot,
                version,
                hash,
            } => {
                write!(
                    f,
                    "sled has remove_mupdate_override set ({mupdate_override_id}), \
                     but host phase 2 slot {slot} image source is set to Artifact \
                     (version {version}, hash {hash})",
                )
            }
            SledKind::NexusZoneGenerationImageSourceMismatch {
                zone1,
                zone2,
                generation,
            } => {
                write!(
                    f,
                    "Nexus zones {} and {} both have generation {generation} but \
                     different image sources ({:?} vs {:?})",
                    zone1.id, zone2.id, zone1.image_source, zone2.image_source,
                )
            }
        }
    }
}

impl Note {
    pub fn display(&self, sort_key: BlippyReportSortKey) -> NoteDisplay<'_> {
        NoteDisplay { note: self, sort_key }
    }
}

#[derive(Debug)]
pub struct NoteDisplay<'a> {
    note: &'a Note,
    sort_key: BlippyReportSortKey,
}

impl fmt::Display for NoteDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.sort_key {
            BlippyReportSortKey::Kind => {
                write!(
                    f,
                    "{}: {} note: {}",
                    self.note.kind.display_component(),
                    self.note.severity,
                    self.note.kind.display_subkind(),
                )
            }
            BlippyReportSortKey::Severity => {
                write!(
                    f,
                    "{} note: {}: {}",
                    self.note.severity,
                    self.note.kind.display_component(),
                    self.note.kind.display_subkind(),
                )
            }
        }
    }
}

#[derive(Debug)]
pub struct Blippy<'a> {
    blueprint: &'a Blueprint,
    notes: Vec<Note>,
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

    pub(crate) fn push_sled_note(
        &mut self,
        sled_id: SledUuid,
        severity: Severity,
        kind: SledKind,
    ) {
        self.notes.push(Note { severity, kind: Kind::Sled { sled_id, kind } });
    }

    pub fn into_report(
        self,
        sort_key: BlippyReportSortKey,
    ) -> BlippyReport<'a> {
        BlippyReport::new(self.blueprint, self.notes, sort_key)
    }
}
