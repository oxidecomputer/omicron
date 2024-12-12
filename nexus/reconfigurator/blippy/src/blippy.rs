// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::inventory::ZpoolName;
use omicron_common::address::DnsSubnet;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::MacAddr;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::net::Ipv6Addr;

use crate::checks;

#[derive(Debug, Clone)]
pub struct Note<'a> {
    pub component: Component,
    pub severity: Severity,
    pub kind: Kind<'a>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub enum Component {
    Sled(SledUuid),
}

#[derive(Debug, Clone)]
pub enum Kind<'a> {
    /// Two running zones have the same underlay IP address.
    DuplicateUnderlayIp {
        zone1: &'a BlueprintZoneConfig,
        zone2: &'a BlueprintZoneConfig,
        ip: Ipv6Addr,
    },
    /// A sled has two zones that are not members of the same sled subnet.
    SledWithMixedUnderlaySubnets {
        zone1: &'a BlueprintZoneConfig,
        zone2: &'a BlueprintZoneConfig,
    },
    /// Two sleds are using the same sled subnet.
    ConflictingSledSubnets {
        sled1: SledUuid,
        sled2: SledUuid,
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
    SledMissingDatasets { sled_id: SledUuid },
    /// A sled is missing entries in `Blueprint::blueprint_disks`.
    SledMissingDisks { sled_id: SledUuid },
    /// A dataset is present but not referenced by any in-service zone or disk.
    OrphanedDataset { dataset: &'a BlueprintDatasetConfig },
    /// A dataset claims to be on a zpool that does not exist.
    DatasetOnNonexistentDisk { dataset: &'a BlueprintDatasetConfig },
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

    pub fn into_report(self) -> BlippyReport<'a> {
        let Self { blueprint, notes } = self;
        BlippyReport { blueprint, notes }
    }
}

#[derive(Debug)]
pub struct BlippyReport<'a> {
    blueprint: &'a Blueprint,
    notes: Vec<Note<'a>>,
}

impl<'a> BlippyReport<'a> {
    pub fn blueprint(&self) -> &'a Blueprint {
        self.blueprint
    }

    pub fn notes(&self) -> &[Note<'a>] {
        &self.notes
    }
}
