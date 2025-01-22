// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An implementation of a `VisitBlueprint` visitor that is used to construct
//! displayable output.

use super::blueprint_display::{
    constants::*, linear_table_modified, linear_table_unchanged,
    BpClickhouseServersTableSchema, BpDatasetsTableSchema, BpDiffState,
    BpGeneration, BpOmicronZonesTableSchema, BpPhysicalDisksTableSchema,
    BpTable, BpTableColumn, BpTableData, BpTableRow, KvListWithHeading, KvPair,
};
use super::diff_visitors::visit_blueprint::{
    SledInsert, SledRemove, VisitBlueprint,
};
use super::diff_visitors::visit_blueprint_datasets_config::VisitBlueprintDatasetsConfig;
use super::diff_visitors::visit_blueprint_physical_disks_config::VisitBlueprintPhysicalDisksConfig;
use super::diff_visitors::visit_blueprint_zones_config::VisitBlueprintZonesConfig;
use super::diff_visitors::{BpVisitorContext, Change};
use super::id_map::IdMap;
use super::{
    zone_sort_key, Blueprint, BlueprintDatasetDisposition,
    BlueprintPhysicalDiskDisposition, ClickhouseClusterConfig,
    CockroachDbPreserveDowngrade, DiffBeforeClickhouseClusterConfig,
};

use diffus::{edit::Edit, Diffable};
use nexus_sled_agent_shared::inventory::ZoneKind;
use omicron_common::api::external::{ByteCount, Generation};
use omicron_common::disk::{CompressionAlgorithm, DiskIdentity};
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::{OmicronZoneUuid, PhysicalDiskUuid};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::deployment::blueprint_display::BpClickhouseKeepersTableSchema;
use crate::deployment::{
    BlueprintDatasetConfig, BlueprintDatasetsConfig, BlueprintMetadata,
    BlueprintPhysicalDiskConfig, BlueprintPhysicalDisksConfig,
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZoneType,
    BlueprintZonesConfig, CollectionDatasetIdentifier, DiffBeforeMetadata,
    ZoneSortKey, ZpoolName,
};
use crate::external_api::views::SledState;

/// Tables for added sleds in a blueprint diff
pub struct SledTables {
    pub disks: Option<BpTable>,
    pub datasets: Option<BpTable>,
    pub zones: Option<BpTable>,
}

/// Output of a `BlueprintDiffer`
#[derive(Default)]
pub struct BpDiffOutput {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub added_sleds: BTreeMap<SledUuid, SledTables>,
    pub removed_sleds: BTreeMap<SledUuid, SledTables>,
}

/// A single value in a diff.
pub enum DiffValue<'e, T> {
    Unchanged(&'e T),
    Change(Change<'e, T>),
}

pub struct ModifiedZone<'e> {
    disposition: DiffValue<'e, BlueprintZoneDisposition>,
    filesystem_pool: DiffValue<'e, Option<ZpoolName>>,
    zone_type: DiffValue<'e, BlueprintZoneType>,
}

impl<'e> ModifiedZone<'e> {
    /// Initialize a `ModifiedZone`.
    ///
    /// We always initialize to the `before` state as if this value is
    /// unchanged. If a change callback fires for a given field, then we'll
    /// update the value.
    pub fn new(before: &'e BlueprintZoneConfig) -> ModifiedZone<'e> {
        ModifiedZone {
            disposition: DiffValue::Unchanged(&before.disposition),
            filesystem_pool: DiffValue::Unchanged(&before.filesystem_pool),
            zone_type: DiffValue::Unchanged(&before.zone_type),
        }
    }
}

impl<'e> ModifiedDisk<'e> {
    /// Initialize a `ModifiedDisk`.
    ///
    /// We always initialize to the `before` state as if this value is
    /// unchanged. If a change callback fires for a given field, then we'll
    /// update the value.
    pub fn new(before: &'e BlueprintPhysicalDiskConfig) -> ModifiedDisk<'e> {
        ModifiedDisk { disposition: DiffValue::Unchanged(&before.disposition) }
    }
}

pub struct ModifiedDisk<'e> {
    disposition: DiffValue<'e, BlueprintPhysicalDiskDisposition>,
}

pub struct ModifiedDataset<'e> {
    dispostion: DiffValue<'e, BlueprintDatasetDisposition>,
    quota: DiffValue<'e, Option<ByteCount>>,
    reservation: DiffValue<'e, Option<ByteCount>>,
    compression: DiffValue<'e, CompressionAlgorithm>,
}

/// All modifications of a sled that we track for purposes of diff display output
pub struct ModifiedSled<'e> {
    sled_state: DiffValue<'e, SledState>,
    zones_generation: DiffValue<'e, Generation>,
    zones_inserted: IdMap<BlueprintZoneConfig>,
    zones_removed: IdMap<BlueprintZoneConfig>,
    zones_unchanged: IdMap<BlueprintZoneConfig>,
    zones_modified: BTreeMap<OmicronZoneUuid, ModifiedZone<'e>>,
    disks_generation: DiffValue<'e, Generation>,
    disks_inserted: IdMap<BlueprintPhysicalDiskConfig>,
    disks_removed: IdMap<BlueprintPhysicalDiskConfig>,
    disks_unchanged: IdMap<BlueprintPhysicalDiskConfig>,
    disks_modified: BTreeMap<PhysicalDiskUuid, ModifiedDisk<'e>>,
    datasets_generation: DiffValue<'e, Generation>,
    datasets_inserted: IdMap<BlueprintDatasetConfig>,
    datasets_removed: IdMap<BlueprintDatasetConfig>,
    datasets_unchanged: IdMap<BlueprintDatasetConfig>,
    datasets_modified: BTreeMap<PhysicalDiskUuid, ModifiedDataset<'e>>,
}

impl<'e> ModifiedSled<'e> {
    /// Initialize a `ModifiedSled`.
    ///
    /// We always initialized `DiffValue`s to the `before` state as if they are
    /// unchanged. If a change callback fires, then we'll update the state. We
    /// do the same for unchanged zones, disks, and datasets.
    pub fn new(before: &'e Blueprint, sled_id: SledUuid) -> ModifiedSled<'e> {
        let zones_cfg = before.blueprint_zones.get(&sled_id).unwrap();
        let disks_cfg = before.blueprint_disks.get(&sled_id).unwrap();
        let datasets_cfg = before.blueprint_datasets.get(&sled_id).unwrap();
        ModifiedSled {
            sled_state: DiffValue::Unchanged(
                before
                    .sled_state
                    .get(&sled_id)
                    .unwrap_or(&SledState::Decommissioned),
            ),
            zones_generation: DiffValue::Unchanged(&zones_cfg.generation),
            zones_inserted: IdMap::new(),
            zones_removed: IdMap::new(),
            zones_unchanged: zones_cfg.zones.clone(),
            zones_modified: BTreeMap::new(),
            disks_generation: DiffValue::Unchanged(&disks_cfg.generation),
            disks_inserted: IdMap::new(),
            disks_removed: IdMap::new(),
            disks_unchanged: disks_cfg.disks.clone(),
            disks_modified: BTreeMap::new(),
            datasets_generation: DiffValue::Unchanged(&datasets_cfg.generation),
            datasets_inserted: IdMap::new(),
            datasets_removed: IdMap::new(),
            datasets_unchanged: datasets_cfg.datasets.clone(),
            datasets_modified: BTreeMap::new(),
        }
    }
}

/// Accumulated state about diffs inside a `BlueprintDiffer`.
///
/// Tied to the lifetime of a diffus diff.
#[derive(Default)]
pub struct BpDiffAccumulator<'e> {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    added_sleds: BTreeMap<SledUuid, SledInsert<'e>>,
    removed_sleds: BTreeMap<SledUuid, SledRemove<'e>>,
    unchanged_sleds: BTreeSet<SledUuid>,
    modified_sleds: BTreeMap<SledUuid, ModifiedSled<'e>>,
}

/// A mechanism for creating tables from blueprint diffs via a `VisitBlueprint` implementation.
pub struct BlueprintDiffer<'e> {
    before: &'e Blueprint,
    after: &'e Blueprint,
    ctx: BpVisitorContext,
    /// An accumulator for diff state while traversing a visitor
    acc: BpDiffAccumulator<'e>,
    diff: Edit<'e, Blueprint>,
}

impl<'e> BlueprintDiffer<'e> {
    pub fn new(
        before: &'e Blueprint,
        after: &'e Blueprint,
    ) -> BlueprintDiffer<'e> {
        let diff = before.diff(&after);
        BlueprintDiffer {
            before,
            after,
            ctx: BpVisitorContext::default(),
            acc: BpDiffAccumulator::default(),
            diff,
        }
    }
}

impl<'e> VisitBlueprint<'e> for BlueprintDiffer<'e> {
    type ZonesVisitor = Self;
    type DisksVisitor = Self;
    type DatasetsVisitor = Self;

    fn zones_visitor(&mut self) -> &mut Self::ZonesVisitor {
        &mut *self
    }

    fn disks_visitor(&mut self) -> &mut Self::DisksVisitor {
        &mut *self
    }

    fn datasets_visitor(&mut self) -> &mut Self::DatasetsVisitor {
        &mut *self
    }

    fn visit_sled_insert(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: SledInsert<'e>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_sled_insert".to_string();
            self.acc.errors.push(err);
            return;
        };

        self.acc.added_sleds.insert(sled_id, node);
    }

    fn visit_sled_remove(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: SledRemove<'e>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_sled_remove".to_string();
            self.acc.errors.push(err);
            return;
        };

        self.acc.removed_sleds.insert(sled_id, node);
    }

    fn visit_sled_state_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, SledState>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err = "Missing sled id in ctx for visit_sled_state_change"
                .to_string();
            self.acc.errors.push(err);
            return;
        };
        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.sled_state = DiffValue::Change(change);
    }
}

impl<'e> VisitBlueprintZonesConfig<'e> for BlueprintDiffer<'e> {
    fn visit_generation_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, Generation>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for zones visit_generation_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };
        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.zones_generation = DiffValue::Change(change);
    }

    fn visit_zones_insert(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintZoneConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_zones_insert".to_string();
            self.acc.errors.push(err);
            return;
        };
        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.zones_inserted.insert(node.clone());
    }

    fn visit_zones_remove(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintZoneConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_zones_remove".to_string();
            self.acc.errors.push(err);
            return;
        };
        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.zones_removed.insert(node.clone());

        // Remove this zone from the unchanged zones to compensate
        // for constructor initialization.
        s.zones_unchanged.remove(&node.id);
    }

    fn visit_zone_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, BlueprintZoneConfig>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_zone_change".to_string();
            self.acc.errors.push(err);
            return;
        };

        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.zones_modified
            .insert(change.before.id, ModifiedZone::new(&change.before));

        // At least one of the fields for this zone is going to change in a
        // follow up callback, so we want to remove it from the unchanged zones
        // to compensate for constructor initialization.
        s.zones_unchanged.remove(&change.before.id);
    }

    fn visit_zone_disposition_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, BlueprintZoneDisposition>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_zone_disposition_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        let Some(zone_id) = ctx.zone_id else {
            let err =
                "Missing zone id in ctx for visit_zone_disposition_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_zone_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.acc.modified_sleds.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_zone_change` callback fired and
        // created the `ModifiedZone` entry.
        s.zones_modified.get_mut(&zone_id).unwrap().disposition =
            DiffValue::Change(change);
    }

    fn visit_zone_filesystem_pool_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, Option<ZpoolName>>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_zone_filesystem_pool_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        let Some(zone_id) = ctx.zone_id else {
            let err =
                "Missing zone id in ctx for visit_zone_filesystem_pool_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_zone_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.acc.modified_sleds.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_zone_change` callback fired and
        // created the `ModifiedZone` entry.
        s.zones_modified.get_mut(&zone_id).unwrap().filesystem_pool =
            DiffValue::Change(change);
    }

    fn visit_zone_zone_type_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, BlueprintZoneType>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err = "Missing sled id in ctx for visit_zone_zone_type_change"
                .to_string();
            self.acc.errors.push(err);
            return;
        };

        let Some(zone_id) = ctx.zone_id else {
            let err = "Missing zone id in ctx for visit_zone_zone_type_change"
                .to_string();
            self.acc.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_zone_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.acc.modified_sleds.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_zone_change` callback fired and
        // created the `ModifiedZone` entry.
        s.zones_modified.get_mut(&zone_id).unwrap().zone_type =
            DiffValue::Change(change);
    }
}
impl<'e> VisitBlueprintPhysicalDisksConfig<'e> for BlueprintDiffer<'e> {
    fn visit_generation_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, Generation>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for disks visit_generation_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };
        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.disks_generation = DiffValue::Change(change);
    }

    fn visit_disks_insert(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintPhysicalDiskConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_disks_insert".to_string();
            self.acc.errors.push(err);
            return;
        };
        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.disks_inserted.insert(node.clone());
    }

    fn visit_disks_remove(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintPhysicalDiskConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_disks_remove".to_string();
            self.acc.errors.push(err);
            return;
        };
        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.disks_removed.insert(node.clone());

        // Remove this zone from the unchanged disks to compensate for
        // constructor initialization.
        s.disks_unchanged.remove(&node.id);
    }

    fn visit_disk_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, BlueprintPhysicalDiskConfig>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_disk_change".to_string();
            self.acc.errors.push(err);
            return;
        };

        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.disks_modified
            .insert(change.before.id, ModifiedDisk::new(&change.before));

        // At least one of the fields for this zone is going to change in a
        // follow up callback, so we want to remove it from the unchanged disks
        // to compensate for constructor initialization.
        s.disks_unchanged.remove(&change.before.id);
    }

    fn visit_disk_disposition_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, BlueprintPhysicalDiskDisposition>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_disk_disposition_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        let Some(disk_id) = ctx.disk_id else {
            let err =
                "Missing disk id in ctx for visit_disk_disposition_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_disk_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.acc.modified_sleds.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_disk_change` callback fired and
        // created the `ModifiedZone` entry.
        s.disks_modified.get_mut(&disk_id).unwrap().disposition =
            DiffValue::Change(change);
    }
}
impl<'e> VisitBlueprintDatasetsConfig<'e> for BlueprintDiffer<'e> {}

/// Create a `BpTable` from a `BlueprintPhysicalDisksConfig`.
fn disks_table(
    state: BpDiffState,
    disks: Option<&BlueprintPhysicalDisksConfig>,
) -> Option<BpTable> {
    disks.map(|disks_config| {
        let rows = disks_config
            .disks
            .iter()
            .map(|d| {
                BpTableRow::from_strings(
                    state,
                    vec![
                        d.identity.vendor.clone(),
                        d.identity.model.clone(),
                        d.identity.serial.clone(),
                    ],
                )
            })
            .collect();
        BpTable::new(
            BpPhysicalDisksTableSchema {},
            disks_config.generation.into(),
            rows,
        )
    })
}

/// Create a `BpTable` from a `BlueprintDatasetsConfig`
fn datasets_table(
    state: BpDiffState,
    datasets: Option<&BlueprintDatasetsConfig>,
) -> Option<BpTable> {
    datasets.map(|datasets_config| {
        // `self.datasets` is naturally ordered by ID, but that doesn't play
        // well with expectorate-based tests: We end up sorted by (random)
        // UUIDs. We redact the UUIDs, but that still results in test-to-test
        // variance in the _order_ of the rows. We can work around this for now
        // by sorting by dataset kind: after UUID redaction, that produces
        // a stable table ordering for datasets.
        let mut rows = datasets_config.datasets.iter().collect::<Vec<_>>();
        rows.sort_unstable_by_key(|d| (&d.kind, &d.pool));
        let rows = rows
            .into_iter()
            .map(move |dataset| {
                BpTableRow::from_strings(state, dataset.as_strings())
            })
            .collect();
        BpTable::new(
            BpDatasetsTableSchema {},
            datasets_config.generation.into(),
            rows,
        )
    })
}

/// Create a `BpTable` from a `BlueprintZonesConfig`
fn zones_table(
    state: BpDiffState,
    zones: Option<&BlueprintZonesConfig>,
) -> Option<BpTable> {
    zones.map(|zones_config| {
        let rows = zones_config
            .zones
            .iter()
            .map(|zone| {
                BpTableRow::from_strings(
                    state,
                    vec![
                        zone.kind().report_str().to_string(),
                        zone.id().to_string(),
                        zone.disposition.to_string(),
                        zone.underlay_ip().to_string(),
                    ],
                )
            })
            .collect();
        BpTable::new(
            BpOmicronZonesTableSchema {},
            zones_config.generation.into(),
            rows,
        )
    })
}
