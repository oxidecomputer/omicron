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
use omicron_uuid_kinds::{BlueprintUuid, DatasetUuid, SledUuid};
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

pub struct ModifiedDisk<'e> {
    disposition: DiffValue<'e, BlueprintPhysicalDiskDisposition>,
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

pub struct ModifiedDataset<'e> {
    disposition: DiffValue<'e, BlueprintDatasetDisposition>,
    quota: DiffValue<'e, Option<ByteCount>>,
    reservation: DiffValue<'e, Option<ByteCount>>,
    compression: DiffValue<'e, CompressionAlgorithm>,
}

impl<'e> ModifiedDataset<'e> {
    /// Initialize a `ModifiedDataset`.
    ///
    /// We always initialize to the `before` state as if this value is
    /// unchanged. If a change callback fires for a given field, then we'll
    /// update the value.
    pub fn new(before: &'e BlueprintDatasetConfig) -> ModifiedDataset<'e> {
        ModifiedDataset {
            disposition: DiffValue::Unchanged(&before.disposition),
            quota: DiffValue::Unchanged(&before.quota),
            reservation: DiffValue::Unchanged(&before.reservation),
            compression: DiffValue::Unchanged(&before.compression),
        }
    }
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
    datasets_modified: BTreeMap<DatasetUuid, ModifiedDataset<'e>>,
}

impl<'e> From<&ModifiedSled<'e>> for SledTables {
    fn from(value: &ModifiedSled<'e>) -> Self {
        todo!()
    }
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

/// All possible modifications to `BlueprintMetadata`
pub struct MetadataDiff<'e> {
    blueprint_id: DiffValue<'e, BlueprintUuid>,
    parent_blueprint_id: DiffValue<'e, Option<BlueprintUuid>>,
    internal_dns_version: DiffValue<'e, Generation>,
    external_dns_version: DiffValue<'e, Generation>,
    cockroachdb_fingerprint: DiffValue<'e, String>,
    cockroachdb_setting_preserve_downgrade:
        DiffValue<'e, CockroachDbPreserveDowngrade>,
    creator: DiffValue<'e, String>,
    comment: DiffValue<'e, String>,
}

impl<'e> MetadataDiff<'e> {
    /// Initialize a `MetadataDiff`.
    ///
    /// We always initialize to the `before` state as if this value is
    /// unchanged. If a change callback fires for a given field, then we'll
    /// update the value.
    pub fn new(
        before: &'e Blueprint,
        after: &'e Blueprint,
    ) -> MetadataDiff<'e> {
        // We don't get a callback for `id`, so just fill it in here.
        let blueprint_id = if before.id == after.id {
            DiffValue::Unchanged(&before.id)
        } else {
            DiffValue::Change(Change { before: &before.id, after: &after.id })
        };
        MetadataDiff {
            blueprint_id,
            parent_blueprint_id: DiffValue::Unchanged(
                &before.parent_blueprint_id,
            ),
            internal_dns_version: DiffValue::Unchanged(
                &before.internal_dns_version,
            ),
            external_dns_version: DiffValue::Unchanged(
                &before.external_dns_version,
            ),
            cockroachdb_fingerprint: DiffValue::Unchanged(
                &before.cockroachdb_fingerprint,
            ),
            cockroachdb_setting_preserve_downgrade: DiffValue::Unchanged(
                &before.cockroachdb_setting_preserve_downgrade,
            ),
            creator: DiffValue::Unchanged(&before.creator),
            comment: DiffValue::Unchanged(&before.comment),
        }
    }
}

/// Accumulated state about diffs inside a `BlueprintDiffer`.
///
/// Tied to the lifetime of a diffus diff.
pub struct BpDiffAccumulator<'e> {
    before: &'e Blueprint,
    after: &'e Blueprint,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub added_sleds: BTreeMap<SledUuid, SledInsert<'e>>,
    pub removed_sleds: BTreeMap<SledUuid, SledRemove<'e>>,
    pub unchanged_sleds: BTreeSet<SledUuid>,
    pub modified_sleds: BTreeMap<SledUuid, ModifiedSled<'e>>,
    pub metadata: MetadataDiff<'e>,

    // TODO: Change once we have a visitor for `ClickhouseClusterConfig`
    pub clickhouse_cluster_config:
        DiffValue<'e, Option<ClickhouseClusterConfig>>,
}

impl<'e> BpDiffAccumulator<'e> {
    pub fn new(
        before: &'e Blueprint,
        after: &'e Blueprint,
    ) -> BpDiffAccumulator<'e> {
        BpDiffAccumulator {
            before,
            after,
            errors: vec![],
            warnings: vec![],
            added_sleds: BTreeMap::new(),
            removed_sleds: BTreeMap::new(),
            unchanged_sleds: BTreeSet::new(),
            modified_sleds: BTreeMap::new(),
            metadata: MetadataDiff::new(before, after),
            clickhouse_cluster_config: DiffValue::Unchanged(
                &before.clickhouse_cluster_config,
            ),
        }
    }
}

/// A mechanism for creating tables from blueprint diffs via a `VisitBlueprint` implementation.
pub struct BlueprintDiffer<'e> {
    before: &'e Blueprint,
    after: &'e Blueprint,
    /// An accumulator for diff state while traversing a visitor
    acc: BpDiffAccumulator<'e>,
}

impl<'e> BlueprintDiffer<'e> {
    pub fn new(
        before: &'e Blueprint,
        after: &'e Blueprint,
    ) -> BlueprintDiffer<'e> {
        BlueprintDiffer {
            before,
            after,
            acc: BpDiffAccumulator::new(before, after),
        }
    }

    pub fn diff(mut self) -> BpDiffAccumulator<'e> {
        let mut ctx = BpVisitorContext::default();
        let diff = self.before.diff(&self.after);
        self.visit_blueprint(&mut ctx, diff);

        // Unchanged sleds are those that do not exist in added, removed, or
        // modified.
        let changed_sleds: BTreeSet<_> = self
            .acc
            .added_sleds
            .keys()
            .cloned()
            .chain(self.acc.removed_sleds.keys().cloned())
            .chain(self.acc.modified_sleds.keys().cloned())
            .collect();

        let before_sleds: BTreeSet<_> = self.before.sleds().collect();
        self.acc.unchanged_sleds =
            before_sleds.difference(&changed_sleds).cloned().collect();

        self.acc
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

    fn visit_parent_blueprint_id_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, Option<BlueprintUuid>>,
    ) {
        self.acc.metadata.parent_blueprint_id = DiffValue::Change(change);
    }

    fn visit_internal_dns_version_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, Generation>,
    ) {
        self.acc.metadata.internal_dns_version = DiffValue::Change(change);
    }

    fn visit_external_dns_version_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, Generation>,
    ) {
        self.acc.metadata.external_dns_version = DiffValue::Change(change);
    }

    fn visit_cockroachdb_fingerprint_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, String>,
    ) {
        self.acc.metadata.cockroachdb_fingerprint = DiffValue::Change(change);
    }

    fn visit_cockroachdb_setting_preserve_downgrade_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, CockroachDbPreserveDowngrade>,
    ) {
        self.acc.metadata.cockroachdb_setting_preserve_downgrade =
            DiffValue::Change(change);
    }

    fn visit_creator_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, String>,
    ) {
        self.acc.metadata.creator = DiffValue::Change(change);
    }

    fn visit_comment_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, String>,
    ) {
        self.acc.metadata.comment = DiffValue::Change(change);
    }

    fn visit_clickhouse_cluster_config_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, Option<ClickhouseClusterConfig>>,
    ) {
        // TODO: Change this once we have a visitor for `ClickhouseClusterconfig`
        self.acc.clickhouse_cluster_config = DiffValue::Change(change);
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
impl<'e> VisitBlueprintDatasetsConfig<'e> for BlueprintDiffer<'e> {
    fn visit_generation_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, Generation>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for datasets visit_generation_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };
        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.datasets_generation = DiffValue::Change(change);
    }

    fn visit_datasets_insert(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintDatasetConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_datasets_insert".to_string();
            self.acc.errors.push(err);
            return;
        };
        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.datasets_inserted.insert(node.clone());
    }

    fn visit_datasets_remove(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintDatasetConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_datasets_remove".to_string();
            self.acc.errors.push(err);
            return;
        };
        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.datasets_removed.insert(node.clone());

        // Remove this zone from the unchanged datasets to compensate
        // for constructor initialization.
        s.datasets_unchanged.remove(&node.id);
    }

    fn visit_dataset_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, BlueprintDatasetConfig>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_dataset_change".to_string();
            self.acc.errors.push(err);
            return;
        };

        let s = self
            .acc
            .modified_sleds
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.datasets_modified
            .insert(change.before.id, ModifiedDataset::new(&change.before));

        // At least one of the fields for this dataset is going to change in a
        // follow up callback, so we want to remove it from the unchanged datasets
        // to compensate for constructor initialization.
        s.datasets_unchanged.remove(&change.before.id);
    }

    fn visit_dataset_disposition_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, BlueprintDatasetDisposition>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_dataset_disposition_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        let Some(dataset_id) = ctx.dataset_id else {
            let err =
                "Missing dataset id in ctx for visit_dataset_disposition_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.acc.modified_sleds.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedDataset` entry.
        s.datasets_modified.get_mut(&dataset_id).unwrap().disposition =
            DiffValue::Change(change);
    }

    fn visit_dataset_quota_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, Option<ByteCount>>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err = "Missing sled id in ctx for visit_dataset_quota_change"
                .to_string();
            self.acc.errors.push(err);
            return;
        };

        let Some(dataset_id) = ctx.dataset_id else {
            let err =
                "Missing dataset id in ctx for visit_dataset_quota_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.acc.modified_sleds.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedDataset` entry.
        s.datasets_modified.get_mut(&dataset_id).unwrap().quota =
            DiffValue::Change(change);
    }

    fn visit_dataset_reservation_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, Option<ByteCount>>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_dataset_reservation_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        let Some(dataset_id) = ctx.dataset_id else {
            let err =
                "Missing dataset id in ctx for visit_dataset_reservation_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.acc.modified_sleds.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedDataset` entry.
        s.datasets_modified.get_mut(&dataset_id).unwrap().reservation =
            DiffValue::Change(change);
    }

    fn visit_dataset_compression_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, CompressionAlgorithm>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_dataset_compression_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        let Some(dataset_id) = ctx.dataset_id else {
            let err =
                "Missing dataset id in ctx for visit_dataset_compression_change"
                    .to_string();
            self.acc.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.acc.modified_sleds.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedDataset` entry.
        s.datasets_modified.get_mut(&dataset_id).unwrap().compression =
            DiffValue::Change(change);
    }
}

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
    pub unchanged_sleds: BTreeMap<SledUuid, SledTables>,
    pub modified_sleds: BTreeMap<SledUuid, SledTables>,
}

impl<'e> From<&BpDiffAccumulator<'e>> for BpDiffOutput {
    fn from(value: &BpDiffAccumulator<'e>) -> Self {
        BpDiffOutput {
            errors: value.errors.clone(),
            warnings: value.warnings.clone(),
            added_sleds: value
                .added_sleds
                .iter()
                .map(|(sled_id, insert)| {
                    (
                        *sled_id,
                        SledTables {
                            disks: disks_table(
                                BpDiffState::Added,
                                insert.disks,
                            ),
                            datasets: datasets_table(
                                BpDiffState::Added,
                                insert.datasets,
                            ),
                            zones: zones_table(
                                BpDiffState::Added,
                                insert.zones,
                            ),
                        },
                    )
                })
                .collect(),
            removed_sleds: value
                .removed_sleds
                .iter()
                .map(|(sled_id, remove)| {
                    (
                        *sled_id,
                        SledTables {
                            disks: disks_table(
                                BpDiffState::Removed,
                                remove.disks,
                            ),
                            datasets: datasets_table(
                                BpDiffState::Removed,
                                remove.datasets,
                            ),
                            zones: zones_table(
                                BpDiffState::Removed,
                                remove.zones,
                            ),
                        },
                    )
                })
                .collect(),
            unchanged_sleds: value
                .unchanged_sleds
                .iter()
                .map(|sled_id| {
                    (
                        *sled_id,
                        SledTables {
                            disks: disks_table(
                                BpDiffState::Unchanged,
                                value.before.blueprint_disks.get(sled_id),
                            ),
                            datasets: datasets_table(
                                BpDiffState::Unchanged,
                                value.before.blueprint_datasets.get(sled_id),
                            ),
                            zones: zones_table(
                                BpDiffState::Unchanged,
                                value.before.blueprint_zones.get(sled_id),
                            ),
                        },
                    )
                })
                .collect(),
            modified_sleds: value
                .modified_sleds
                .iter()
                .map(|(sled_id, modified)| (*sled_id, modified.into()))
                .collect(),
        }
    }
}

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
