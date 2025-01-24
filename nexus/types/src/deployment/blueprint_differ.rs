// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An implementation of a `VisitBlueprint` visitor that is used to construct
//! a `BlueprintDiff`.

use super::blueprint_diff::{
    BlueprintDiff, DiffValue, ModifiedDataset, ModifiedDisk, ModifiedSled,
    ModifiedZone,
};
use super::diff_visitors::visit_blueprint::{
    SledInsert, SledRemove, VisitBlueprint,
};
use super::diff_visitors::visit_blueprint_datasets_config::VisitBlueprintDatasetsConfig;
use super::diff_visitors::visit_blueprint_physical_disks_config::VisitBlueprintPhysicalDisksConfig;
use super::diff_visitors::visit_blueprint_zones_config::VisitBlueprintZonesConfig;
pub use super::diff_visitors::{BpVisitorContext, Change};
use super::{
    Blueprint, BlueprintDatasetDisposition, BlueprintPhysicalDiskDisposition,
    ClickhouseClusterConfig, CockroachDbPreserveDowngrade,
};

use diffus::Diffable;
use omicron_common::api::external::{ByteCount, Generation};
use omicron_common::disk::CompressionAlgorithm;
use omicron_uuid_kinds::BlueprintUuid;
use std::collections::BTreeSet;

use crate::deployment::{
    BlueprintDatasetConfig, BlueprintPhysicalDiskConfig, BlueprintZoneConfig,
    BlueprintZoneDisposition, BlueprintZoneType, ZpoolName,
};
use crate::external_api::views::SledState;

/// A mechanism for creating a `BlueprintDiff` via a `VisitBlueprint`
/// implementation.
pub struct BlueprintDiffer<'e> {
    before: &'e Blueprint,
    after: &'e Blueprint,
    /// An accumulator for diff state while traversing a visitor
    diff: BlueprintDiff<'e>,
}

impl<'e> BlueprintDiffer<'e> {
    pub fn new(
        before: &'e Blueprint,
        after: &'e Blueprint,
    ) -> BlueprintDiffer<'e> {
        BlueprintDiffer {
            before,
            after,
            diff: BlueprintDiff::new(before, after),
        }
    }

    pub fn diff(mut self) -> BlueprintDiff<'e> {
        let mut ctx = BpVisitorContext::default();
        let diff = self.before.diff(&self.after);
        self.visit_blueprint(&mut ctx, diff);

        // Unchanged sleds are those that do not exist in added, removed, or
        // modified.
        let changed_sleds: BTreeSet<_> = self
            .diff
            .sleds_added
            .keys()
            .cloned()
            .chain(self.diff.sleds_removed.keys().cloned())
            .chain(self.diff.sleds_modified.keys().cloned())
            .collect();

        let before_sleds: BTreeSet<_> = self.before.sleds().collect();
        self.diff.sleds_unchanged =
            before_sleds.difference(&changed_sleds).cloned().collect();

        self.diff
    }
}

impl<'e> VisitBlueprint<'e> for BlueprintDiffer<'e> {
    fn zones_visitor(
        &mut self,
    ) -> Option<&mut impl VisitBlueprintZonesConfig<'e>> {
        Some(&mut *self)
    }

    fn disks_visitor(
        &mut self,
    ) -> Option<&mut impl VisitBlueprintPhysicalDisksConfig<'e>> {
        Some(&mut *self)
    }

    fn datasets_visitor(
        &mut self,
    ) -> Option<&mut impl VisitBlueprintDatasetsConfig<'e>> {
        Some(&mut *self)
    }

    fn visit_sled_insert(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: SledInsert<'e>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_sled_insert".to_string();
            self.diff.errors.push(err);
            return;
        };

        self.diff.sleds_added.insert(sled_id, node);
    }

    fn visit_sled_remove(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: SledRemove<'e>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_sled_remove".to_string();
            self.diff.errors.push(err);
            return;
        };

        self.diff.sleds_removed.insert(sled_id, node);
    }

    fn visit_sled_state_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, SledState>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err = "Missing sled id in ctx for visit_sled_state_change"
                .to_string();
            self.diff.errors.push(err);
            return;
        };
        let s = self
            .diff
            .sleds_modified
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.sled_state = DiffValue::Changed(change);
    }

    fn visit_parent_blueprint_id_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, Option<BlueprintUuid>>,
    ) {
        self.diff.metadata.parent_blueprint_id = DiffValue::Changed(change);
    }

    fn visit_internal_dns_version_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, Generation>,
    ) {
        self.diff.metadata.internal_dns_version = DiffValue::Changed(change);
    }

    fn visit_external_dns_version_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, Generation>,
    ) {
        self.diff.metadata.external_dns_version = DiffValue::Changed(change);
    }

    fn visit_cockroachdb_fingerprint_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, String>,
    ) {
        self.diff.metadata.cockroachdb_fingerprint = DiffValue::Changed(change);
    }

    fn visit_cockroachdb_setting_preserve_downgrade_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, CockroachDbPreserveDowngrade>,
    ) {
        self.diff.metadata.cockroachdb_setting_preserve_downgrade =
            DiffValue::Changed(change);
    }

    fn visit_creator_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, String>,
    ) {
        self.diff.metadata.creator = DiffValue::Changed(change);
    }

    fn visit_comment_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, String>,
    ) {
        self.diff.metadata.comment = DiffValue::Changed(change);
    }

    fn visit_clickhouse_cluster_config_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        change: Change<'e, Option<ClickhouseClusterConfig>>,
    ) {
        // TODO: Change this once we have a visitor for `ClickhouseClusterconfig`
        self.diff.clickhouse_cluster_config = DiffValue::Changed(change);
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
            self.diff.errors.push(err);
            return;
        };
        let s = self
            .diff
            .sleds_modified
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.zones_generation = DiffValue::Changed(change);
    }

    fn visit_zones_insert(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintZoneConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_zones_insert".to_string();
            self.diff.errors.push(err);
            return;
        };
        let s = self
            .diff
            .sleds_modified
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.zones_added.insert(node.clone());
    }

    fn visit_zones_remove(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintZoneConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_zones_remove".to_string();
            self.diff.errors.push(err);
            return;
        };
        let s = self
            .diff
            .sleds_modified
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
            self.diff.errors.push(err);
            return;
        };

        let s = self
            .diff
            .sleds_modified
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
            self.diff.errors.push(err);
            return;
        };

        let Some(zone_id) = ctx.zone_id else {
            let err =
                "Missing zone id in ctx for visit_zone_disposition_change"
                    .to_string();
            self.diff.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_zone_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.diff.sleds_modified.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_zone_change` callback fired and
        // created the `ModifiedZone` entry.
        s.zones_modified.get_mut(&zone_id).unwrap().disposition =
            DiffValue::Changed(change);
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
            self.diff.errors.push(err);
            return;
        };

        let Some(zone_id) = ctx.zone_id else {
            let err =
                "Missing zone id in ctx for visit_zone_filesystem_pool_change"
                    .to_string();
            self.diff.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_zone_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.diff.sleds_modified.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_zone_change` callback fired and
        // created the `ModifiedZone` entry.
        s.zones_modified.get_mut(&zone_id).unwrap().filesystem_pool =
            DiffValue::Changed(change);
    }

    fn visit_zone_zone_type_change(
        &mut self,
        _: &mut BpVisitorContext,
        change: Change<'e, BlueprintZoneType>,
    ) {
        self.diff.errors.push(format!(
            "Zone type not allowed to change. before: {:?}, after: {:?}",
            change.before, change.after
        ));
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
            self.diff.errors.push(err);
            return;
        };
        let s = self
            .diff
            .sleds_modified
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.disks_generation = DiffValue::Changed(change);
    }

    fn visit_disks_insert(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintPhysicalDiskConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_disks_insert".to_string();
            self.diff.errors.push(err);
            return;
        };
        let s = self
            .diff
            .sleds_modified
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.disks_added.insert(node.clone());
    }

    fn visit_disks_remove(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintPhysicalDiskConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_disks_remove".to_string();
            self.diff.errors.push(err);
            return;
        };
        let s = self
            .diff
            .sleds_modified
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
            self.diff.errors.push(err);
            return;
        };

        let s = self
            .diff
            .sleds_modified
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
            self.diff.errors.push(err);
            return;
        };

        let Some(disk_id) = ctx.disk_id else {
            let err =
                "Missing disk id in ctx for visit_disk_disposition_change"
                    .to_string();
            self.diff.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_disk_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.diff.sleds_modified.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_disk_change` callback fired and
        // created the `ModifiedZone` entry.
        s.disks_modified.get_mut(&disk_id).unwrap().disposition =
            DiffValue::Changed(change);
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
            self.diff.errors.push(err);
            return;
        };
        let s = self
            .diff
            .sleds_modified
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.datasets_generation = DiffValue::Changed(change);
    }

    fn visit_datasets_insert(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintDatasetConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_datasets_insert".to_string();
            self.diff.errors.push(err);
            return;
        };
        let s = self
            .diff
            .sleds_modified
            .entry(sled_id)
            .or_insert(ModifiedSled::new(&self.before, sled_id));
        s.datasets_added.insert(node.clone());
    }

    fn visit_datasets_remove(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintDatasetConfig,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err =
                "Missing sled id in ctx for visit_datasets_remove".to_string();
            self.diff.errors.push(err);
            return;
        };
        let s = self
            .diff
            .sleds_modified
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
            self.diff.errors.push(err);
            return;
        };

        let s = self
            .diff
            .sleds_modified
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
            self.diff.errors.push(err);
            return;
        };

        let Some(dataset_id) = ctx.dataset_id else {
            let err =
                "Missing dataset id in ctx for visit_dataset_disposition_change"
                    .to_string();
            self.diff.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.diff.sleds_modified.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedDataset` entry.
        s.datasets_modified.get_mut(&dataset_id).unwrap().disposition =
            DiffValue::Changed(change);
    }

    fn visit_dataset_quota_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, Option<ByteCount>>,
    ) {
        let Some(sled_id) = ctx.sled_id else {
            let err = "Missing sled id in ctx for visit_dataset_quota_change"
                .to_string();
            self.diff.errors.push(err);
            return;
        };

        let Some(dataset_id) = ctx.dataset_id else {
            let err =
                "Missing dataset id in ctx for visit_dataset_quota_change"
                    .to_string();
            self.diff.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.diff.sleds_modified.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedDataset` entry.
        s.datasets_modified.get_mut(&dataset_id).unwrap().quota =
            DiffValue::Changed(change);
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
            self.diff.errors.push(err);
            return;
        };

        let Some(dataset_id) = ctx.dataset_id else {
            let err =
                "Missing dataset id in ctx for visit_dataset_reservation_change"
                    .to_string();
            self.diff.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.diff.sleds_modified.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedDataset` entry.
        s.datasets_modified.get_mut(&dataset_id).unwrap().reservation =
            DiffValue::Changed(change);
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
            self.diff.errors.push(err);
            return;
        };

        let Some(dataset_id) = ctx.dataset_id else {
            let err =
                "Missing dataset id in ctx for visit_dataset_compression_change"
                    .to_string();
            self.diff.errors.push(err);
            return;
        };

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedSled` entry if it didn't exist.
        let s = self.diff.sleds_modified.get_mut(&sled_id).unwrap();

        // Safety: We guarantee a `visit_dataset_change` callback fired and
        // created the `ModifiedDataset` entry.
        s.datasets_modified.get_mut(&dataset_id).unwrap().compression =
            DiffValue::Changed(change);
    }
}
