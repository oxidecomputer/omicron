// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A visitor for a `Blueprint`

use super::{
    visit_blueprint_datasets_config::VisitBlueprintDatasetsConfig,
    visit_blueprint_physical_disks_config::VisitBlueprintPhysicalDisksConfig,
    visit_blueprint_zones_config::VisitBlueprintZonesConfig, BpVisitorContext,
    BpVisitorError, Change,
};
use crate::{
    deployment::{
        Blueprint, BlueprintDatasetsConfig, BlueprintPhysicalDisksConfig,
        BlueprintZonesConfig, ClickhouseClusterConfig,
        CockroachDbPreserveDowngrade, SledUuid,
    },
    external_api::views::SledState,
};
use diffus::edit::{map, Edit};
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::BlueprintUuid;
use std::collections::{BTreeMap, BTreeSet};

/// State and Resources for an inserted sled
#[derive(Debug, Clone, Copy)]
pub struct SledInsert<'e> {
    pub sled_state: SledState,
    pub zones: Option<&'e BlueprintZonesConfig>,
    pub disks: Option<&'e BlueprintPhysicalDisksConfig>,
    pub datasets: Option<&'e BlueprintDatasetsConfig>,
}

impl<'e> SledInsert<'e> {
    pub fn new(sled_state: SledState) -> SledInsert<'e> {
        SledInsert { sled_state, zones: None, disks: None, datasets: None }
    }
}

/// State and Resources for a removed sled
#[derive(Debug, Clone, Copy)]
pub struct SledRemove<'e> {
    pub sled_state: SledState,
    pub zones: Option<&'e BlueprintZonesConfig>,
    pub disks: Option<&'e BlueprintPhysicalDisksConfig>,
    pub datasets: Option<&'e BlueprintDatasetsConfig>,
}

impl<'e> SledRemove<'e> {
    pub fn new(sled_state: SledState) -> SledRemove<'e> {
        SledRemove { sled_state, zones: None, disks: None, datasets: None }
    }
}

/// Allow default implementations for types.
impl<'e> VisitBlueprintDatasetsConfig<'e> for () {}
impl<'e> VisitBlueprintPhysicalDisksConfig<'e> for () {}
impl<'e> VisitBlueprintZonesConfig<'e> for () {}

/// A trait to visit a [`Blueprint`]
pub trait VisitBlueprint<'e> {
    fn zones_visitor(
        &mut self,
    ) -> Option<&mut impl VisitBlueprintZonesConfig<'e>> {
        Option::<&mut ()>::None
    }
    fn disks_visitor(
        &mut self,
    ) -> Option<&mut impl VisitBlueprintPhysicalDisksConfig<'e>> {
        Option::<&mut ()>::None
    }
    fn datasets_visitor(
        &mut self,
    ) -> Option<&mut impl VisitBlueprintDatasetsConfig<'e>> {
        Option::<&mut ()>::None
    }

    fn visit_blueprint(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: Edit<'e, Blueprint>,
    ) {
        visit_blueprint(self, ctx, node);
    }

    fn visit_sled_inserts(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: BTreeMap<SledUuid, SledInsert<'e>>,
    ) {
        visit_sled_inserts(self, ctx, node);
    }

    fn visit_sled_removes(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: BTreeMap<SledUuid, SledRemove<'e>>,
    ) {
        visit_sled_removes(self, ctx, node);
    }

    /// A sled has been inserted
    fn visit_sled_insert(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _val: SledInsert<'e>,
    ) {
        // Leaf node
    }

    /// A sled has been removed
    fn visit_sled_remove(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _val: SledRemove<'e>,
    ) {
        // Leaf node
    }

    // A sled's state has been changed
    fn visit_sled_state_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, SledState>,
    ) {
        // Leaf node
    }

    fn visit_parent_blueprint_id_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, Option<BlueprintUuid>>,
    ) {
        // Leaf node
    }

    fn visit_internal_dns_version_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, Generation>,
    ) {
        // Leaf node
    }

    fn visit_external_dns_version_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, Generation>,
    ) {
        // Leaf node
    }

    fn visit_cockroachdb_fingerprint_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, String>,
    ) {
        // Leaf node
    }

    fn visit_cockroachdb_setting_preserve_downgrade_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, CockroachDbPreserveDowngrade>,
    ) {
        // Leaf node
    }

    fn visit_clickhouse_cluster_config_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, Option<ClickhouseClusterConfig>>,
    ) {
        // Leaf node (for now)
        //
        // TODO: This should call a free function that uses a visitor for
        // `ClickhouseClusterConfig`. First we need to implmement that visitor
        // though.
    }

    fn visit_creator_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, String>,
    ) {
        // Leaf node
    }

    fn visit_comment_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, String>,
    ) {
        // Leaf node
    }
}

pub fn visit_blueprint<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: Edit<'e, Blueprint>,
) where
    V: VisitBlueprint<'e> + ?Sized,
{
    let Edit::Change { before, after, diff } = node else {
        return;
    };

    // Due to the fact that we have 4 different maps keyed by sled_id, we can
    // only determine additions and removals easily by using the `before` and
    // `after` blueprints explicitly. This will be unnecessary once these maps
    // are collapsed.
    //
    // See https://github.com/oxidecomputer/omicron/issues/7078
    let before_sleds: BTreeSet<_> = before
        .sled_state
        .keys()
        .chain(before.blueprint_zones.keys())
        .chain(before.blueprint_disks.keys())
        .chain(before.blueprint_datasets.keys())
        .collect();
    let after_sleds: BTreeSet<_> = after
        .sled_state
        .keys()
        .chain(after.blueprint_zones.keys())
        .chain(after.blueprint_disks.keys())
        .chain(after.blueprint_datasets.keys())
        .collect();
    let all_sleds: BTreeSet<_> =
        before_sleds.union(&after_sleds).map(|&sled_id| *sled_id).collect();

    // All sleds that have state, zones, disks or datasets in `after_*`, but not
    // `before_*` have been added.
    let sled_ids_added: BTreeSet<_> = after_sleds
        .difference(&before_sleds)
        .map(|&sled_id| *sled_id)
        .collect();

    // All sleds that have state, zones, disks or datasets in `before_*`, but not
    // `after_*` have been removed.
    let sled_ids_removed: BTreeSet<_> = before_sleds
        .difference(&after_sleds)
        .map(|&sled_id| *sled_id)
        .collect();

    let mut sled_inserts = BTreeMap::new();
    let mut sled_removes = BTreeMap::new();

    // Build up the set of all edits for a given sled
    // This is going to be much easier once maps are collapsed!!!
    if let Edit::Change { diff, .. } = &diff.sled_state {
        for (&sled_id, edit) in diff {
            ctx.sled_id = Some(*sled_id);
            match edit {
                map::Edit::Insert(&sled_state) => {
                    sled_inserts.insert(*sled_id, SledInsert::new(sled_state));
                }
                map::Edit::Remove(&sled_state) => {
                    // Work around a quirk of sled decommissioning. If a sled
                    // has a before state of `decommissioned`, it may or may not
                    // be present in `after` (presence will depend on whether
                    // or not the sled was present in the `PlanningInput`).
                    // However, we may still have entries in `zones`, `disks`,
                    // or `datasets`  that haven't been fully cleaned up yet.
                    if sled_ids_removed.contains(sled_id) {
                        sled_removes
                            .insert(*sled_id, SledRemove::new(sled_state));
                    }
                }
                map::Edit::Change { before, after, .. } => {
                    v.visit_sled_state_change(ctx, Change::new(before, after));
                }
                map::Edit::Copy(_) => {}
            }
        }
        ctx.sled_id = None;
    }
    if let Edit::Change { diff, .. } = &diff.blueprint_zones {
        for (&sled_id, edit) in diff {
            ctx.sled_id = Some(*sled_id);
            match edit {
                map::Edit::Insert(zones) => {
                    sled_inserts
                        .entry(*sled_id)
                        .and_modify(|e| e.zones = Some(zones))
                        .or_insert_with(|| {
                            // This is a *bug*. We don't have a valid `sled_state`
                            // insert. Once we collapse the maps this will no longer
                            // be an issue, but for now we insert what the sled state
                            // should be for a newly inserted sled.
                            ctx.errors.push(
                                BpVisitorError::MissingSledStateOnZonesInsert {
                                    sled_id: *sled_id,
                                },
                            );
                            let mut insert = SledInsert::new(SledState::Active);
                            insert.zones = Some(zones);
                            insert
                        });
                }
                map::Edit::Remove(zones) => {
                    if sled_ids_removed.contains(sled_id) {
                        sled_removes
                            .entry(*sled_id)
                            .and_modify(|e| e.zones = Some(*zones))
                            .or_insert_with(|| {
                                // This is a a workaround, where in some cases we have removed
                                // the sled-state from the blueprint before any other sled resources.
                                //
                                // We backfill the sled state so we can create a proper
                                // `SledRemove` entry.
                                let mut remove =
                                    SledRemove::new(SledState::Decommissioned);
                                remove.zones = Some(zones);
                                remove
                            });
                    }
                }
                map::Edit::Change { diff, .. } => {
                    if let Some(v) = v.zones_visitor() {
                        v.visit_zones_edit(ctx, diff);
                    }
                }
                map::Edit::Copy(_) => {}
            }
        }
        ctx.sled_id = None;
    }
    if let Edit::Change { diff, .. } = &diff.blueprint_disks {
        for (&sled_id, edit) in diff {
            ctx.sled_id = Some(*sled_id);
            match edit {
                map::Edit::Insert(disks) => {
                    sled_inserts
                        .entry(*sled_id)
                        .and_modify(|e| e.disks = Some(disks))
                        .or_insert_with(|| {
                            // This is a *bug*. We don't have a valid `sled_state`
                            // insert. Once we collapse the maps this will no longer
                            // be an issue, but for now we insert what the sled state
                            // should be for a newly inserted sled.
                            ctx.errors.push(
                                BpVisitorError::MissingSledStateOnDisksInsert {
                                    sled_id: *sled_id,
                                },
                            );
                            let mut insert = SledInsert::new(SledState::Active);
                            insert.disks = Some(disks);
                            insert
                        });
                }
                map::Edit::Remove(disks) => {
                    if !sled_ids_removed.contains(sled_id) {
                        // Backwards compatibility. We shouldn't be removing
                        // datasets until we remove the sled. They should just be
                        // marked expunged.
                        if let Some(v) = v.disks_visitor() {
                            for disk in &disks.disks {
                                v.visit_disks_remove(ctx, disk);
                            }
                        }
                    } else {
                        sled_removes
                            .entry(*sled_id)
                            .and_modify(|e| e.disks = Some(*disks))
                            .or_insert_with(|| {
                                // This is a a workaround, where in some cases we have removed
                                // the sled-state from the blueprint before any other sled resources.
                                //
                                // We backfill the sled state so we can create a proper
                                // `SledRemove` entry.
                                let mut remove =
                                    SledRemove::new(SledState::Decommissioned);
                                remove.disks = Some(disks);
                                remove
                            });
                    }
                }
                map::Edit::Change { diff, .. } => {
                    if let Some(v) = v.disks_visitor() {
                        v.visit_disks_edit(ctx, diff);
                    }
                }
                map::Edit::Copy(_) => {}
            }
        }
        ctx.sled_id = None;
    }
    if let Edit::Change { diff, .. } = &diff.blueprint_datasets {
        for (&sled_id, edit) in diff {
            ctx.sled_id = Some(*sled_id);
            match edit {
                map::Edit::Insert(datasets) => {
                    sled_inserts
                        .entry(*sled_id)
                        .and_modify(|e| e.datasets = Some(datasets))
                        .or_insert_with(|| {
                            // This is a *bug*. We don't have a valid `sled_state`
                            // insert. Once we collapse the maps this will no longer
                            // be an issue, but for now we insert what the sled state
                            // should be for a newly inserted sled.
                            ctx.errors.push(
                                BpVisitorError::MissingSledStateOnDatasetsInsert {
                                    sled_id: *sled_id,
                                },
                            );
                            let mut insert = SledInsert::new(SledState::Active);
                            insert.datasets = Some(datasets);
                            insert
                        });
                }
                map::Edit::Remove(datasets) => {
                    // Backwards compatibility: We shouldn't be removing
                    // datasets until we remove the sled. They should just be
                    // marked expunged.
                    if !sled_ids_removed.contains(sled_id) {
                        if let Some(v) = v.datasets_visitor() {
                            for dataset in &datasets.datasets {
                                v.visit_datasets_remove(ctx, dataset);
                            }
                        }
                    } else {
                        sled_removes
                            .entry(*sled_id)
                            .and_modify(|e| e.datasets = Some(*datasets))
                            .or_insert_with(|| {
                                // This is a a workaround, where in some cases we have removed
                                // the sled-state from the blueprint before any other sled resources.
                                //
                                // We backfill the sled state so we can create a proper
                                // `SledRemove` entry.
                                let mut remove =
                                    SledRemove::new(SledState::Decommissioned);
                                remove.datasets = Some(datasets);
                                remove
                            });
                    }
                }
                map::Edit::Change { diff, .. } => {
                    if let Some(v) = v.datasets_visitor() {
                        v.visit_datasets_edit(ctx, diff);
                    }
                }
                map::Edit::Copy(_) => {}
            }
        }
        ctx.sled_id = None;
    }

    v.visit_sled_inserts(ctx, sled_inserts);
    v.visit_sled_removes(ctx, sled_removes);

    if let Edit::Change { before, after, .. } = diff.parent_blueprint_id {
        v.visit_parent_blueprint_id_change(ctx, Change::new(before, after));
    }
    if let Edit::Change { before, after, .. } = diff.internal_dns_version {
        v.visit_internal_dns_version_change(ctx, Change::new(before, after));
    }
    if let Edit::Change { before, after, .. } = diff.external_dns_version {
        v.visit_external_dns_version_change(ctx, Change::new(before, after));
    }
    if let Edit::Change { before, after, .. } = diff.cockroachdb_fingerprint {
        v.visit_cockroachdb_fingerprint_change(ctx, Change::new(before, after));
    }
    if let Edit::Change { before, after, .. } =
        diff.cockroachdb_setting_preserve_downgrade
    {
        v.visit_cockroachdb_setting_preserve_downgrade_change(
            ctx,
            Change::new(before, after),
        );
    }
    if let Edit::Change { before, after, .. } = diff.clickhouse_cluster_config {
        // TODO: We need a separate visitor for this config
        v.visit_clickhouse_cluster_config_change(
            ctx,
            Change::new(before, after),
        );
    }
    if let Edit::Change { before, after, .. } = diff.creator {
        v.visit_creator_change(ctx, Change::new(before, after));
    }
    if let Edit::Change { before, after, .. } = diff.comment {
        v.visit_comment_change(ctx, Change::new(before, after));
    }
}

pub fn visit_sled_inserts<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: BTreeMap<SledUuid, SledInsert<'e>>,
) where
    V: VisitBlueprint<'e> + ?Sized,
{
    for (sled_id, insert) in &node {
        ctx.sled_id = Some(*sled_id);
        v.visit_sled_insert(ctx, *insert);
    }
    ctx.sled_id = None;
}

pub fn visit_sled_removes<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: BTreeMap<SledUuid, SledRemove<'e>>,
) where
    V: VisitBlueprint<'e> + ?Sized,
{
    for (sled_id, remove) in &node {
        ctx.sled_id = Some(*sled_id);
        v.visit_sled_remove(ctx, *remove);
    }
    ctx.sled_id = None;
}
