// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A visitor for `BlueprintZonesConfig`

use super::{
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZoneType,
    BlueprintZonesConfig, BpVisitorContext, Change, EditedBlueprintZoneConfig,
};

use diffus::edit::{map, Edit};
use omicron_common::{api::external::Generation, zpool_name::ZpoolName};

/// A trait to visit a [`BlueprintZonesConfig`]
pub trait VisitBlueprintZonesConfig<'e> {
    fn visit_root(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: Edit<'e, BlueprintZonesConfig>,
    ) {
        visit_root(self, ctx, node);
    }

    /// A change to `BlueprintZonesConfig::generation`
    fn visit_generation_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, Generation>,
    ) {
        visit_generation_change(self, ctx, change);
    }

    /// An insert to `BlueprintZonesConfig::zones`
    fn visit_zones_insert(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintZoneConfig,
    ) {
        visit_zones_insert(self, ctx, node);
    }

    /// A removal from `BlueprintZonesConfig::zones`
    fn visit_zones_remove(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintZoneConfig,
    ) {
        visit_zones_remove(self, ctx, node);
    }

    // A change in a value in `BlueprintZonesConfig::zones`
    fn visit_zone_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, BlueprintZoneConfig>,
    ) {
        // TODO: This requires another change to diffus_derive
        //        visit_zone_change(self, ctx, change);
    }

    // The representation of a diffus generated `EditedBlueprintZoneConfig`
    // which contains recursive edits for each field when there is at least
    // one change.
    //
    // This the equivalent node in the tree to `visit_zone_change`, but gives
    // a diffus_derive generated structure rather than the before and after of the original structs
    // that were diffed.
    fn visit_zone_edit(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &EditedBlueprintZoneConfig<'e>,
    ) {
        visit_zone_edit(self, ctx, node);
    }

    /// A change in `BlueprintZoneConfig::disposition`
    fn visit_zone_disposition_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, BlueprintZoneDisposition>,
    ) {
        visit_zone_disposition_change(self, ctx, change);
    }

    /// A change in a `BlueprintZoneConfig::filesystem_pool`
    fn visit_zone_filesystem_pool_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, Option<ZpoolName>>,
    ) {
        visit_zone_filesystem_pool_change(self, ctx, change);
    }

    /// A change in a `BlueprintZoneConfig::zone_type`
    ///
    /// A `BlueprintZoneType` is a complicated structure. In order to keep the
    /// first version of this visitor tractable, we just return the a `Change`
    /// rather than a diffus `EditedBlueprintZoneType` that we have to walk.
    /// We'll likely want to add this to get full coverage, but it can come
    /// later, and should probably live in it's own visitor that we call from
    /// this point.
    fn visit_zone_zone_type_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: Change<'e, BlueprintZoneType>,
    ) {
        visit_zone_zone_type_change(self, ctx, node);
    }
}

/// The root of the diff for a `BlueprintZonesConfig`
pub fn visit_root<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: Edit<'e, BlueprintZonesConfig>,
) where
    V: VisitBlueprintZonesConfig<'e> + ?Sized,
{
    if let Edit::Change(bp_zones_config) = node {
        if let Edit::Change(diff) = bp_zones_config.generation {
            v.visit_generation_change(ctx, diff.into());
        }
        if let Edit::Change(diff) = bp_zones_config.zones {
            for (&zone_id, bp_zone_config_edit) in &diff {
                ctx.zone_id = Some(*zone_id);
                match bp_zone_config_edit {
                    map::Edit::Copy(_) => {}
                    map::Edit::Insert(bp_zone_config) => {
                        v.visit_zones_insert(ctx, bp_zone_config);
                    }
                    map::Edit::Remove(bp_zone_config) => {
                        v.visit_zones_remove(ctx, bp_zone_config);
                    }
                    map::Edit::Change(edited_bp_zone_config) => {
                        v.visit_zone_edit(ctx, edited_bp_zone_config)
                    }
                }
            }
        }
    }
}

pub fn visit_zone_edit<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: &EditedBlueprintZoneConfig<'e>,
) where
    V: VisitBlueprintZonesConfig<'e> + ?Sized,
{
    if let Edit::Change(disposition) = &node.disposition {
        v.visit_zone_disposition_change(ctx, disposition.into());
    }
    if let Edit::Change(filesystem_pool) = &node.filesystem_pool {
        v.visit_zone_filesystem_pool_change(ctx, filesystem_pool.into());
    }
    if let Edit::Change(zone_type) = &node.zone_type {
        v.visit_zone_zone_type_change(ctx, zone_type.into());
    }
}

/// Macro used to implement empty leaf node visitor functions
macro_rules! empty_leaf {
    ($name: ident, $t2: ty) => {
        fn $name<'e, V>(_v: &mut V, _ctx: &mut BpVisitorContext, _node: $t2)
        where
            V: VisitBlueprintZonesConfig<'e> + ?Sized,
        {
        }
    };
}

empty_leaf!(visit_generation_change, Change<'e, Generation>);
empty_leaf!(
    visit_zone_disposition_change,
    Change<'e, BlueprintZoneDisposition>
);
empty_leaf!(visit_zone_filesystem_pool_change, Change<'e, Option<ZpoolName>>);
empty_leaf!(visit_zone_zone_type_change, Change<'e, BlueprintZoneType>);
empty_leaf!(visit_zones_insert, &BlueprintZoneConfig);
empty_leaf!(visit_zones_remove, &BlueprintZoneConfig);
