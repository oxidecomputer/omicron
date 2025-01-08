// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A visitor for `BlueprintZonesConfig`

use super::{
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZoneType,
    BlueprintZonesConfig, BpVisitorContext, Change, EditedBlueprintZoneConfig,
    EditedBlueprintZonesConfig,
};

use diffus::edit::{enm, map, Edit};
use omicron_common::{api::external::Generation, zpool_name::ZpoolName};
use omicron_uuid_kinds::{OmicronZoneUuid, SledUuid};

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
        visit_zone_change(self, ctx, change);
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
        node: EditedBlueprintZoneConfig,
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
                        todo!()
                    }
                }
            }
        }
    }
}
