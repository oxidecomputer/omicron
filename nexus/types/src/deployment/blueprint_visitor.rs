// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An API for visiting diffs between blueprints generated via [diffus](https://github.com/oxidecomputer/diffus)
//!
//! Modelled after [`syn::visit`](https://docs.rs/syn/1/syn/visit).

use diffus::edit::{enm, map, Edit};
use omicron_common::{api::external::Generation, zpool_name::ZpoolName};
use omicron_uuid_kinds::{OmicronZoneUuid, SledUuid};

use super::{
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZoneType,
    BlueprintZonesConfig, EditedBlueprintZoneConfig,
    EditedBlueprintZonesConfig,
};

/// A context for blueprint related visitors
pub struct BpVisitorContext {
    pub sled_id: Option<SledUuid>,
    pub zone_id: Option<OmicronZoneUuid>,
}

#[derive(Debug, Clone, Copy)]
pub struct Change<'e, T> {
    pub before: &'e T,
    pub after: &'e T,
}

impl<'e, T> Change<'e, T> {
    pub fn new(before: &'e T, after: &'e T) -> Change<'e, T> {
        Change { before, after }
    }
}

/// A trait to visit a [`BlueprintZonesConfig`]
pub trait VisitBlueprintZonesConfig<'e> {
    fn visit_root(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: Edit<'e, BlueprintZonesConfig>,
    );

    /// A change to `BlueprintZonesConfig::generation`
    fn visit_generation_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, Generation>,
    );

    /// An insert to `BlueprintZonesConfig::zones`
    fn visit_zones_insert(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintZoneConfig,
    );

    /// A removal from `BlueprintZonesConfig::zones`
    fn visit_zones_remove(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &BlueprintZoneConfig,
    );

    // A change in a value in `BlueprintZonesConfig::zones`
    fn visit_zone_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, BlueprintZoneConfig>,
    );

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
    );

    /// A change in `BlueprintZoneConfig::disposition`
    fn visit_zone_disposition_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: Change<'e, BlueprintZoneDisposition>,
    );

    /// A change in a `BlueprintZoneConfig::filesystem_pool`
    fn visit_zone_filesystem_pool_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: Change<'e, Option<ZpoolName>>,
    );

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
    );
}
