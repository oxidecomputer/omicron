// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An API modelled after [`syn::visit`](https://docs.rs/syn/1/syn/visit).
use crate::deployment::Blueprint;
use crate::external_api::views::SledState;
use diffus::edit::{enm, map, Edit};
use omicron_common::{api::external::Generation, zpool_name::EditedZpoolName};
use omicron_uuid_kinds::{OmicronZoneUuid, SledUuid};
use std::collections::BTreeMap;
use uuid::Uuid;

// Structs are prefixed via `Edited` when `diffus_derive` generates them.
// In this case each field of the edited version becomes a type like
// `diffus::edit::Edit<'a, OriginalFieldType>`.
use super::{
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZonesConfig,
    EditedBlueprint, EditedBlueprintZoneConfig, EditedBlueprintZonesConfig,
    ZpoolName,
};

#[derive(Debug, Clone)]
pub struct Change<'e, T> {
    pub before: &'e T,
    pub after: &'e T,
}

/// A trait to visit all the edits of a `Blueprint`.
///
/// Nodes are ordered by field and grouped hierarchically
pub trait Visit<'e> {
    // Visitors for the blueprint itself
    fn visit_blueprint_edit(&mut self, node: &'e Edit<'e, Blueprint>) {
        visit_blueprint_edit(self, node);
    }
    fn visit_blueprint_copy(&mut self, node: &'e Blueprint) {
        visit_blueprint_copy(self, node);
    }
    fn visit_blueprint_change(&mut self, node: &'e EditedBlueprint) {
        visit_blueprint_change(self, node);
    }

    // Visitors for `id`
    fn visit_id_edit(&mut self, node: &'e Edit<Uuid>) {
        visit_id_edit(self, node);
    }
    fn visit_id_copy(&mut self, node: &'e Uuid) {
        visit_id_copy(self, node);
    }
    fn visit_id_change(&mut self, node: Change<'e, Uuid>) {
        visit_id_change(self, node);
    }

    // Visitors for `sled_state`
    fn visit_sled_state_edit(
        &mut self,
        node: &'e Edit<BTreeMap<SledUuid, SledState>>,
    ) {
        visit_sled_state_edit(self, node);
    }
    fn visit_sled_state_copy(
        &mut self,
        node: &'e BTreeMap<SledUuid, SledState>,
    ) {
        visit_sled_state_copy(self, node);
    }
    fn visit_sled_state_change(
        &mut self,
        node: &'e BTreeMap<&'e SledUuid, map::Edit<SledState>>,
    ) {
        visit_sled_state_change(self, node);
    }
    fn visit_sled_state_map_copy(
        &mut self,
        sled_id: &'e SledUuid,
        state: &'e SledState,
    ) {
        visit_sled_state_map_copy(self, sled_id, state);
    }
    fn visit_sled_state_map_insert(
        &mut self,
        sled_id: &'e SledUuid,
        state: &'e SledState,
    ) {
        visit_sled_state_map_insert(self, sled_id, state);
    }
    fn visit_sled_state_map_remove(
        &mut self,
        sled_id: &'e SledUuid,
        state: &'e SledState,
    ) {
        visit_sled_state_map_remove(self, sled_id, state);
    }
    fn visit_sled_state_map_change(
        &mut self,
        sled_id: &'e SledUuid,
        node: Change<'e, SledState>,
    ) {
        visit_sled_state_map_change(self, sled_id, node);
    }

    /// Visitors for `blueprint_zone`
    fn visit_blueprint_zones_copy(
        &mut self,
        node: &'e BTreeMap<SledUuid, BlueprintZonesConfig>,
    ) {
        visit_blueprint_zones_copy(self, node);
    }
    fn visit_blueprint_zones_change(
        &mut self,
        node: &'e BTreeMap<&'e SledUuid, map::Edit<BlueprintZonesConfig>>,
    ) {
        visit_blueprint_zones_change(self, node);
    }
    fn visit_blueprint_zones_map_copy(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e BlueprintZonesConfig,
    ) {
        visit_blueprint_zones_map_copy(self, sled_id, node);
    }
    fn visit_blueprint_zones_map_insert(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e BlueprintZonesConfig,
    ) {
        visit_blueprint_zones_map_insert(self, sled_id, node);
    }
    fn visit_blueprint_zones_map_remove(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e BlueprintZonesConfig,
    ) {
        visit_blueprint_zones_map_remove(self, sled_id, node);
    }
    fn visit_blueprint_zones_map_change(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e EditedBlueprintZonesConfig,
    ) {
        visit_blueprint_zones_map_change(self, sled_id, node);
    }

    // blueprint.blueprint_zones.generation
    fn visit_blueprint_zones_generation_copy(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e Generation,
    ) {
        visit_blueprint_zones_generation_copy(self, sled_id, node);
    }
    fn visit_blueprint_zones_generation_change(
        &mut self,
        sled_id: &'e SledUuid,
        node: Change<'e, Generation>,
    ) {
        visit_blueprint_zones_generation_change(self, sled_id, node);
    }

    // blueprint.blueprint_zones.zones
    fn visit_blueprint_zones_zones_copy(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e BTreeMap<OmicronZoneUuid, BlueprintZoneConfig>,
    ) {
        visit_blueprint_zones_zones_copy(self, sled_id, node);
    }
    fn visit_blueprint_zones_zone_config_copy(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e BlueprintZoneConfig,
    ) {
        visit_blueprint_zones_zone_config_copy(self, sled_id, node);
    }
    fn visit_blueprint_zones_zone_config_insert(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e BlueprintZoneConfig,
    ) {
        visit_blueprint_zones_zone_config_insert(self, sled_id, node);
    }
    fn visit_blueprint_zones_zone_config_remove(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e BlueprintZoneConfig,
    ) {
        visit_blueprint_zones_zone_config_remove(self, sled_id, node);
    }
    fn visit_blueprint_zones_zone_config_change(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e EditedBlueprintZoneConfig,
    ) {
        visit_blueprint_zones_zone_config_change(self, sled_id, node);
    }
    fn visit_blueprint_zones_zone_config_disposition_copy(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e BlueprintZoneDisposition,
    ) {
        visit_blueprint_zones_zone_config_disposition_copy(self, sled_id, node);
    }
    fn visit_blueprint_zones_zone_config_disposition_change(
        &mut self,
        sled_id: &'e SledUuid,
        node: Change<'e, BlueprintZoneDisposition>,
    ) {
        visit_blueprint_zones_zone_config_disposition_change(
            self, sled_id, node,
        )
    }
    fn visit_blueprint_zones_zone_config_filesystem_pool_copy(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e Option<ZpoolName>,
    ) {
        visit_blueprint_zones_zone_config_filesystem_pool_copy(
            self, sled_id, node,
        );
    }
    fn visit_blueprint_zones_zone_config_filesystem_pool_change(
        &mut self,
        sled_id: &'e SledUuid,
        node: Change<'e, Option<ZpoolName>>,
    ) {
        visit_blueprint_zones_zone_config_filesystem_pool_change(
            self, sled_id, node,
        );
    }
}

/// The root of the diff
pub fn visit_blueprint_edit<'e, V>(v: &mut V, node: &'e Edit<'e, Blueprint>)
where
    V: Visit<'e> + ?Sized,
{
    match node {
        Edit::Copy(blueprint) => v.visit_blueprint_copy(*blueprint),
        Edit::Change(edited_blueprint) => {
            v.visit_blueprint_change(edited_blueprint)
        }
    }
}

/// The blueprint has changed in some manner
pub fn visit_blueprint_change<'e, V>(v: &mut V, node: &'e EditedBlueprint)
where
    V: Visit<'e> + ?Sized,
{
    v.visit_id_edit(&node.id);
    v.visit_sled_state_edit(&node.sled_state);

    match &node.blueprint_zones {
        Edit::Copy(node) => v.visit_blueprint_zones_copy(node),
        Edit::Change(node) => v.visit_blueprint_zones_change(&node),
    }
}

pub fn visit_id_edit<'e, V>(v: &mut V, node: &'e Edit<Uuid>)
where
    V: Visit<'e> + ?Sized,
{
    match node {
        Edit::Copy(node) => v.visit_id_copy(*node),
        Edit::Change((before, after)) => {
            v.visit_id_change(Change { before: *before, after: *after })
        }
    }
}

pub fn visit_sled_state_edit<'e, V>(
    v: &mut V,
    node: &'e Edit<BTreeMap<SledUuid, SledState>>,
) where
    V: Visit<'e> + ?Sized,
{
    match node {
        Edit::Copy(node) => v.visit_sled_state_copy(node),
        Edit::Change(node) => v.visit_sled_state_change(node),
    }
}

pub fn visit_sled_state_change<'e, V>(
    v: &mut V,
    node: &'e BTreeMap<&'e SledUuid, map::Edit<SledState>>,
) where
    V: Visit<'e> + ?Sized,
{
    for (sled_id, node) in node {
        match node {
            map::Edit::Copy(sled_state) => {
                v.visit_sled_state_map_copy(*sled_id, *sled_state);
            }
            map::Edit::Insert(sled_state) => {
                v.visit_sled_state_map_insert(*sled_id, *sled_state);
            }
            map::Edit::Remove(sled_state) => {
                v.visit_sled_state_map_remove(*sled_id, *sled_state);
            }
            map::Edit::Change(diff) => {
                match diff {
                    enm::Edit::Copy(_) => {
                        // We know this map entry was modified, so it won't be a copy
                        unreachable!()
                    }
                    enm::Edit::VariantChanged(before, after) => {
                        v.visit_sled_state_map_change(
                            *sled_id,
                            Change { before: *before, after: *after },
                        );
                    }
                    enm::Edit::AssociatedChanged(_) => {
                        // `SledState` has no associated data
                        unreachable!()
                    }
                }
            }
        }
    }
}

pub fn visit_blueprint_zones_change<'e, V>(
    v: &mut V,
    node: &'e BTreeMap<&'e SledUuid, map::Edit<BlueprintZonesConfig>>,
) where
    V: Visit<'e> + ?Sized,
{
    for (sled_id, node) in node {
        match node {
            map::Edit::Copy(node) => {
                v.visit_blueprint_zones_map_copy(sled_id, node)
            }
            map::Edit::Insert(node) => {
                v.visit_blueprint_zones_map_insert(sled_id, node)
            }
            map::Edit::Remove(node) => {
                v.visit_blueprint_zones_map_remove(sled_id, node)
            }
            map::Edit::Change(node) => {
                v.visit_blueprint_zones_map_change(sled_id, node)
            }
        }
    }
}

pub fn visit_blueprint_zones_map_change<'e, V>(
    v: &mut V,
    sled_id: &'e SledUuid,
    node: &'e EditedBlueprintZonesConfig,
) where
    V: Visit<'e> + ?Sized,
{
    // Has the generation changed?
    match &node.generation {
        Edit::Copy(node) => {
            v.visit_blueprint_zones_generation_copy(sled_id, *node);
        }
        Edit::Change((before, after)) => {
            v.visit_blueprint_zones_generation_change(
                sled_id,
                Change { before: *before, after: *after },
            );
        }
    }

    match &node.zones {
        Edit::Copy(node) => v.visit_blueprint_zones_zones_copy(sled_id, *node),
        Edit::Change(zones) => {
            for (_, node) in zones {
                match node {
                    map::Edit::Copy(node) => {
                        v.visit_blueprint_zones_zone_config_copy(
                            sled_id, *node,
                        );
                    }
                    map::Edit::Insert(node) => {
                        v.visit_blueprint_zones_zone_config_insert(
                            sled_id, *node,
                        );
                    }
                    map::Edit::Remove(node) => {
                        v.visit_blueprint_zones_zone_config_remove(
                            sled_id, node,
                        );
                    }
                    map::Edit::Change(node) => {
                        v.visit_blueprint_zones_zone_config_change(
                            sled_id, node,
                        );
                    }
                }
            }
        }
    }
}

pub fn visit_blueprint_zones_zone_config_change<'e, V>(
    v: &mut V,
    sled_id: &'e SledUuid,
    node: &'e EditedBlueprintZoneConfig,
) where
    V: Visit<'e> + ?Sized,
{
    match &node.disposition {
        Edit::Copy(node) => {
            v.visit_blueprint_zones_zone_config_disposition_copy(
                sled_id, *node,
            );
        }
        Edit::Change(node) => {
            match node {
                enm::Edit::Copy(node) => {
                    v.visit_blueprint_zones_zone_config_disposition_copy(
                        sled_id, *node,
                    );
                }
                enm::Edit::VariantChanged(before, after) => {
                    v.visit_blueprint_zones_zone_config_disposition_change(
                        sled_id,
                        Change { before: *before, after: *after },
                    );
                }
                enm::Edit::AssociatedChanged(_) => {
                    // There's no associated data for a `BlueprintZoneDisposition`
                    unreachable!()
                }
            }
        }
    }

    // No need to match on `id` as that will show up as a map insert in the prior node

    match &node.filesystem_pool {
        Edit::Copy(node) => {
            v.visit_blueprint_zones_zone_config_filesystem_pool_copy(
                sled_id, *node,
            );
        }
        Edit::Change(node) => match node {
            enm::Edit::Copy(node) => {
                v.visit_blueprint_zones_zone_config_filesystem_pool_copy(
                    sled_id, *node,
                );
            }
            enm::Edit::VariantChanged(before, after) => {
                v.visit_blueprint_zones_zone_config_filesystem_pool_change(
                    sled_id,
                    Change { before: *before, after: *after },
                );
            }
            enm::Edit::AssociatedChanged(_diff) => {
                // `ZpoolName`'s cannot change
                unreachable!();
            }
        },
    }
}

// 2 parameter version of macro to implement empty leaf visitor methods with a
// `Change` parameter last
macro_rules! empty_visit_change_2 {
    ($name: ident, $t: ty) => {
        fn $name<'e, V>(_v: &mut V, _change: Change<'e, $t>)
        where
            V: Visit<'e> + ?Sized,
        {
        }
    };
}

// 3 parameter version of macro to implement empty leaf visitor methods with a
// `Change` parameter last
macro_rules! empty_visit_change_3 {
    ($name: ident, $t: ty, $t2: ty) => {
        fn $name<'e, V>(_v: &mut V, _key: &'e $t, _change: Change<'e, $t2>)
        where
            V: Visit<'e> + ?Sized,
        {
        }
    };
}

// 2 parameter version of macro to implement empty leaf visitor methods of the
// right form
macro_rules! empty_visit_2 {
    ($name: ident, $t: ty) => {
        fn $name<'e, V>(_v: &mut V, _node: &'e $t)
        where
            V: Visit<'e> + ?Sized,
        {
        }
    };
}

// 3 parameter version of macro to implement empty leaf visitor methods of the
// right form
macro_rules! empty_visit_3 {
    ($name: ident, $t: ty, $t2: ty) => {
        fn $name<'e, V>(_v: &mut V, _key: &'e $t, _node: &'e $t2)
        where
            V: Visit<'e> + ?Sized,
        {
        }
    };
}

empty_visit_2!(visit_blueprint_copy, Blueprint);
empty_visit_2!(visit_id_copy, Uuid);
empty_visit_change_2!(visit_id_change, Uuid);
empty_visit_2!(visit_sled_state_copy, BTreeMap <SledUuid, SledState>);
empty_visit_3!(visit_sled_state_map_copy, SledUuid, SledState);
empty_visit_change_3!(visit_sled_state_map_change, SledUuid, SledState);
empty_visit_3!(visit_sled_state_map_insert, SledUuid, SledState);
empty_visit_3!(visit_sled_state_map_remove, SledUuid, SledState);
empty_visit_2!(visit_blueprint_zones_copy, BTreeMap<SledUuid, BlueprintZonesConfig>);
empty_visit_3!(visit_blueprint_zones_map_copy, SledUuid, BlueprintZonesConfig);
empty_visit_3!(
    visit_blueprint_zones_map_insert,
    SledUuid,
    BlueprintZonesConfig
);
empty_visit_3!(
    visit_blueprint_zones_map_remove,
    SledUuid,
    BlueprintZonesConfig
);
empty_visit_3!(visit_blueprint_zones_generation_copy, SledUuid, Generation);
empty_visit_change_3!(
    visit_blueprint_zones_generation_change,
    SledUuid,
    Generation
);
empty_visit_3!(
    visit_blueprint_zones_zones_copy,
    SledUuid,
    BTreeMap<OmicronZoneUuid, BlueprintZoneConfig>
);
empty_visit_3!(
    visit_blueprint_zones_zone_config_copy,
    SledUuid,
    BlueprintZoneConfig
);
empty_visit_3!(
    visit_blueprint_zones_zone_config_insert,
    SledUuid,
    BlueprintZoneConfig
);
empty_visit_3!(
    visit_blueprint_zones_zone_config_remove,
    SledUuid,
    BlueprintZoneConfig
);
empty_visit_3!(
    visit_blueprint_zones_zone_config_disposition_copy,
    SledUuid,
    BlueprintZoneDisposition
);
empty_visit_change_3!(
    visit_blueprint_zones_zone_config_disposition_change,
    SledUuid,
    BlueprintZoneDisposition
);
empty_visit_3!(
    visit_blueprint_zones_zone_config_filesystem_pool_copy,
    SledUuid,
    Option<ZpoolName>
);
empty_visit_change_3!(
    visit_blueprint_zones_zone_config_filesystem_pool_change,
    SledUuid,
    Option<ZpoolName>
);

/// A visitor for debug printing walks of a blueprint
pub struct DebugVisitor;

impl<'e> Visit<'e> for DebugVisitor {
    fn visit_blueprint_edit(&mut self, node: &'e Edit<'e, Blueprint>) {
        println!("blueprint edit");

        visit_blueprint_edit(self, node);
    }

    fn visit_blueprint_copy(&mut self, node: &'e Blueprint) {
        println!("blueprint copy");
        visit_blueprint_copy(self, node);
    }

    fn visit_blueprint_change(&mut self, node: &'e EditedBlueprint) {
        println!("blueprint change");
        visit_blueprint_change(self, node);
    }

    fn visit_id_change(&mut self, node: Change<'e, Uuid>) {
        println!("blueprint id changed: {:?}", node);
    }

    fn visit_sled_state_map_insert(
        &mut self,
        sled_id: &'e SledUuid,
        state: &'e SledState,
    ) {
        println!("sled added {}: {}", sled_id, state);
    }

    fn visit_sled_state_map_remove(
        &mut self,
        sled_id: &'e SledUuid,
        state: &'e SledState,
    ) {
        println!("sled removed {}: {}", sled_id, state);
    }

    fn visit_sled_state_map_change(
        &mut self,
        sled_id: &'e SledUuid,
        node: Change<'e, SledState>,
    ) {
        println!("sled state changed {}: {:?}", sled_id, node);
    }

    fn visit_blueprint_zones_map_insert(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e BlueprintZonesConfig,
    ) {
        println!("Added zones to sled_{}: {:#?}", sled_id, node);
    }

    fn visit_blueprint_zones_map_remove(
        &mut self,
        sled_id: &'e SledUuid,
        node: &'e BlueprintZonesConfig,
    ) {
        println!("Removed zones from sled_{}: {:#?}", sled_id, node);
    }
}
