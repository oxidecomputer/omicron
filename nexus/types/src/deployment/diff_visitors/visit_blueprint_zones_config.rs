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
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, Generation>,
    ) {
        // Leaf node
    }

    /// An insert to `BlueprintZonesConfig::zones`
    fn visit_zones_insert(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _node: &BlueprintZoneConfig,
    ) {
        // Leaf node
    }

    /// A removal from `BlueprintZonesConfig::zones`
    fn visit_zones_remove(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _node: &BlueprintZoneConfig,
    ) {
        // Leaf node
    }

    // A change in a value in `BlueprintZonesConfig::zones`
    fn visit_zone_change(
        &mut self,
        ctx: &mut BpVisitorContext,
        change: Change<'e, BlueprintZoneConfig>,
    ) {
        // Leaf node
    }

    // The representation of a diffus generated `EditedBlueprintZoneConfig`
    // which contains recursive edits for each field when there is at least
    // one change.
    //
    // This the equivalent node in the tree to `visit_zone_change`, but gives a
    // diffus_derive generated structure rather than the before and after of the
    // original structs that were diffed.
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
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, BlueprintZoneDisposition>,
    ) {
        // Leaf node
    }

    /// A change in a `BlueprintZoneConfig::filesystem_pool`
    fn visit_zone_filesystem_pool_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, Option<ZpoolName>>,
    ) {
        // Leaf node
    }

    /// A change in a `BlueprintZoneConfig::zone_type`
    ///
    /// A `BlueprintZoneType` is a complicated structure. In order to keep the
    /// first version of this visitor tractable, we just return the a `Change`
    /// rather than a diffus `EditedBlueprintZoneType` that we have to walk.
    /// We'll likely want to add this to get full coverage, but it can come
    /// later, and should probably live in its own visitor that we call from
    /// this point.
    fn visit_zone_zone_type_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _node: Change<'e, BlueprintZoneType>,
    ) {
        // Leaf node - for now
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
    if let Edit::Change { diff: bp_zones_config, .. } = node {
        if let Edit::Change { diff, .. } = bp_zones_config.generation {
            v.visit_generation_change(ctx, diff.into());
        }
        if let Edit::Change { diff, .. } = bp_zones_config.zones {
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
                    map::Edit::Change {
                        before,
                        after,
                        diff: edited_bp_zone_config,
                        ..
                    } => {
                        v.visit_zone_change(ctx, Change::new(before, after));
                        v.visit_zone_edit(ctx, edited_bp_zone_config);
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
    if let Edit::Change { diff, .. } = &node.disposition {
        v.visit_zone_disposition_change(ctx, diff.into());
    }
    if let Edit::Change { diff, .. } = &node.filesystem_pool {
        v.visit_zone_filesystem_pool_change(ctx, diff.into());
    }
    if let Edit::Change { diff, .. } = &node.zone_type {
        v.visit_zone_zone_type_change(ctx, diff.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diffus::Diffable;
    use std::collections::BTreeSet;
    use test_strategy::proptest;

    struct TestVisitor<'a> {
        before: &'a BlueprintZonesConfig,
        after: &'a BlueprintZonesConfig,
        total_inserts: usize,
        total_removes: usize,
    }

    impl<'a> TestVisitor<'a> {
        pub fn new(
            before: &'a BlueprintZonesConfig,
            after: &'a BlueprintZonesConfig,
        ) -> Self {
            TestVisitor { before, after, total_inserts: 0, total_removes: 0 }
        }
    }

    impl<'e> VisitBlueprintZonesConfig<'e> for TestVisitor<'e> {
        fn visit_generation_change(
            &mut self,
            _ctx: &mut BpVisitorContext,
            change: Change<'e, Generation>,
        ) {
            assert_eq!(change.before, &self.before.generation);
            assert_eq!(change.after, &self.after.generation);
            assert_ne!(self.before.generation, self.after.generation);
        }

        fn visit_zones_insert(
            &mut self,
            _ctx: &mut BpVisitorContext,
            node: &BlueprintZoneConfig,
        ) {
            let before: BTreeSet<_> = self.before.zones.keys().collect();
            let after: BTreeSet<_> = self.after.zones.keys().collect();
            assert!(!before.contains(&node.id));
            assert!(after.contains(&node.id));

            // The inserted node is the same as what's in `after`
            assert_eq!(node, self.after.zones.get(&node.id).unwrap());

            self.total_inserts += 1;
        }

        fn visit_zones_remove(
            &mut self,
            _ctx: &mut BpVisitorContext,
            node: &BlueprintZoneConfig,
        ) {
            let before: BTreeSet<_> = self.before.zones.keys().collect();
            let after: BTreeSet<_> = self.after.zones.keys().collect();
            assert!(before.contains(&node.id));
            assert!(!after.contains(&node.id));

            // The removed node is the same as what's in `before`
            assert_eq!(node, self.before.zones.get(&node.id).unwrap());

            self.total_removes += 1;
        }
    }

    #[proptest]
    fn diff(before: BlueprintZonesConfig, after: BlueprintZonesConfig) {
        let mut ctx = BpVisitorContext::default();
        let mut visitor = TestVisitor::new(&before, &after);
        let diff = before.diff(&after);
        visitor.visit_root(&mut ctx, diff);

        assert_eq!(
            visitor.total_inserts.wrapping_sub(visitor.total_removes),
            after.zones.len().wrapping_sub(before.zones.len())
        );
    }
}
