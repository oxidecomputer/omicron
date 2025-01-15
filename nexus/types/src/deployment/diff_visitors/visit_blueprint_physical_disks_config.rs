// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A visitor for `BlueprintPhysicalDisksConfig`

use super::Change;
use crate::deployment::{
    BlueprintPhysicalDiskConfig, BlueprintPhysicalDiskDisposition,
    BlueprintPhysicalDisksConfig, BpVisitorContext, DiskIdentity,
    EditedBlueprintPhysicalDiskConfig, ZpoolUuid,
};
use diffus::edit::{map, Edit};
use omicron_common::api::external::Generation;

/// A trait to visit a [`BlueprintPhysicalDisksConfig`]
pub trait VisitBlueprintPhysicalDisksConfig<'e> {
    fn visit_root(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: Edit<'e, BlueprintPhysicalDisksConfig>,
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

    /// An insert to `BlueprintPhysicalDisksConfig::disks`
    fn visit_disks_insert(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _node: &BlueprintPhysicalDiskConfig,
    ) {
        // Leaf node
    }

    /// A removal from `BlueprintPhysicalDisksConfig::disks`
    fn visit_disks_remove(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _node: &BlueprintPhysicalDiskConfig,
    ) {
        // Leaf node
    }

    /// A change in a value in `BlueprintPhysicalDisksConfig::disks`
    fn visit_disk_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, BlueprintPhysicalDiskConfig>,
    ) {
        // Leaf node
    }

    /// A point where we allow direct access to a `diffus_derive` generated
    /// struct representing a change to a `BlueprintPhysicalDiskConfig`.
    ///
    /// This the equivalent node in the tree to `visit_disk_change`, but gives a
    /// `diffus_derive` generated structure rather than the before and after of
    /// the original structs that were diffed.
    fn visit_disk_edit(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &EditedBlueprintPhysicalDiskConfig<'e>,
    ) {
        visit_disk_edit(self, ctx, node);
    }

    /// A change in `BlueprintPhysicalDiskConfig::disposition`
    fn visit_disk_disposition_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, BlueprintPhysicalDiskDisposition>,
    ) {
        // Leaf node
    }

    /// A change in `BlueprintPhysicalDiskConfig::identity`
    fn visit_disk_identity_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, DiskIdentity>,
    ) {
        // Leaf node
    }

    /// A change in `BlueprintPhysicalDiskConfig::pool_id`
    fn visit_disk_pool_id_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, ZpoolUuid>,
    ) {
        // Leaf node
    }
}

/// The root of the diff for a [`BlueprintPhysicalDisksConfig`]
pub fn visit_root<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: Edit<'e, BlueprintPhysicalDisksConfig>,
) where
    V: VisitBlueprintPhysicalDisksConfig<'e> + ?Sized,
{
    if let Edit::Change { diff, .. } = node {
        if let Edit::Change { diff, .. } = diff.generation {
            v.visit_generation_change(ctx, diff.into());
        }

        if let Edit::Change { diff, .. } = diff.disks {
            for (&disk_id, edit) in &diff {
                ctx.disk_id = Some(*disk_id);
                match edit {
                    map::Edit::Copy(_) => {}
                    map::Edit::Insert(disk_config) => {
                        v.visit_disks_insert(ctx, disk_config);
                    }
                    map::Edit::Remove(disk_config) => {
                        v.visit_disks_remove(ctx, disk_config);
                    }
                    map::Edit::Change { before, after, diff } => {
                        v.visit_disk_change(ctx, Change::new(before, after));
                        v.visit_disk_edit(ctx, diff);
                    }
                }
            }
            // Reset the context
            ctx.disk_id = None;
        }
    }
}

pub fn visit_disk_edit<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: &EditedBlueprintPhysicalDiskConfig<'e>,
) where
    V: VisitBlueprintPhysicalDisksConfig<'e> + ?Sized,
{
    if let Edit::Change { before, after, .. } = &node.disposition {
        v.visit_disk_disposition_change(ctx, Change::new(*before, *after));
    }
    if let Edit::Change { before, after, .. } = &node.identity {
        v.visit_disk_identity_change(ctx, Change::new(*before, *after));
    }
    if let Edit::Change { before, after, .. } = &node.pool_id {
        v.visit_disk_pool_id_change(ctx, Change::new(*before, *after));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diffus::Diffable;
    use omicron_uuid_kinds::{PhysicalDiskUuid, ZpoolUuid};

    struct TestVisitor<'a> {
        before: &'a BlueprintPhysicalDisksConfig,
        after: &'a BlueprintPhysicalDisksConfig,
        total_inserts: usize,
        total_removes: usize,
        change_callbacks_fired: usize,
    }

    impl<'a> TestVisitor<'a> {
        pub fn new(
            before: &'a BlueprintPhysicalDisksConfig,
            after: &'a BlueprintPhysicalDisksConfig,
        ) -> Self {
            TestVisitor {
                before,
                after,
                total_inserts: 0,
                total_removes: 0,
                change_callbacks_fired: 0,
            }
        }
    }

    impl<'e> VisitBlueprintPhysicalDisksConfig<'e> for TestVisitor<'e> {
        fn visit_generation_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, Generation>,
        ) {
            assert_eq!(change.before, &self.before.generation);
            assert_eq!(change.after, &self.after.generation);
            assert_ne!(self.before.generation, self.after.generation);

            // We aren't operating on a specific disk
            assert!(ctx.disk_id.is_none());

            self.change_callbacks_fired += 1;
        }

        fn visit_disks_insert(
            &mut self,
            ctx: &mut BpVisitorContext,
            node: &BlueprintPhysicalDiskConfig,
        ) {
            assert!(!self.before.disks.contains_key(&node.id));
            assert!(self.after.disks.contains_key(&node.id));

            // The inserted node is the same as what's in `after`
            assert_eq!(node, self.after.disks.get(&node.id).unwrap());

            // The key for the current disk id was filled in
            assert_eq!(ctx.disk_id, Some(node.id));

            self.total_inserts += 1;
            self.change_callbacks_fired += 1;
        }

        fn visit_disks_remove(
            &mut self,
            ctx: &mut BpVisitorContext,
            node: &BlueprintPhysicalDiskConfig,
        ) {
            assert!(self.before.disks.contains_key(&node.id));
            assert!(!self.after.disks.contains_key(&node.id));

            // The removed node is the same as what's in `before`
            assert_eq!(node, self.before.disks.get(&node.id).unwrap());

            // The key for the current disk id was filled in
            assert_eq!(ctx.disk_id, Some(node.id));

            self.total_removes += 1;
            self.change_callbacks_fired += 1;
        }

        fn visit_disk_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, BlueprintPhysicalDiskConfig>,
        ) {
            // The key for the current disk id was filled in and
            // the disk with the same id was changed
            assert_eq!(ctx.disk_id, Some(change.before.id));
            assert_eq!(ctx.disk_id, Some(change.after.id));

            // The change is actually correct
            assert_eq!(
                self.before.disks.get(&ctx.disk_id.unwrap()),
                Some(change.before)
            );
            assert_eq!(
                self.after.disks.get(&ctx.disk_id.unwrap()),
                Some(change.after)
            );
            self.change_callbacks_fired += 1;
        }

        fn visit_disk_disposition_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, BlueprintPhysicalDiskDisposition>,
        ) {
            assert_ne!(change.before, change.after);
            assert_eq!(
                self.before
                    .disks
                    .get(&ctx.disk_id.unwrap())
                    .unwrap()
                    .disposition,
                *change.before
            );
            assert_eq!(
                self.after
                    .disks
                    .get(&ctx.disk_id.unwrap())
                    .unwrap()
                    .disposition,
                *change.after
            );
            self.change_callbacks_fired += 1;
        }

        fn visit_disk_identity_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, DiskIdentity>,
        ) {
            assert_ne!(change.before, change.after);
            assert_eq!(
                self.before.disks.get(&ctx.disk_id.unwrap()).unwrap().identity,
                *change.before
            );
            assert_eq!(
                self.after.disks.get(&ctx.disk_id.unwrap()).unwrap().identity,
                *change.after
            );
            self.change_callbacks_fired += 1;
        }

        fn visit_disk_pool_id_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, ZpoolUuid>,
        ) {
            assert_ne!(change.before, change.after);
            assert_eq!(
                self.before.disks.get(&ctx.disk_id.unwrap()).unwrap().pool_id,
                *change.before
            );
            assert_eq!(
                self.after.disks.get(&ctx.disk_id.unwrap()).unwrap().pool_id,
                *change.after
            );
            self.change_callbacks_fired += 1;
        }
    }

    #[test]
    fn diff_same_config() {
        let before = BlueprintPhysicalDisksConfig {
            generation: Generation::new(),
            disks: [BlueprintPhysicalDiskConfig {
                disposition: BlueprintPhysicalDiskDisposition::InService,
                identity: DiskIdentity {
                    vendor: "test".into(),
                    model: "test".into(),
                    serial: "test".into(),
                },
                id: PhysicalDiskUuid::new_v4(),
                pool_id: ZpoolUuid::new_v4(),
            }]
            .into_iter()
            .collect(),
        };
        let after = before.clone();

        let mut ctx = BpVisitorContext::default();
        let mut visitor = TestVisitor::new(&before, &after);
        let diff = before.diff(&after);
        visitor.visit_root(&mut ctx, diff);

        // No change callbacks should fire
        assert_eq!(0, visitor.change_callbacks_fired);
    }

    #[test]
    fn diff_field_changes() {
        let before = BlueprintPhysicalDisksConfig {
            generation: Generation::new(),
            disks: [BlueprintPhysicalDiskConfig {
                disposition: BlueprintPhysicalDiskDisposition::InService,
                identity: DiskIdentity {
                    vendor: "test".into(),
                    model: "test".into(),
                    serial: "test".into(),
                },
                id: PhysicalDiskUuid::new_v4(),
                pool_id: ZpoolUuid::new_v4(),
            }]
            .into_iter()
            .collect(),
        };
        let mut after = before.clone();
        after.generation = after.generation.next();
        {
            let mut disk = after.disks.iter_mut().next().unwrap();
            disk.disposition = BlueprintPhysicalDiskDisposition::Expunged;
            disk.identity.vendor = "modified".into();
            disk.pool_id = ZpoolUuid::new_v4();
        }

        let mut ctx = BpVisitorContext::default();
        let mut visitor = TestVisitor::new(&before, &after);
        let diff = before.diff(&after);
        visitor.visit_root(&mut ctx, diff);

        // A bunch of callbacks fire due to our diffs
        assert_eq!(5, visitor.change_callbacks_fired);
    }

    #[test]
    fn diff_insert_delete_disks() {
        let config1 = BlueprintPhysicalDiskConfig {
            disposition: BlueprintPhysicalDiskDisposition::InService,
            identity: DiskIdentity {
                vendor: "test".into(),
                model: "test".into(),
                serial: "test".into(),
            },
            id: PhysicalDiskUuid::new_v4(),
            pool_id: ZpoolUuid::new_v4(),
        };
        // Using a different `id` means this zone will count as an inserted one,
        // and the original zone will be counted as removed.
        let mut config2 = config1.clone();
        config2.id = PhysicalDiskUuid::new_v4();

        let before = BlueprintPhysicalDisksConfig {
            generation: Generation::new(),
            disks: [config1].into_iter().collect(),
        };

        let after = BlueprintPhysicalDisksConfig {
            generation: Generation::new(),
            disks: [config2].into_iter().collect(),
        };

        let mut ctx = BpVisitorContext::default();
        let mut visitor = TestVisitor::new(&before, &after);
        let diff = before.diff(&after);
        visitor.visit_root(&mut ctx, diff);

        assert_eq!(visitor.total_inserts, 1);
        assert_eq!(visitor.total_removes, 1);
        assert_eq!(visitor.change_callbacks_fired, 2);
    }
}
