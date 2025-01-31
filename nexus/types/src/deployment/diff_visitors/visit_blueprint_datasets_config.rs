// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A visitor for `BlueprintDatasetsConfig`

use super::Change;
use crate::deployment::{
    BlueprintDatasetConfig, BlueprintDatasetDisposition,
    BlueprintDatasetsConfig, BpVisitorContext, DatasetKind,
    EditedBlueprintDatasetConfig, EditedBlueprintDatasetsConfig, ZpoolName,
};
use diffus::edit::{map, Edit};
use omicron_common::{
    api::external::{ByteCount, Generation},
    disk::CompressionAlgorithm,
};
use std::net::SocketAddrV6;

/// A trait to visit a [`BlueprintDatasetsConfig`]
pub trait VisitBlueprintDatasetsConfig<'e> {
    fn visit_root(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: Edit<'e, BlueprintDatasetsConfig>,
    ) {
        visit_root(self, ctx, node);
    }

    /// An alternate top-level call for use by wrapper visitors
    fn visit_datasets_edit(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &EditedBlueprintDatasetsConfig<'e>,
    ) {
        visit_datasets_edit(self, ctx, node);
    }

    /// A change to `BlueprintZonesConfig::generation`
    fn visit_generation_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, Generation>,
    ) {
        // Leaf node
    }

    /// An insert to `BlueprintDatasetsConfig::datasets`
    fn visit_datasets_insert(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _node: &BlueprintDatasetConfig,
    ) {
        // Leaf node
    }

    /// A removal from `BlueprintDatasetsConfig::datasets`
    fn visit_datasets_remove(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _node: &BlueprintDatasetConfig,
    ) {
        // Leaf node
    }

    /// A change in a value in `BlueprintDatasetsConfig::datasets`
    fn visit_dataset_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, BlueprintDatasetConfig>,
    ) {
        // Leaf node
    }

    /// A point where we allow direct access to a `diffus_derive` generated
    /// struct representing a change to a `BlueprintDatasetConfig`.
    ///
    /// This is the equivalent node in the tree to `visit_dataset_change`, but gives a
    /// `diffus_derive` generated structure rather than the before and after of
    /// the original structs that were diffed.
    fn visit_dataset_edit(
        &mut self,
        ctx: &mut BpVisitorContext,
        node: &EditedBlueprintDatasetConfig<'e>,
    ) {
        visit_dataset_edit(self, ctx, node);
    }

    /// A change in `BlueprintDatasetConfig::disposition`
    fn visit_dataset_disposition_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, BlueprintDatasetDisposition>,
    ) {
        // Leaf node
    }
    /// A change in `BlueprintDatasetConfig::pool`
    fn visit_dataset_pool_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, ZpoolName>,
    ) {
        // Leaf node
    }
    /// A change in `BlueprintDatasetConfig::kind`
    fn visit_dataset_kind_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, DatasetKind>,
    ) {
        // Leaf node
    }
    /// A change in `BlueprintDatasetConfig::address`
    fn visit_dataset_address_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, Option<SocketAddrV6>>,
    ) {
        // Leaf node
    }
    /// A change in `BlueprintDatasetConfig::quota`
    fn visit_dataset_quota_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, Option<ByteCount>>,
    ) {
        // Leaf node
    }

    /// A change in `BlueprintDatasetConfig::reservation`
    fn visit_dataset_reservation_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, Option<ByteCount>>,
    ) {
        // Leaf node
    }

    /// A change in `BlueprintDatasetConfig::compression`
    fn visit_dataset_compression_change(
        &mut self,
        _ctx: &mut BpVisitorContext,
        _change: Change<'e, CompressionAlgorithm>,
    ) {
        // Leaf node
    }
}

pub fn visit_root<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: Edit<'e, BlueprintDatasetsConfig>,
) where
    V: VisitBlueprintDatasetsConfig<'e> + ?Sized,
{
    if let Edit::Change { diff, .. } = node {
        v.visit_datasets_edit(ctx, &diff);
    }
}

pub fn visit_datasets_edit<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: &EditedBlueprintDatasetsConfig<'e>,
) where
    V: VisitBlueprintDatasetsConfig<'e> + ?Sized,
{
    if let Edit::Change { diff, .. } = node.generation {
        v.visit_generation_change(ctx, diff.into());
    }
    if let Edit::Change { diff, .. } = &node.datasets {
        for (&dataset_id, edit) in diff {
            ctx.dataset_id = Some(*dataset_id);
            match edit {
                map::Edit::Copy(_) => {}
                map::Edit::Insert(dataset_config) => {
                    v.visit_datasets_insert(ctx, dataset_config);
                }
                map::Edit::Remove(dataset_config) => {
                    v.visit_datasets_remove(ctx, dataset_config);
                }
                map::Edit::Change { before, after, diff } => {
                    v.visit_dataset_change(ctx, Change::new(before, after));
                    v.visit_dataset_edit(ctx, &diff);
                }
            }
        }
        // Reset the context
        ctx.dataset_id = None;
    }
}

pub fn visit_dataset_edit<'e, V>(
    v: &mut V,
    ctx: &mut BpVisitorContext,
    node: &EditedBlueprintDatasetConfig<'e>,
) where
    V: VisitBlueprintDatasetsConfig<'e> + ?Sized,
{
    if let Edit::Change { before, after, .. } = &node.disposition {
        v.visit_dataset_disposition_change(ctx, Change::new(*before, *after));
    }
    if let Edit::Change { before, after, .. } = &node.pool {
        v.visit_dataset_pool_change(ctx, Change::new(*before, *after));
    }
    if let Edit::Change { before, after, .. } = &node.kind {
        v.visit_dataset_kind_change(ctx, Change::new(*before, *after));
    }
    if let Edit::Change { before, after, .. } = &node.address {
        v.visit_dataset_address_change(ctx, Change::new(*before, *after));
    }
    if let Edit::Change { before, after, .. } = &node.quota {
        v.visit_dataset_quota_change(ctx, Change::new(*before, *after));
    }
    if let Edit::Change { before, after, .. } = &node.reservation {
        v.visit_dataset_reservation_change(ctx, Change::new(*before, *after));
    }
    if let Edit::Change { before, after, .. } = &node.compression {
        v.visit_dataset_compression_change(ctx, Change::new(*before, *after));
    }
}
#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use super::*;
    use diffus::Diffable;
    use omicron_uuid_kinds::{DatasetUuid, ZpoolUuid};

    struct TestVisitor<'a> {
        before: &'a BlueprintDatasetsConfig,
        after: &'a BlueprintDatasetsConfig,
        total_inserts: usize,
        total_removes: usize,
        change_callbacks_fired: usize,
    }

    impl<'a> TestVisitor<'a> {
        pub fn new(
            before: &'a BlueprintDatasetsConfig,
            after: &'a BlueprintDatasetsConfig,
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

    impl<'e> VisitBlueprintDatasetsConfig<'e> for TestVisitor<'e> {
        fn visit_generation_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, Generation>,
        ) {
            assert_eq!(change.before, &self.before.generation);
            assert_eq!(change.after, &self.after.generation);
            assert_ne!(self.before.generation, self.after.generation);

            // We aren't operating on a specific dataset
            assert!(ctx.dataset_id.is_none());

            self.change_callbacks_fired += 1;
        }

        fn visit_datasets_insert(
            &mut self,
            ctx: &mut BpVisitorContext,
            node: &BlueprintDatasetConfig,
        ) {
            assert!(!self.before.datasets.contains_key(&node.id));
            assert!(self.after.datasets.contains_key(&node.id));

            // The inserted node is the same as what's in `after`
            assert_eq!(node, self.after.datasets.get(&node.id).unwrap());

            // The key for the current dataset id was filled in
            assert_eq!(ctx.dataset_id, Some(node.id));

            self.total_inserts += 1;
            self.change_callbacks_fired += 1;
        }

        fn visit_datasets_remove(
            &mut self,
            ctx: &mut BpVisitorContext,
            node: &BlueprintDatasetConfig,
        ) {
            assert!(self.before.datasets.contains_key(&node.id));
            assert!(!self.after.datasets.contains_key(&node.id));

            // The removed node is the same as what's in `before`
            assert_eq!(node, self.before.datasets.get(&node.id).unwrap());

            // The key for the current dataset id was filled in
            assert_eq!(ctx.dataset_id, Some(node.id));

            self.total_removes += 1;
            self.change_callbacks_fired += 1;
        }

        fn visit_dataset_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, BlueprintDatasetConfig>,
        ) {
            // The key for the current dataset id was filled in and
            // the dataset with the same id was changed
            assert_eq!(ctx.dataset_id, Some(change.before.id));
            assert_eq!(ctx.dataset_id, Some(change.after.id));

            // The change is actually correct
            assert_eq!(
                self.before.datasets.get(&ctx.dataset_id.unwrap()),
                Some(change.before)
            );
            assert_eq!(
                self.after.datasets.get(&ctx.dataset_id.unwrap()),
                Some(change.after)
            );
            self.change_callbacks_fired += 1;
        }

        fn visit_dataset_disposition_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, BlueprintDatasetDisposition>,
        ) {
            assert_ne!(change.before, change.after);
            assert_eq!(
                self.before
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .disposition,
                *change.before
            );
            assert_eq!(
                self.after
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .disposition,
                *change.after
            );
            self.change_callbacks_fired += 1;
        }

        fn visit_dataset_pool_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, ZpoolName>,
        ) {
            assert_ne!(change.before, change.after);
            assert_eq!(
                self.before
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .pool,
                *change.before
            );
            assert_eq!(
                self.after.datasets.get(&ctx.dataset_id.unwrap()).unwrap().pool,
                *change.after
            );
            self.change_callbacks_fired += 1;
        }

        fn visit_dataset_kind_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, DatasetKind>,
        ) {
            assert_ne!(change.before, change.after);
            assert_eq!(
                self.before
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .kind,
                *change.before
            );
            assert_eq!(
                self.after.datasets.get(&ctx.dataset_id.unwrap()).unwrap().kind,
                *change.after
            );
            self.change_callbacks_fired += 1;
        }

        fn visit_dataset_address_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, Option<SocketAddrV6>>,
        ) {
            assert_ne!(change.before, change.after);
            assert_eq!(
                self.before
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .address,
                *change.before
            );
            assert_eq!(
                self.after
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .address,
                *change.after
            );
            self.change_callbacks_fired += 1;
        }

        fn visit_dataset_quota_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, Option<ByteCount>>,
        ) {
            assert_ne!(change.before, change.after);
            assert_eq!(
                self.before
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .quota,
                *change.before
            );
            assert_eq!(
                self.after
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .quota,
                *change.after
            );
            self.change_callbacks_fired += 1;
        }

        fn visit_dataset_reservation_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, Option<ByteCount>>,
        ) {
            assert_ne!(change.before, change.after);
            assert_eq!(
                self.before
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .reservation,
                *change.before
            );
            assert_eq!(
                self.after
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .reservation,
                *change.after
            );
            self.change_callbacks_fired += 1;
        }

        fn visit_dataset_compression_change(
            &mut self,
            ctx: &mut BpVisitorContext,
            change: Change<'e, CompressionAlgorithm>,
        ) {
            assert_ne!(change.before, change.after);
            assert_eq!(
                self.before
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .compression,
                *change.before
            );
            assert_eq!(
                self.after
                    .datasets
                    .get(&ctx.dataset_id.unwrap())
                    .unwrap()
                    .compression,
                *change.after
            );
            self.change_callbacks_fired += 1;
        }
    }

    #[test]
    fn diff_same_config() {
        let before = BlueprintDatasetsConfig {
            generation: Generation::new(),
            datasets: [BlueprintDatasetConfig {
                disposition: BlueprintDatasetDisposition::InService,
                id: DatasetUuid::new_v4(),
                pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
                kind: DatasetKind::Cockroach,
                address: None,
                quota: None,
                reservation: None,
                compression: CompressionAlgorithm::On,
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
        let before = BlueprintDatasetsConfig {
            generation: Generation::new(),
            datasets: [BlueprintDatasetConfig {
                disposition: BlueprintDatasetDisposition::InService,
                id: DatasetUuid::new_v4(),
                pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
                kind: DatasetKind::Cockroach,
                address: None,
                quota: None,
                reservation: None,
                compression: CompressionAlgorithm::On,
            }]
            .into_iter()
            .collect(),
        };
        let mut after = before.clone();
        after.generation = after.generation.next();
        {
            let mut dataset = after.datasets.iter_mut().next().unwrap();
            dataset.disposition = BlueprintDatasetDisposition::Expunged;
            dataset.pool = ZpoolName::new_external(ZpoolUuid::new_v4());
            dataset.kind = DatasetKind::Clickhouse;
            dataset.address = Some(SocketAddrV6::new(
                Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1),
                9999,
                0,
                0,
            ));
            dataset.quota = Some(ByteCount::from_kibibytes_u32(200));
            dataset.reservation = Some(ByteCount::from_mebibytes_u32(100));
            dataset.compression = CompressionAlgorithm::Off;
        }

        let mut ctx = BpVisitorContext::default();
        let mut visitor = TestVisitor::new(&before, &after);
        let diff = before.diff(&after);
        visitor.visit_root(&mut ctx, diff);

        // A bunch of callbacks fire due to our diffs
        // 7 fields modified + the parent change to the dataset + generation
        assert_eq!(9, visitor.change_callbacks_fired);
    }

    #[test]
    fn diff_insert_delete_datasets() {
        let config1 = BlueprintDatasetConfig {
            disposition: BlueprintDatasetDisposition::InService,
            id: DatasetUuid::new_v4(),
            pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
            kind: DatasetKind::Cockroach,
            address: None,
            quota: None,
            reservation: None,
            compression: CompressionAlgorithm::On,
        };

        // Using a different `id` means this zone will count as an inserted one,
        // and the original zone will be counted as removed.
        let mut config2 = config1.clone();
        config2.id = DatasetUuid::new_v4();

        let before = BlueprintDatasetsConfig {
            generation: Generation::new(),
            datasets: [config1].into_iter().collect(),
        };

        let after = BlueprintDatasetsConfig {
            generation: Generation::new(),
            datasets: [config2].into_iter().collect(),
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
