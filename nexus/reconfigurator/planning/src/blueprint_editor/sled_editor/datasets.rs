// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blueprint_builder::EditCounts;
use crate::planner::SledPlannerRng;
use illumos_utils::zpool::ZpoolName;
use nexus_types::deployment::id_map::{self, IdMap};
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::deployment::BlueprintDatasetsConfig;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::ZpoolFilter;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_common::disk::GzipLevel;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;

#[cfg(test)]
use nexus_types::deployment::BlueprintDatasetFilter;

#[derive(Debug, thiserror::Error)]
#[error(
    "invalid blueprint input: multiple datasets with kind {kind:?} \
     on zpool {zpool_id}: {id1}, {id2}"
)]
pub struct MultipleDatasetsOfKind {
    zpool_id: ZpoolUuid,
    kind: DatasetKind,
    id1: DatasetUuid,
    id2: DatasetUuid,
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetsEditError {
    #[error(
        "tried to expunge nonexistent dataset: \
         zpool {zpool_id}, kind {kind}"
    )]
    ExpungeNonexistentDataset { zpool_id: ZpoolUuid, kind: DatasetKind },
}

/// TODO(<https://github.com/oxidecomputer/omicron/issues/6645>): In between
/// the addition of datasets to blueprints and knowing all deployed system
/// have _generated_ a blueprint that populates datasets, we are in a sticky
/// situation where a dataset might have already existed in CRDB with an ID,
/// but the blueprint system doesn't know about it. We accept a map of all
/// existing dataset IDs, and then when determining the ID of a dataset,
/// we'll try these in order:
///
/// 1. Is the dataset in our blueprint already? If so, use its ID.
/// 2. Is the dataset in `preexisting_database_ids`? If so, use that ID.
/// 3. Generate a new random ID.
#[derive(Debug)]
pub(crate) struct DatasetIdsBackfillFromDb(
    BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, DatasetUuid>>,
);

impl DatasetIdsBackfillFromDb {
    pub fn build(
        resources: &SledResources,
    ) -> Result<Self, MultipleDatasetsOfKind> {
        let iter = resources.all_datasets(ZpoolFilter::InService).flat_map(
            |(&zpool_id, configs)| {
                configs.iter().map(move |config| {
                    (zpool_id, config.name.kind().clone(), config.id)
                })
            },
        );

        let mut kind_id_map: BTreeMap<
            ZpoolUuid,
            BTreeMap<DatasetKind, DatasetUuid>,
        > = BTreeMap::new();

        for (zpool_id, kind, dataset_id) in iter {
            let dataset_ids_by_kind = kind_id_map.entry(zpool_id).or_default();
            match dataset_ids_by_kind.entry(kind) {
                Entry::Vacant(slot) => {
                    slot.insert(dataset_id);
                }
                Entry::Occupied(prev) => {
                    return Err(MultipleDatasetsOfKind {
                        zpool_id,
                        kind: prev.key().clone(),
                        id1: *prev.get(),
                        id2: dataset_id,
                    });
                }
            }
        }
        Ok(Self(kind_id_map))
    }

    pub fn empty() -> Self {
        Self(BTreeMap::new())
    }
}

impl DatasetIdsBackfillFromDb {
    fn get(
        &self,
        zpool_id: &ZpoolUuid,
        kind: &DatasetKind,
    ) -> Option<DatasetUuid> {
        self.0.get(zpool_id).and_then(|by_kind| by_kind.get(kind).copied())
    }
}

/// Container for most of the information needed to construct a
/// `BlueprintDatasetConfig`.
///
/// Omitted from this set are the disposition (in practice, this will typically
/// be "in service", as one constructs a `PartialDatasetConfig` to describe a
/// dataset that should be in service) and the ID. Dataset IDs are a little
/// tricky at the moment (see `DatasetIdsBackfillFromDb` above), so they're
/// determined internally by `DatasetsEditor`.
#[derive(Debug)]
pub(crate) struct PartialDatasetConfig {
    pub name: DatasetName,
    pub address: Option<SocketAddrV6>,
    pub quota: Option<ByteCount>,
    pub reservation: Option<ByteCount>,
    pub compression: CompressionAlgorithm,
}

impl PartialDatasetConfig {
    pub fn zpool(&self) -> &ZpoolName {
        self.name.pool()
    }

    pub fn for_debug(zpool: ZpoolName) -> Self {
        const DEBUG_QUOTA_SIZE_GB: u32 = 100;

        Self {
            name: DatasetName::new(zpool, DatasetKind::Debug),
            address: None,
            quota: Some(ByteCount::from_gibibytes_u32(DEBUG_QUOTA_SIZE_GB)),
            reservation: None,
            compression: CompressionAlgorithm::GzipN {
                level: GzipLevel::new::<9>(),
            },
        }
    }

    pub fn for_transient_zone_root(zpool: ZpoolName) -> Self {
        Self {
            name: DatasetName::new(zpool, DatasetKind::TransientZoneRoot),
            address: None,
            quota: None,
            reservation: None,
            compression: CompressionAlgorithm::Off,
        }
    }

    pub fn for_transient_zone(name: DatasetName) -> Self {
        assert!(
            matches!(name.kind(), DatasetKind::TransientZone { .. }),
            "for_transient_zone called with incorrect dataset kind: {name:?}"
        );
        Self {
            name,
            address: None,
            quota: None,
            reservation: None,
            compression: CompressionAlgorithm::Off,
        }
    }

    pub fn for_durable_zone(
        zpool: ZpoolName,
        kind: DatasetKind,
        address: Option<SocketAddrV6>,
    ) -> Self {
        Self {
            name: DatasetName::new(zpool, kind),
            address,
            quota: None,
            reservation: None,
            compression: CompressionAlgorithm::Off,
        }
    }
}

#[derive(Debug)]
pub(super) struct DatasetsEditor {
    preexisting_dataset_ids: DatasetIdsBackfillFromDb,
    config: BlueprintDatasetsConfig,
    // Cache of _in service only_ datasets, identified by (zpool, kind).
    in_service_by_zpool_and_kind:
        BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, DatasetUuid>>,
    // Cache of _expunged_ dataset IDs. This serves as a list of IDs from
    // `preexisting_dataset_ids` to ignore, as we shouldn't reuse old IDs if
    // they belong to expunged datasets. We should be able to remove this when
    // we remove `preexisting_dataset_ids`.
    expunged_datasets: BTreeSet<DatasetUuid>,
    counts: EditCounts,
}

impl DatasetsEditor {
    pub fn new(
        config: BlueprintDatasetsConfig,
        preexisting_dataset_ids: DatasetIdsBackfillFromDb,
    ) -> Result<Self, MultipleDatasetsOfKind> {
        let mut in_service_by_zpool_and_kind = BTreeMap::new();
        let mut expunged_datasets = BTreeSet::new();
        for dataset in config.datasets.iter() {
            match dataset.disposition {
                BlueprintDatasetDisposition::InService => {
                    let by_kind: &mut BTreeMap<_, _> =
                        in_service_by_zpool_and_kind
                            .entry(dataset.pool.id())
                            .or_default();
                    match by_kind.entry(dataset.kind.clone()) {
                        Entry::Vacant(slot) => {
                            slot.insert(dataset.id);
                        }
                        Entry::Occupied(prev) => {
                            return Err(MultipleDatasetsOfKind {
                                zpool_id: dataset.pool.id(),
                                kind: dataset.kind.clone(),
                                id1: *prev.get(),
                                id2: dataset.id,
                            });
                        }
                    }
                }
                BlueprintDatasetDisposition::Expunged => {
                    expunged_datasets.insert(dataset.id);
                }
            }
        }
        Ok(Self {
            preexisting_dataset_ids,
            config,
            in_service_by_zpool_and_kind,
            expunged_datasets,
            counts: EditCounts::zeroes(),
        })
    }

    pub fn empty(preexisting_dataset_ids: DatasetIdsBackfillFromDb) -> Self {
        Self {
            preexisting_dataset_ids,
            config: BlueprintDatasetsConfig {
                generation: Generation::new(),
                datasets: IdMap::new(),
            },
            in_service_by_zpool_and_kind: BTreeMap::new(),
            expunged_datasets: BTreeSet::new(),
            counts: EditCounts::zeroes(),
        }
    }

    pub fn finalize(self) -> (BlueprintDatasetsConfig, EditCounts) {
        let mut config = self.config;
        if self.counts.has_nonzero_counts() {
            config.generation = config.generation.next();
        }
        (config, self.counts)
    }

    pub fn edit_counts(&self) -> EditCounts {
        self.counts
    }

    #[cfg(test)]
    pub fn datasets(
        &self,
        filter: BlueprintDatasetFilter,
    ) -> impl Iterator<Item = &BlueprintDatasetConfig> {
        self.config
            .datasets
            .iter()
            .filter(move |dataset| dataset.disposition.matches(filter))
    }

    // Private method; panics if given an ID that isn't present in
    // `self.config.datasets`. Callers must ensure the ID is valid.
    fn expunge_by_known_valid_id(&mut self, id: DatasetUuid) {
        let mut dataset = self
            .config
            .datasets
            .get_mut(&id)
            .expect("expunge_impl called with invalid ID");
        match dataset.disposition {
            BlueprintDatasetDisposition::InService => {
                dataset.disposition = BlueprintDatasetDisposition::Expunged;
                self.counts.expunged += 1;
            }
            BlueprintDatasetDisposition::Expunged => {
                // already expunged; nothing to do
            }
        }
        self.expunged_datasets.insert(dataset.id);
    }

    /// Expunge a dataset identified by its zpool + kind combo.
    ///
    /// TODO-cleanup This is a little fishy and should be replaced with
    /// an expunge-by-ID method instead, but that requires some rework
    /// (<https://github.com/oxidecomputer/omicron/issues/7214>).
    pub fn expunge(
        &mut self,
        zpool: &ZpoolUuid,
        kind: &DatasetKind,
    ) -> Result<(), DatasetsEditError> {
        let Some(id) = self
            .in_service_by_zpool_and_kind
            .get_mut(zpool)
            .and_then(|by_kind| by_kind.remove(kind))
        else {
            return Err(DatasetsEditError::ExpungeNonexistentDataset {
                zpool_id: *zpool,
                kind: kind.clone(),
            });
        };
        self.expunge_by_known_valid_id(id);
        Ok(())
    }

    pub fn expunge_all_on_zpool(&mut self, zpool: &ZpoolUuid) {
        let Some(by_kind) = self.in_service_by_zpool_and_kind.remove(zpool)
        else {
            return;
        };

        for id in by_kind.into_values() {
            self.expunge_by_known_valid_id(id);
        }
    }

    pub fn ensure_in_service(
        &mut self,
        dataset: PartialDatasetConfig,
        rng: &mut SledPlannerRng,
    ) -> id_map::RefMut<'_, BlueprintDatasetConfig> {
        // Convert the partial config into a full config by finding or
        // generating its ID.
        let PartialDatasetConfig {
            name,
            address,
            quota,
            reservation,
            compression,
        } = dataset;
        let (pool, kind) = name.into_parts();

        let id = {
            // If there is a dataset of the given `kind` on the given
            // `zpool`, find its ID.
            //
            // This prefers IDs we already have; if we don't have one, it
            // falls back to backfilling based on IDs recorded in the
            // database from before blueprints tracked datasets (see
            // `DatasetIdsBackfillFromDb` above).
            if let Some(blueprint_id) = self
                .in_service_by_zpool_and_kind
                .get(&pool.id())
                .and_then(|by_kind| by_kind.get(&kind).copied())
            {
                blueprint_id
            } else if let Some(preexisting_database_id) =
                self.preexisting_dataset_ids.get(&pool.id(), &kind)
            {
                // Only use old database IDs if this ID hasn't been expunged.
                //
                // This check won't work if there's a preexisting_database_id
                // for an old dataset that has been both expunged _and removed_,
                // as we have no way of knowing about completely removed
                // datasets. However:
                //
                // 1. `DatasetIdsBackfillFromDb::build()` filters to only
                //    in-service datasets, so we should never find a database ID
                //    for a removed dataset.
                // 2. We don't yet ever remove datasets anyway, and hopefully
                //    `DatasetIdsBackfillFromDb` is entirely removed by then (it
                //    should be removeable after R12, once we've guaranteed all
                //    blueprints have datasets).
                if !self.expunged_datasets.contains(&preexisting_database_id) {
                    preexisting_database_id
                } else {
                    rng.next_dataset()
                }
            } else {
                rng.next_dataset()
            }
        };

        let dataset = BlueprintDatasetConfig {
            disposition: BlueprintDatasetDisposition::InService,
            id,
            pool,
            kind,
            address,
            quota,
            reservation,
            compression,
        };

        // Add or update our config with this new dataset info.
        match self.config.datasets.entry(dataset.id) {
            id_map::Entry::Vacant(slot) => {
                self.in_service_by_zpool_and_kind
                    .entry(dataset.pool.id())
                    .or_default()
                    .insert(dataset.kind.clone(), dataset.id);
                self.counts.added += 1;
                slot.insert(dataset)
            }
            id_map::Entry::Occupied(mut prev) => {
                if *prev.get() != dataset {
                    self.counts.updated += 1;
                    prev.insert(dataset);
                }
                prev.into_mut()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::PlannerRng;
    use nexus_types::deployment::BlueprintDatasetFilter;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::SledUuid;
    use proptest::prelude::*;
    use std::collections::BTreeSet;
    use test_strategy::proptest;
    use test_strategy::Arbitrary;
    use uuid::Uuid;

    // Helper functions to "tag" an iterator (i.e., turn it into an iterator of
    // tuples) for use with `build_test_config()` below.
    fn all_in_service<I>(
        value: I,
    ) -> impl Iterator<Item = (BlueprintDatasetDisposition, DatasetKind)>
    where
        I: IntoIterator<Item = DatasetKind>,
    {
        value
            .into_iter()
            .map(|kind| (BlueprintDatasetDisposition::InService, kind))
    }
    fn all_expunged<I>(
        value: I,
    ) -> impl Iterator<Item = (BlueprintDatasetDisposition, DatasetKind)>
    where
        I: IntoIterator<Item = DatasetKind>,
    {
        value
            .into_iter()
            .map(|kind| (BlueprintDatasetDisposition::Expunged, kind))
    }

    fn build_test_config<I, J>(values: I) -> BlueprintDatasetsConfig
    where
        I: Iterator<Item = J>,
        J: Iterator<Item = (BlueprintDatasetDisposition, DatasetKind)>,
    {
        let mut datasets = IdMap::new();
        let mut dataset_id_index = 0;
        for (zpool_id_index, disposition_kinds) in values.enumerate() {
            let zpool_id = ZpoolUuid::from_untyped_uuid(Uuid::from_u128(
                zpool_id_index as u128,
            ));
            for (disposition, kind) in disposition_kinds {
                let id = {
                    let id = DatasetUuid::from_untyped_uuid(Uuid::from_u128(
                        dataset_id_index,
                    ));
                    dataset_id_index += 1;
                    id
                };
                let dataset = BlueprintDatasetConfig {
                    disposition,
                    id,
                    pool: ZpoolName::new_external(zpool_id),
                    kind,
                    address: None,
                    quota: None,
                    reservation: None,
                    compression: CompressionAlgorithm::Off,
                };
                let prev = datasets.insert(dataset);
                assert!(prev.is_none(), "no duplicate dataset IDs");
            }
        }
        let mut generation = Generation::new();
        if dataset_id_index > 0 {
            generation = generation.next();
        }
        BlueprintDatasetsConfig { generation, datasets }
    }

    #[derive(Debug, Arbitrary)]
    struct DatasetKindSet {
        #[strategy(prop::collection::btree_set(any::<DatasetKind>(), 0..16))]
        kinds: BTreeSet<DatasetKind>,
    }

    #[derive(Debug, Arbitrary)]
    struct ZpoolsWithInServiceDatasets {
        #[strategy(prop::collection::vec(any::<DatasetKindSet>(), 0..10))]
        by_zpool: Vec<DatasetKindSet>,
    }

    impl ZpoolsWithInServiceDatasets {
        fn into_config(self) -> BlueprintDatasetsConfig {
            build_test_config(
                self.by_zpool
                    .into_iter()
                    .map(|kinds| all_in_service(kinds.kinds)),
            )
        }
    }

    #[derive(Debug, Arbitrary)]
    struct DatasetKindVec {
        #[strategy(prop::collection::vec(any::<DatasetKind>(), 0..32))]
        kinds: Vec<DatasetKind>,
    }

    #[derive(Debug, Arbitrary)]
    struct ZpoolsWithExpungedDatasets {
        #[strategy(prop::collection::vec(any::<DatasetKindVec>(), 0..10))]
        by_zpool: Vec<DatasetKindVec>,
    }

    impl ZpoolsWithExpungedDatasets {
        fn into_config(self) -> BlueprintDatasetsConfig {
            build_test_config(
                self.by_zpool
                    .into_iter()
                    .map(|kinds| all_expunged(kinds.kinds)),
            )
        }
    }

    // Proptest helper to construct zpools with both in-service datasets (the
    // first element of the tuple: a set of kinds) and expunged datasets (the
    // second element of the tuple: a vec of kinds).
    #[derive(Debug, Arbitrary)]
    struct ZpoolsWithMixedDatasets {
        #[strategy(prop::collection::vec(any::<(DatasetKindSet, DatasetKindVec)>(), 0..10))]
        by_zpool: Vec<(DatasetKindSet, DatasetKindVec)>,
    }

    impl ZpoolsWithMixedDatasets {
        fn into_config(self) -> BlueprintDatasetsConfig {
            build_test_config(self.by_zpool.into_iter().map(
                |(in_service, expunged)| {
                    all_in_service(in_service.kinds)
                        .chain(all_expunged(expunged.kinds))
                },
            ))
        }
    }

    #[proptest]
    fn proptest_create_editor_with_in_service_datasets(
        by_zpool: ZpoolsWithInServiceDatasets,
    ) {
        _ = DatasetsEditor::new(
            by_zpool.into_config(),
            DatasetIdsBackfillFromDb::empty(),
        )
        .expect("built editor");
    }

    #[proptest]
    fn proptest_create_editor_with_expunged_datasets(
        by_zpool: ZpoolsWithExpungedDatasets,
    ) {
        _ = DatasetsEditor::new(
            by_zpool.into_config(),
            DatasetIdsBackfillFromDb::empty(),
        )
        .expect("built editor");
    }

    #[proptest]
    fn proptest_add_same_kind_after_expunging(
        initial: ZpoolsWithMixedDatasets,
        rng_seed: u32,
    ) {
        let config = initial.into_config();
        let mut editor = DatasetsEditor::new(
            config.clone(),
            DatasetIdsBackfillFromDb::empty(),
        )
        .expect("built editor");

        let mut rng = PlannerRng::from_seed((
            rng_seed,
            "proptest_add_same_kind_after_expunging",
        ));

        // We need a sled ID to get a sled-specific RNG from `rng`; we're not
        // testing blueprints as a whole here, so steal a blueprint ID and use
        // it as a sled ID to get reproducibility.
        let sled_id = SledUuid::from_untyped_uuid(
            rng.next_blueprint().into_untyped_uuid(),
        );
        let rng = rng.sled_rng(sled_id);

        // For each originally-in-service dataset:
        //
        // 1. Expunge that dataset
        // 2. Add a new dataset of the same kind
        // 3. Ensure the new dataset ID is freshly-generated
        for dataset in config.datasets.iter().filter(|dataset| {
            dataset.disposition.matches(BlueprintDatasetFilter::InService)
        }) {
            editor
                .expunge(&dataset.pool.id(), &dataset.kind)
                .expect("expunged dataset");

            let new_dataset = PartialDatasetConfig {
                name: DatasetName::new(
                    dataset.pool.clone(),
                    dataset.kind.clone(),
                ),
                address: dataset.address,
                quota: dataset.quota,
                reservation: dataset.reservation,
                compression: dataset.compression,
            };
            let new_dataset = editor.ensure_in_service(new_dataset, rng);
            assert_ne!(dataset.id, new_dataset.id);
        }

        // Repeat the test above, but this time assume all the dataset IDs were
        // also present in the backfill database map. We should not reuse IDs
        // after expunging zones.
        let database_backfill = {
            let mut by_zpool: BTreeMap<_, BTreeMap<_, _>> = BTreeMap::new();
            for dataset in config.datasets.iter().filter(|dataset| {
                dataset.disposition.matches(BlueprintDatasetFilter::InService)
            }) {
                let prev = by_zpool
                    .entry(dataset.pool.id())
                    .or_default()
                    .insert(dataset.kind.clone(), dataset.id);
                assert!(
                    prev.is_none(),
                    "duplicate (pool,kind) in-service input"
                );
            }
            DatasetIdsBackfillFromDb(by_zpool)
        };
        let mut editor = DatasetsEditor::new(config.clone(), database_backfill)
            .expect("built editor");
        for dataset in config.datasets.iter().filter(|dataset| {
            dataset.disposition.matches(BlueprintDatasetFilter::InService)
        }) {
            editor
                .expunge(&dataset.pool.id(), &dataset.kind)
                .expect("expunged dataset");

            let new_dataset = PartialDatasetConfig {
                name: DatasetName::new(
                    dataset.pool.clone(),
                    dataset.kind.clone(),
                ),
                address: dataset.address,
                quota: dataset.quota,
                reservation: dataset.reservation,
                compression: dataset.compression,
            };
            let new_dataset = editor.ensure_in_service(new_dataset, rng);
            assert_ne!(dataset.id, new_dataset.id);
        }
    }

    #[proptest]
    fn proptest_add_same_kind_after_expunging_by_zpool(
        initial: ZpoolsWithMixedDatasets,
        rng_seed: u32,
    ) {
        let config = initial.into_config();
        let all_zpools = config
            .datasets
            .iter()
            .map(|dataset| dataset.pool.id())
            .collect::<BTreeSet<_>>();
        let mut editor = DatasetsEditor::new(
            config.clone(),
            DatasetIdsBackfillFromDb::empty(),
        )
        .expect("built editor");

        let mut rng = PlannerRng::from_seed((
            rng_seed,
            "proptest_add_same_kind_after_expunging",
        ));

        // We need a sled ID to get a sled-specific RNG from `rng`; we're not
        // testing blueprints as a whole here, so steal a blueprint ID and use
        // it as a sled ID to get reproducibility.
        let sled_id = SledUuid::from_untyped_uuid(
            rng.next_blueprint().into_untyped_uuid(),
        );
        let rng = rng.sled_rng(sled_id);

        // Expunge all datasets on all zpools, by zpool.
        for zpool_id in &all_zpools {
            editor.expunge_all_on_zpool(zpool_id);
            // There should no longer be any in-service datasets on this zpool.
            assert!(
                !editor
                    .datasets(BlueprintDatasetFilter::InService)
                    .any(|dataset| dataset.pool.id() == *zpool_id),
                "in-service dataset remains after expunging zpool"
            );
        }

        // For each originally-in-service dataset:
        //
        // 1. Add a new dataset of the same kind
        // 2. Ensure the new dataset ID is freshly-generated
        for dataset in config.datasets.iter().filter(|dataset| {
            dataset.disposition.matches(BlueprintDatasetFilter::InService)
        }) {
            let new_dataset = PartialDatasetConfig {
                name: DatasetName::new(
                    dataset.pool.clone(),
                    dataset.kind.clone(),
                ),
                address: dataset.address,
                quota: dataset.quota,
                reservation: dataset.reservation,
                compression: dataset.compression,
            };
            let new_dataset = editor.ensure_in_service(new_dataset, rng);
            assert_ne!(dataset.id, new_dataset.id);
        }

        // Repeat the test above, but this time assume all the dataset IDs were
        // also present in the backfill database map. We should not reuse IDs
        // after expunging zones.
        let database_backfill = {
            let mut by_zpool: BTreeMap<_, BTreeMap<_, _>> = BTreeMap::new();
            for dataset in config.datasets.iter().filter(|dataset| {
                dataset.disposition.matches(BlueprintDatasetFilter::InService)
            }) {
                let prev = by_zpool
                    .entry(dataset.pool.id())
                    .or_default()
                    .insert(dataset.kind.clone(), dataset.id);
                assert!(
                    prev.is_none(),
                    "duplicate (pool,kind) in-service input"
                );
            }
            DatasetIdsBackfillFromDb(by_zpool)
        };
        let mut editor = DatasetsEditor::new(config.clone(), database_backfill)
            .expect("built editor");
        for zpool_id in &all_zpools {
            editor.expunge_all_on_zpool(zpool_id);
            // There should no longer be any in-service datasets on this zpool.
            assert!(
                !editor
                    .datasets(BlueprintDatasetFilter::InService)
                    .any(|dataset| dataset.pool.id() == *zpool_id),
                "in-service dataset remains after expunging zpool"
            );
        }
        for dataset in config.datasets.iter().filter(|dataset| {
            dataset.disposition.matches(BlueprintDatasetFilter::InService)
        }) {
            let new_dataset = PartialDatasetConfig {
                name: DatasetName::new(
                    dataset.pool.clone(),
                    dataset.kind.clone(),
                ),
                address: dataset.address,
                quota: dataset.quota,
                reservation: dataset.reservation,
                compression: dataset.compression,
            };
            let new_dataset = editor.ensure_in_service(new_dataset, rng);
            assert_ne!(dataset.id, new_dataset.id);
        }
    }
}
