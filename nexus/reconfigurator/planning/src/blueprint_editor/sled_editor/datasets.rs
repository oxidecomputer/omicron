// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blueprint_builder::EditCounts;
use crate::planner::SledPlannerRng;
use id_map::IdMap;
use illumos_utils::zpool::ZpoolName;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use omicron_common::api::external::ByteCount;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_common::disk::GzipLevel;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::net::SocketAddrV6;

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

/// Container for most of the information needed to construct a
/// `BlueprintDatasetConfig`.
///
/// Omitted from this set are the disposition (in practice, this will typically
/// be "in service", as one constructs a `PartialDatasetConfig` to describe a
/// dataset that should be in service) and the ID.
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

    pub fn for_local_storage_root(zpool: ZpoolName) -> Self {
        Self {
            name: DatasetName::new(zpool, DatasetKind::LocalStorage),
            address: None,
            quota: None,
            reservation: None,
            compression: CompressionAlgorithm::Off,
        }
    }
}

#[derive(Debug)]
pub(super) struct DatasetsEditor {
    datasets: IdMap<BlueprintDatasetConfig>,
    // Cache of _in service only_ datasets, identified by (zpool, kind).
    in_service_by_zpool_and_kind:
        BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, DatasetUuid>>,
    counts: EditCounts,
}

impl DatasetsEditor {
    pub fn new(
        datasets: IdMap<BlueprintDatasetConfig>,
    ) -> Result<Self, MultipleDatasetsOfKind> {
        let mut in_service_by_zpool_and_kind = BTreeMap::new();
        for dataset in datasets.iter() {
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
                BlueprintDatasetDisposition::Expunged => (),
            }
        }
        Ok(Self {
            datasets,
            in_service_by_zpool_and_kind,
            counts: EditCounts::zeroes(),
        })
    }

    pub fn empty() -> Self {
        Self {
            datasets: IdMap::new(),
            in_service_by_zpool_and_kind: BTreeMap::new(),
            counts: EditCounts::zeroes(),
        }
    }

    pub fn finalize(self) -> (IdMap<BlueprintDatasetConfig>, EditCounts) {
        (self.datasets, self.counts)
    }

    pub fn edit_counts(&self) -> EditCounts {
        self.counts
    }

    pub fn datasets<F>(
        &self,
        mut filter: F,
    ) -> impl Iterator<Item = &BlueprintDatasetConfig>
    where
        F: FnMut(BlueprintDatasetDisposition) -> bool,
    {
        self.datasets.iter().filter(move |dataset| filter(dataset.disposition))
    }

    // Private method; panics if given an ID that isn't present in
    // `self.config.datasets`. Callers must ensure the ID is valid.
    fn expunge_by_known_valid_id(&mut self, id: DatasetUuid) -> bool {
        let mut dataset = self
            .datasets
            .get_mut(&id)
            .expect("expunge_impl called with invalid ID");
        match dataset.disposition {
            BlueprintDatasetDisposition::InService => {
                dataset.disposition = BlueprintDatasetDisposition::Expunged;
                self.counts.expunged += 1;
                true
            }
            BlueprintDatasetDisposition::Expunged => {
                // already expunged; nothing to do
                false
            }
        }
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

    pub fn expunge_all_on_zpool(&mut self, zpool: &ZpoolUuid) -> usize {
        let Some(by_kind) = self.in_service_by_zpool_and_kind.remove(zpool)
        else {
            return 0;
        };

        let mut nexpunged = 0;
        for id in by_kind.into_values() {
            if self.expunge_by_known_valid_id(id) {
                nexpunged += 1;
            }
        }

        nexpunged
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
            // TODO-cleanup Is this guaranteed to be correct? E.g., if we're
            // supposed to be creating a new Nexus dataset on a particular zpool
            // when there's already another in-service Nexus dataset on that
            // zpool, we'll reuse that ID and update its properties instead of
            // creating a new one (or perhaps detecting this situation and
            // returning an error). Can we force our caller to pass in the
            // dataset ID we should be using? That would require tracking it in
            // the relevant parent blueprint structures (zones for zone-related
            // datasets, presumably disks for the top-level debug/zone root
            // datasets?).
            if let Some(bp_dataset_id) = self
                .in_service_by_zpool_and_kind
                .get(&pool.id())
                .and_then(|by_kind| by_kind.get(&kind).copied())
            {
                bp_dataset_id
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
        match self.datasets.entry(dataset.id) {
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
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::SledUuid;
    use proptest::prelude::*;
    use std::collections::BTreeSet;
    use test_strategy::Arbitrary;
    use test_strategy::proptest;
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

    fn build_test_config<I, J>(values: I) -> IdMap<BlueprintDatasetConfig>
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
        datasets
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
        fn into_config(self) -> IdMap<BlueprintDatasetConfig> {
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
        fn into_config(self) -> IdMap<BlueprintDatasetConfig> {
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
        fn into_config(self) -> IdMap<BlueprintDatasetConfig> {
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
        _ = DatasetsEditor::new(by_zpool.into_config()).expect("built editor");
    }

    #[proptest]
    fn proptest_create_editor_with_expunged_datasets(
        by_zpool: ZpoolsWithExpungedDatasets,
    ) {
        _ = DatasetsEditor::new(by_zpool.into_config()).expect("built editor");
    }

    #[proptest]
    fn proptest_add_same_kind_after_expunging(
        initial: ZpoolsWithMixedDatasets,
        rng_seed: u32,
    ) {
        let datasets = initial.into_config();
        let mut editor =
            DatasetsEditor::new(datasets.clone()).expect("built editor");

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
        for dataset in datasets
            .iter()
            .filter(|dataset| dataset.disposition.is_in_service())
        {
            editor
                .expunge(&dataset.pool.id(), &dataset.kind)
                .expect("expunged dataset");

            let new_dataset = PartialDatasetConfig {
                name: DatasetName::new(dataset.pool, dataset.kind.clone()),
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
        let datasets = initial.into_config();
        let all_zpools = datasets
            .iter()
            .map(|dataset| dataset.pool.id())
            .collect::<BTreeSet<_>>();
        let mut editor =
            DatasetsEditor::new(datasets.clone()).expect("built editor");

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
                    .datasets(BlueprintDatasetDisposition::is_in_service)
                    .any(|dataset| dataset.pool.id() == *zpool_id),
                "in-service dataset remains after expunging zpool"
            );
        }

        // For each originally-in-service dataset:
        //
        // 1. Add a new dataset of the same kind
        // 2. Ensure the new dataset ID is freshly-generated
        for dataset in datasets
            .iter()
            .filter(|dataset| dataset.disposition.is_in_service())
        {
            let new_dataset = PartialDatasetConfig {
                name: DatasetName::new(dataset.pool, dataset.kind.clone()),
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
