// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blueprint_builder::EditCounts;
use crate::planner::PlannerRng;
use illumos_utils::zpool::ZpoolName;
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
    #[error("tried to expunge nonexistent dataset: {id}")]
    ExpungeNonexistentDataset { id: DatasetUuid },
}

/// TODO(https://github.com/oxidecomputer/omicron/issues/6645): In between
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
pub(crate) struct PreexistingDatasetIds(
    BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, DatasetUuid>>,
);

impl PreexistingDatasetIds {
    pub fn build(
        resources: &SledResources,
    ) -> Result<Self, MultipleDatasetsOfKind> {
        let iter = resources.all_datasets(ZpoolFilter::InService).flat_map(
            |(&zpool_id, configs)| {
                configs.iter().map(move |config| {
                    (zpool_id, config.name.dataset().clone(), config.id)
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

impl PreexistingDatasetIds {
    fn get(
        &self,
        zpool_id: &ZpoolUuid,
        kind: &DatasetKind,
    ) -> Option<DatasetUuid> {
        self.0.get(zpool_id).and_then(|by_kind| by_kind.get(kind).copied())
    }
}

#[derive(Debug)]
pub(crate) struct PartialDatasetConfig {
    pub name: DatasetName,
    pub address: Option<SocketAddrV6>,
    pub quota: Option<ByteCount>,
    pub reservation: Option<ByteCount>,
    pub compression: CompressionAlgorithm,
}

impl PartialDatasetConfig {
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
            matches!(name.dataset(), DatasetKind::TransientZone { .. }),
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

    // Helper to generate a full `BlueprintDatasetConfig` from a partial config;
    // we either look up the ID (if we're updating an existing dataset) or
    // generate a new one via `rng`.
    //
    // TODO-cleanup It seems awkward we don't know whether we're updating or
    // adding at this point. For zones, should we store the dataset ID
    // explicitly so we don't need to do this lookup for updates? Less sure what
    // we'd do with extra datasets like Debug and ZoneRoot.
    pub fn build(
        self,
        datasets: &DatasetsEditor,
        rng: &mut PlannerRng,
    ) -> BlueprintDatasetConfig {
        let Self { name, address, quota, reservation, compression } = self;
        let (pool, kind) = name.into_parts();
        let id = datasets
            .get_id(&pool.id(), &kind)
            .unwrap_or_else(|| rng.next_dataset());
        BlueprintDatasetConfig {
            disposition: BlueprintDatasetDisposition::InService,
            id,
            pool,
            kind,
            address,
            quota,
            reservation,
            compression,
        }
    }
}

#[derive(Debug)]
pub(super) struct DatasetsEditor {
    preexisting_dataset_ids: PreexistingDatasetIds,
    config: BlueprintDatasetsConfig,
    by_zpool_and_kind: BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, DatasetUuid>>,
    counts: EditCounts,
}

impl DatasetsEditor {
    pub fn new(
        config: BlueprintDatasetsConfig,
        preexisting_dataset_ids: PreexistingDatasetIds,
    ) -> Result<Self, MultipleDatasetsOfKind> {
        let mut by_zpool_and_kind = BTreeMap::new();
        for dataset in config.datasets.values() {
            let by_kind: &mut BTreeMap<_, _> =
                by_zpool_and_kind.entry(dataset.pool.id()).or_default();
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
        Ok(Self {
            preexisting_dataset_ids,
            config,
            by_zpool_and_kind,
            counts: EditCounts::zeroes(),
        })
    }

    pub fn empty(preexisting_dataset_ids: PreexistingDatasetIds) -> Self {
        Self {
            preexisting_dataset_ids,
            config: BlueprintDatasetsConfig {
                generation: Generation::new(),
                datasets: BTreeMap::new(),
            },
            by_zpool_and_kind: BTreeMap::new(),
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

    /// If there is a dataset of the given `kind` on the given `zpool`, return
    /// its ID.
    pub fn get_id(
        &self,
        zpool: &ZpoolUuid,
        kind: &DatasetKind,
    ) -> Option<DatasetUuid> {
        if let Some(blueprint_id) = self
            .by_zpool_and_kind
            .get(zpool)
            .and_then(|by_kind| by_kind.get(kind).copied())
        {
            return Some(blueprint_id);
        };
        if let Some(preexisting_database_id) =
            self.preexisting_dataset_ids.get(zpool, kind)
        {
            return Some(preexisting_database_id);
        };
        None
    }

    pub fn expunge(
        &mut self,
        id: &DatasetUuid,
    ) -> Result<(), DatasetsEditError> {
        Self::expunge_by_id(&mut self.config, id, &mut self.counts)
    }

    fn expunge_by_id(
        config: &mut BlueprintDatasetsConfig,
        id: &DatasetUuid,
        counts: &mut EditCounts,
    ) -> Result<(), DatasetsEditError> {
        let dataset = config.datasets.get_mut(id).ok_or_else(|| {
            DatasetsEditError::ExpungeNonexistentDataset { id: *id }
        })?;
        match dataset.disposition {
            BlueprintDatasetDisposition::InService => {
                dataset.disposition = BlueprintDatasetDisposition::Expunged;
                counts.expunged += 1;
            }
            BlueprintDatasetDisposition::Expunged => {
                // already expunged; nothing to do
            }
        }
        Ok(())
    }

    pub fn expunge_all_on_zpool(&mut self, zpool: &ZpoolUuid) {
        let Some(by_kind) = self.by_zpool_and_kind.get(zpool) else {
            return;
        };

        for id in by_kind.values() {
            Self::expunge_by_id(&mut self.config, id, &mut self.counts)
                .expect("by_zpool_and_kind out of sync");
        }
    }

    pub fn ensure(&mut self, dataset: BlueprintDatasetConfig) {
        match self.config.datasets.entry(dataset.id) {
            Entry::Vacant(slot) => {
                self.by_zpool_and_kind
                    .entry(dataset.pool.id())
                    .or_default()
                    .insert(dataset.kind.clone(), dataset.id);
                slot.insert(dataset);
                self.counts.added += 1;
            }
            Entry::Occupied(mut prev) => {
                if *prev.get() != dataset {
                    prev.insert(dataset);
                    self.counts.updated += 1;
                }
            }
        }
    }
}
