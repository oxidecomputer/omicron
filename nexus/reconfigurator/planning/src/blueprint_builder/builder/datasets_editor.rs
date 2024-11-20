// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper for editing the datasets of a Blueprint

use crate::planner::PlannerRng;

use super::Ensure;
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
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;

#[derive(Debug, thiserror::Error)]
pub enum BlueprintDatasetsEditError {
    #[error(
        "{data_source} inconsistency: multiple datasets with kind {kind:?} \
         on zpool {zpool_id}: {id1}, {id2}"
    )]
    MultipleDatasetsOfKind {
        data_source: &'static str,
        zpool_id: ZpoolUuid,
        kind: DatasetKind,
        id1: DatasetUuid,
        id2: DatasetUuid,
    },
}

/// Helper for working with sets of datasets on each sled
///
/// Tracking the set of datasets is slightly non-trivial because we need to
/// bump the per-sled generation number iff the datasets are changed.  So
/// we need to keep track of whether we've changed the datasets relative
/// to the parent blueprint.
#[derive(Debug)]
pub(super) struct BlueprintDatasetsEditor {
    current: BTreeMap<SledUuid, BlueprintDatasetsConfig>,
    changed: BTreeSet<SledUuid>,
}

impl BlueprintDatasetsEditor {
    pub fn new(current: BTreeMap<SledUuid, BlueprintDatasetsConfig>) -> Self {
        Self { current, changed: BTreeSet::new() }
    }

    /// Get a helper to edit the datasets of a specific sled.
    ///
    /// If any changes are made via the returned editor, the sled will be
    /// recorded as needing a generation bump in its dataset config when the
    /// editor is dropped.
    pub fn sled_datasets_editor<'a>(
        &'a mut self,
        sled_id: SledUuid,
        sled_resources: &SledResources,
        rng: &'a mut PlannerRng,
    ) -> Result<SledDatasetsEditor<'a>, BlueprintDatasetsEditError> {
        let config = self
            .current
            .entry(sled_id)
            .or_insert_with(empty_blueprint_datasets_config);

        // Gather all dataset IDs known to the database.
        //
        // See the comment below where this is used; this is a
        // backwards-compatibility layer for
        // https://github.com/oxidecomputer/omicron/issues/6645.
        let database_dataset_ids = build_dataset_kind_id_map(
            "database",
            sled_resources.all_datasets(ZpoolFilter::InService).flat_map(
                |(&zpool_id, configs)| {
                    configs.iter().map(move |config| {
                        (zpool_id, config.name.dataset().clone(), config.id)
                    })
                },
            ),
        )?;

        SledDatasetsEditor::new(
            rng,
            database_dataset_ids,
            sled_id,
            config,
            &mut self.changed,
        )
    }

    pub fn build(
        mut self,
        sled_ids: impl Iterator<Item = SledUuid>,
    ) -> BTreeMap<SledUuid, BlueprintDatasetsConfig> {
        sled_ids
            .map(|sled_id| {
                let config = match self.current.remove(&sled_id) {
                    Some(mut config) => {
                        // Bump generation number for any sled whose
                        // DatasetsConfig changed
                        if self.changed.contains(&sled_id) {
                            config.generation = config.generation.next()
                        }
                        config
                    }
                    None => empty_blueprint_datasets_config(),
                };
                (sled_id, config)
            })
            .collect()
    }
}

#[derive(Debug)]
pub(super) struct SledDatasetsEditor<'a> {
    rng: &'a mut PlannerRng,
    blueprint_dataset_ids:
        BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, DatasetUuid>>,
    database_dataset_ids:
        BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, DatasetUuid>>,
    config: &'a mut BlueprintDatasetsConfig,
    changed: bool,
    sled_id: SledUuid,
    parent_changed_set: &'a mut BTreeSet<SledUuid>,
}

impl Drop for SledDatasetsEditor<'_> {
    fn drop(&mut self) {
        if self.changed {
            self.parent_changed_set.insert(self.sled_id);
        }
    }
}

impl<'a> SledDatasetsEditor<'a> {
    fn new(
        rng: &'a mut PlannerRng,
        database_dataset_ids: BTreeMap<
            ZpoolUuid,
            BTreeMap<DatasetKind, DatasetUuid>,
        >,
        sled_id: SledUuid,
        config: &'a mut BlueprintDatasetsConfig,
        parent_changed_set: &'a mut BTreeSet<SledUuid>,
    ) -> Result<Self, BlueprintDatasetsEditError> {
        let blueprint_dataset_ids = build_dataset_kind_id_map(
            "parent blueprint",
            config.datasets.values().map(|dataset| {
                (dataset.pool.id(), dataset.kind.clone(), dataset.id)
            }),
        )?;
        Ok(Self {
            rng,
            blueprint_dataset_ids,
            database_dataset_ids,
            config,
            changed: false,
            sled_id,
            parent_changed_set,
        })
    }

    pub fn database_dataset_ids(
        &self,
    ) -> &BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, DatasetUuid>> {
        &self.database_dataset_ids
    }

    pub fn datasets(&self) -> impl Iterator<Item = &BlueprintDatasetConfig> {
        self.config.datasets.values()
    }

    pub fn expunge_datasets_if<F>(&mut self, mut expunge_if: F) -> usize
    where
        F: FnMut(&BlueprintDatasetConfig) -> bool,
    {
        let mut num_expunged = 0;

        for dataset in self.config.datasets.values_mut() {
            match dataset.disposition {
                // Already expunged; ignore
                BlueprintDatasetDisposition::Expunged => continue,
                // Potentially expungeable
                BlueprintDatasetDisposition::InService => (),
            }
            if expunge_if(&*dataset) {
                dataset.disposition = BlueprintDatasetDisposition::Expunged;
                num_expunged += 1;
                self.changed = true;
            }
        }

        num_expunged
    }

    pub fn ensure_debug_dataset(
        &mut self,
        zpool: ZpoolName,
    ) -> (DatasetUuid, Ensure) {
        const DEBUG_QUOTA_SIZE_GB: u32 = 100;

        let address = None;
        let quota = Some(ByteCount::from_gibibytes_u32(DEBUG_QUOTA_SIZE_GB));
        let reservation = None;

        self.ensure_dataset(
            DatasetName::new(zpool, DatasetKind::Debug),
            address,
            quota,
            reservation,
            CompressionAlgorithm::GzipN { level: GzipLevel::new::<9>() },
        )
    }

    pub fn ensure_zone_root_dataset(
        &mut self,
        zpool: ZpoolName,
    ) -> (DatasetUuid, Ensure) {
        let address = None;
        let quota = None;
        let reservation = None;

        self.ensure_dataset(
            DatasetName::new(zpool, DatasetKind::TransientZoneRoot),
            address,
            quota,
            reservation,
            CompressionAlgorithm::Off,
        )
    }

    /// Ensures a dataset exists on this sled.
    ///
    /// - If the dataset exists in the blueprint already, use it.
    /// - Otherwise, if the dataset exists in the database, re-use the UUID, but
    ///   add it to the blueprint.
    /// - Otherwse, create a new dataset in the blueprint, which will propagate
    ///   to the database during execution.
    pub fn ensure_dataset(
        &mut self,
        dataset: DatasetName,
        address: Option<SocketAddrV6>,
        quota: Option<ByteCount>,
        reservation: Option<ByteCount>,
        compression: CompressionAlgorithm,
    ) -> (DatasetUuid, Ensure) {
        let zpool_id = dataset.pool().id();
        let kind = dataset.dataset();

        let make_config = |id: DatasetUuid| BlueprintDatasetConfig {
            disposition: BlueprintDatasetDisposition::InService,
            id,
            pool: dataset.pool().clone(),
            kind: kind.clone(),
            address,
            quota,
            reservation,
            compression,
        };

        // Is this dataset already in the blueprint? If so, update it if it's
        // changed.
        if let Some(existing_id) = self
            .blueprint_dataset_ids
            .get(&zpool_id)
            .and_then(|kind_to_id| kind_to_id.get(kind))
        {
            // We built `self.blueprint_dataset_ids` based on the contents of
            // `self.config.datasets`, so we know we can unwrap this `get_mut`.
            let old_config = self.config.datasets.get_mut(existing_id).expect(
                "internal inconsistency: \
                 entry in blueprint_dataset_ids but not current",
            );
            let new_config = make_config(*existing_id);

            let ensure = if new_config != *old_config {
                *old_config = new_config;
                self.changed = true;
                Ensure::Updated
            } else {
                Ensure::NotNeeded
            };

            return (old_config.id, ensure);
        }

        // Is there a dataset ID matching this one in the database? If so, use
        // that.
        //
        // TODO(https://github.com/oxidecomputer/omicron/issues/6645): We
        // could avoid reading from the datastore if we were confident all
        // provisioned datasets existed in the parent blueprint.
        let id = self
            .database_dataset_ids
            .get(&zpool_id)
            .and_then(|kind_to_id| kind_to_id.get(kind))
            .copied()
            .unwrap_or_else(|| self.rng.next_dataset());

        self.config.datasets.insert(id, make_config(id));
        self.changed = true;

        // We updated our config, so also record this ID in our "present in
        // the blueprint" map. We know the entry doesn't exist or we would have
        // found it when we checked above.
        self.blueprint_dataset_ids
            .entry(zpool_id)
            .or_default()
            .insert(kind.clone(), id);

        (id, Ensure::Added)
    }
}

fn build_dataset_kind_id_map(
    data_source: &'static str,
    iter: impl Iterator<Item = (ZpoolUuid, DatasetKind, DatasetUuid)>,
) -> Result<
    BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, DatasetUuid>>,
    BlueprintDatasetsEditError,
> {
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
                return Err(
                    BlueprintDatasetsEditError::MultipleDatasetsOfKind {
                        data_source,
                        zpool_id,
                        kind: prev.key().clone(),
                        id1: *prev.get(),
                        id2: dataset_id,
                    },
                );
            }
        }
    }
    Ok(kind_id_map)
}

fn empty_blueprint_datasets_config() -> BlueprintDatasetsConfig {
    BlueprintDatasetsConfig {
        generation: Generation::new(),
        datasets: BTreeMap::new(),
    }
}
