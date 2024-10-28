// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::blueprint_zones_editor::BlueprintZonesEditor;
use super::disks::BlueprintDisksBuilder;
use crate::blueprint_builder::EnsureMultiple;
use illumos_utils::zpool::ZpoolName;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::deployment::BlueprintDatasetsConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::ZpoolFilter;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use slog::info;
use slog::Logger;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;

#[derive(Debug, thiserror::Error)]
pub enum EditDatasetsError {
    #[error("internal error editing disks: {0} left over expunged datasets")]
    LeftoverExpunges(usize),
    #[error("internal error editing disks: {0} left over updated datasets")]
    LeftoverUpdates(usize),
}

/// Helper for working with sets of datasets on each sled
pub(super) struct BlueprintDatasetsBuilder<'a> {
    changed_datasets: BTreeMap<SledUuid, BlueprintDatasetsConfig>,
    parent_datasets: &'a BTreeMap<SledUuid, BlueprintDatasetsConfig>,
}

impl<'a> BlueprintDatasetsBuilder<'a> {
    pub fn new(parent_blueprint: &'a Blueprint) -> BlueprintDatasetsBuilder {
        BlueprintDatasetsBuilder {
            changed_datasets: BTreeMap::new(),
            parent_datasets: &parent_blueprint.blueprint_datasets,
        }
    }

    /// Ensures that a sled in the blueprint has all the datasets it should.
    ///
    /// We perform the following process to decide what datasets should exist
    /// in the blueprint during the planning phase:
    ///
    /// INPUT                    | OUTPUT
    /// ----------------------------------------------------------------------
    /// zpools in the blueprint  | blueprint datasets for debug, root filesystem
    ///                          | (All zpools should have these datasets)
    /// ----------------------------------------------------------------------
    /// zones in the blueprint   | blueprint datasets for filesystems, durable data
    ///                          | (These datasets are needed for zones)
    /// ----------------------------------------------------------------------
    /// discretionary datasets   | blueprint datasets for discretionary datasets
    /// NOTE: These don't exist, |
    /// at the moment            |
    /// ----------------------------------------------------------------------
    ///
    /// From this process, we should be able to construct "all datasets that
    /// should exist in the new blueprint".
    ///
    /// - If new datasets are proposed, they are added to the blueprint.
    /// - If datasets are changed, they are updated in the blueprint.
    /// - If datasets are not proposed, but they exist in the parent blueprint,
    /// they are expunged.
    pub fn sled_ensure_datasets(
        &mut self,
        sled_id: SledUuid,
        resources: &SledResources,
        disks: &BlueprintDisksBuilder,
        zones_editor: &BlueprintZonesEditor,
        log: &Logger,
    ) -> Result<EnsureMultiple, EditDatasetsError> {
        const DEBUG_QUOTA_SIZE_GB: u32 = 100;

        let (mut additions, mut updates, mut expunges, removals) = {
            let mut datasets_builder = BlueprintSledDatasetsBuilder::new(
                log, sled_id, self, resources,
            );

            // Ensure each zpool has a "Debug" and "Zone Root" dataset.
            let bp_zpools = disks
                .current_sled_disks(sled_id)
                .map(|disk_config| disk_config.pool_id)
                .collect::<Vec<ZpoolUuid>>();
            for zpool_id in bp_zpools {
                let zpool = ZpoolName::new_external(zpool_id);
                let address = None;
                datasets_builder.ensure(
                    DatasetName::new(zpool.clone(), DatasetKind::Debug),
                    address,
                    Some(ByteCount::from_gibibytes_u32(DEBUG_QUOTA_SIZE_GB)),
                    None,
                    CompressionAlgorithm::Off,
                );
                datasets_builder.ensure(
                    DatasetName::new(zpool, DatasetKind::TransientZoneRoot),
                    address,
                    None,
                    None,
                    CompressionAlgorithm::Off,
                );
            }

            // Ensure that datasets needed for zones exist.
            for zone in zones_editor.current_sled_zones(
                sled_id,
                BlueprintZoneFilter::ShouldBeRunning,
            ) {
                // Dataset for transient zone filesystem
                if let Some(fs_zpool) = &zone.filesystem_pool {
                    let name = zone_name(&zone);
                    let address = None;
                    datasets_builder.ensure(
                        DatasetName::new(
                            fs_zpool.clone(),
                            DatasetKind::TransientZone { name },
                        ),
                        address,
                        None,
                        None,
                        CompressionAlgorithm::Off,
                    );
                }

                // Dataset for durable dataset co-located with zone
                if let Some(dataset) = zone.zone_type.durable_dataset() {
                    let zpool = &dataset.dataset.pool_name;
                    let address = match zone.zone_type {
                        BlueprintZoneType::Crucible(
                            blueprint_zone_type::Crucible { address, .. },
                        ) => Some(address),
                        _ => None,
                    };
                    datasets_builder.ensure(
                        DatasetName::new(zpool.clone(), dataset.kind),
                        address,
                        None,
                        None,
                        CompressionAlgorithm::Off,
                    );
                }
            }

            // TODO: Note that we also have datasets in "zone/" for propolis
            // zones, but these are not currently being tracked by blueprints.

            let expunges = datasets_builder.get_expungeable_datasets();
            let removals = datasets_builder.get_removable_datasets();

            let additions = datasets_builder
                .new_datasets
                .into_values()
                .flat_map(|datasets| datasets.into_values().map(|d| (d.id, d)))
                .collect::<BTreeMap<_, _>>();
            let updates = datasets_builder
                .updated_datasets
                .into_values()
                .flat_map(|datasets| {
                    datasets.into_values().map(|dataset| (dataset.id, dataset))
                })
                .collect::<BTreeMap<DatasetUuid, _>>();
            (additions, updates, expunges, removals)
        };

        if additions.is_empty()
            && updates.is_empty()
            && expunges.is_empty()
            && removals.is_empty()
        {
            return Ok(EnsureMultiple::NotNeeded);
        }
        let added = additions.len();
        let updated = updates.len();
        // - When a dataset is expunged, for whatever reason, it is a part of
        // "expunges". This leads to it getting removed from a sled.
        // - When we know that we've safely destroyed all traces of the dataset,
        // it becomes a part of "removals". This means we can remove it from the
        // blueprint.
        let expunged = expunges.len();
        let removed = removals.len();

        let datasets = &mut self.change_sled_datasets(sled_id).datasets;

        // Add all new datasets
        datasets.append(&mut additions);

        for config in datasets.values_mut() {
            // Apply updates
            if let Some(new_config) = updates.remove(&config.id) {
                *config = new_config;
            };

            // Mark unused datasets as expunged.
            //
            // This indicates that the dataset should be removed from the database.
            if expunges.remove(&config.id) {
                config.disposition = BlueprintDatasetDisposition::Expunged;
            }

            // Small optimization -- if no expungement nor updates are left,
            // bail
            if expunges.is_empty() && updates.is_empty() {
                break;
            }
        }

        // These conditions should be dead-code, and arguably could be
        // assertions, but are safety nets to catch programming errors.
        if !expunges.is_empty() {
            return Err(EditDatasetsError::LeftoverExpunges(expunges.len()));
        }
        if !updates.is_empty() {
            return Err(EditDatasetsError::LeftoverUpdates(updates.len()));
        }

        // Remove all datasets that we've finished expunging.
        datasets.retain(|_id, d| {
            if removals.contains(&d.id) {
                debug_assert_eq!(
                    d.disposition,
                    BlueprintDatasetDisposition::Expunged,
                    "Should only remove datasets that are expunged, but dataset {} is {:?}",
                    d.id, d.disposition,
                );
                return false;
            };
            true
        });

        // We sort in the call to "BlueprintDatasetsBuilder::into_datasets_map",
        // so we don't need to sort "datasets" now.
        Ok(EnsureMultiple::Changed { added, updated, expunged, removed })
    }

    fn change_sled_datasets(
        &mut self,
        sled_id: SledUuid,
    ) -> &mut BlueprintDatasetsConfig {
        self.changed_datasets.entry(sled_id).or_insert_with(|| {
            if let Some(old_sled_datasets) = self.parent_datasets.get(&sled_id)
            {
                BlueprintDatasetsConfig {
                    generation: old_sled_datasets.generation.next(),
                    datasets: old_sled_datasets.datasets.clone(),
                }
            } else {
                BlueprintDatasetsConfig {
                    generation: Generation::new(),
                    datasets: BTreeMap::new(),
                }
            }
        })
    }

    /// Iterates over the list of Omicron datasets currently configured for this
    /// sled in the blueprint that's being built
    fn current_sled_datasets(
        &self,
        sled_id: SledUuid,
    ) -> Box<dyn Iterator<Item = &BlueprintDatasetConfig> + '_> {
        if let Some(sled_datasets) = self
            .changed_datasets
            .get(&sled_id)
            .or_else(|| self.parent_datasets.get(&sled_id))
        {
            Box::new(sled_datasets.datasets.values())
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Produces an owned map of datasets for the requested sleds
    pub fn into_datasets_map(
        mut self,
        sled_ids: impl Iterator<Item = SledUuid>,
    ) -> BTreeMap<SledUuid, BlueprintDatasetsConfig> {
        sled_ids
            .map(|sled_id| {
                // Start with self.changed_datasets, which contains entries for any
                // sled whose datasets config is changing in this blueprint.
                let datasets = self
                    .changed_datasets
                    .remove(&sled_id)
                    // If it's not there, use the config from the parent
                    // blueprint.
                    .or_else(|| self.parent_datasets.get(&sled_id).cloned())
                    // If it's not there either, then this must be a new sled
                    // and we haven't added any datasets to it yet.  Use the
                    // standard initial config.
                    .unwrap_or_else(|| BlueprintDatasetsConfig {
                        generation: Generation::new(),
                        datasets: BTreeMap::new(),
                    });

                (sled_id, datasets)
            })
            .collect()
    }
}

/// Helper for working with sets of datasets on a single sled
#[derive(Debug)]
struct BlueprintSledDatasetsBuilder<'a> {
    log: &'a Logger,
    blueprint_datasets:
        BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, &'a BlueprintDatasetConfig>>,
    database_datasets:
        BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, &'a DatasetConfig>>,

    // Datasets which are unchanged from the prior blueprint
    unchanged_datasets:
        BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, BlueprintDatasetConfig>>,
    // Datasets which are new in this blueprint
    new_datasets:
        BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, BlueprintDatasetConfig>>,
    // Datasets which existed in the old blueprint, but which are
    // changing in this one
    updated_datasets:
        BTreeMap<ZpoolUuid, BTreeMap<DatasetKind, BlueprintDatasetConfig>>,
}

impl<'a> BlueprintSledDatasetsBuilder<'a> {
    pub fn new(
        log: &'a Logger,
        sled_id: SledUuid,
        datasets: &'a BlueprintDatasetsBuilder<'_>,
        resources: &'a SledResources,
    ) -> Self {
        // Gather all datasets known to the blueprint
        let mut blueprint_datasets: BTreeMap<
            ZpoolUuid,
            BTreeMap<DatasetKind, &BlueprintDatasetConfig>,
        > = BTreeMap::new();
        for dataset in datasets.current_sled_datasets(sled_id) {
            blueprint_datasets
                .entry(dataset.pool.id())
                .or_default()
                .insert(dataset.kind.clone(), dataset);
        }

        // Gather all datasets known to the database
        let mut database_datasets = BTreeMap::new();
        for (zpool, datasets) in resources.all_datasets(ZpoolFilter::InService)
        {
            let datasets_by_kind = datasets
                .into_iter()
                .map(|dataset| (dataset.name.dataset().clone(), dataset))
                .collect();

            database_datasets.insert(*zpool, datasets_by_kind);
        }

        Self {
            log,
            blueprint_datasets,
            database_datasets,
            unchanged_datasets: BTreeMap::new(),
            new_datasets: BTreeMap::new(),
            updated_datasets: BTreeMap::new(),
        }
    }

    /// Attempts to add a dataset to the builder.
    ///
    /// - If the dataset exists in the blueprint already, use it.
    /// - Otherwise, if the dataset exists in the database, re-use the UUID, but
    /// add it to the blueprint.
    /// - Otherwse, create a new dataset in the blueprint, which will propagate
    /// to the database during execution.
    pub fn ensure(
        &mut self,
        dataset: DatasetName,
        address: Option<SocketAddrV6>,
        quota: Option<ByteCount>,
        reservation: Option<ByteCount>,
        compression: CompressionAlgorithm,
    ) {
        let zpool = dataset.pool();
        let zpool_id = zpool.id();
        let kind = dataset.dataset();

        let make_config = |id: DatasetUuid| BlueprintDatasetConfig {
            disposition: BlueprintDatasetDisposition::InService,
            id,
            pool: zpool.clone(),
            kind: kind.clone(),
            address,
            quota,
            reservation,
            compression,
        };

        // This dataset already exists in the blueprint
        if let Some(old_config) = self.get_from_bp(zpool_id, kind) {
            let new_config = make_config(old_config.id);

            // If it needs updating, add it
            let target = if *old_config != new_config {
                &mut self.updated_datasets
            } else {
                &mut self.unchanged_datasets
            };
            target
                .entry(zpool_id)
                .or_default()
                .insert(new_config.kind.clone(), new_config);
            return;
        }

        // If the dataset exists in the datastore, re-use the UUID.
        //
        // TODO(https://github.com/oxidecomputer/omicron/issues/6645): We
        // could avoid reading from the datastore if we were confident all
        // provisioned datasets existed in the parent blueprint.
        let id = if let Some(old_config) = self.get_from_db(zpool_id, kind) {
            old_config.id
        } else {
            DatasetUuid::new_v4()
        };

        let new_config = make_config(id);
        self.new_datasets
            .entry(zpool_id)
            .or_default()
            .insert(new_config.kind.clone(), new_config);
    }

    /// Returns all datasets in the old blueprint that are not planned to be
    /// part of the new blueprint.
    pub fn get_expungeable_datasets(&self) -> BTreeSet<DatasetUuid> {
        let dataset_exists_in =
            |group: &BTreeMap<
                ZpoolUuid,
                BTreeMap<DatasetKind, BlueprintDatasetConfig>,
            >,
             zpool_id: ZpoolUuid,
             dataset_id: DatasetUuid| {
                let Some(datasets) = group.get(&zpool_id) else {
                    return false;
                };

                datasets.values().any(|config| config.id == dataset_id)
            };

        let mut expunges = BTreeSet::new();

        for (zpool_id, datasets) in &self.blueprint_datasets {
            for dataset_config in datasets.values() {
                match dataset_config.disposition {
                    // Already expunged; ignore
                    BlueprintDatasetDisposition::Expunged => continue,
                    // Potentially expungeable
                    BlueprintDatasetDisposition::InService => (),
                };

                let dataset_id = dataset_config.id;
                if !dataset_exists_in(&self.new_datasets, *zpool_id, dataset_id)
                    && !dataset_exists_in(
                        &self.updated_datasets,
                        *zpool_id,
                        dataset_id,
                    )
                    && !dataset_exists_in(
                        &self.unchanged_datasets,
                        *zpool_id,
                        dataset_id,
                    )
                {
                    info!(self.log, "dataset expungeable (not needed in blueprint)"; "id" => ?dataset_id);
                    expunges.insert(dataset_id);
                }
            }
        }

        expunges
    }

    /// TODO: <https://github.com/oxidecomputer/omicron/issues/6646>
    /// This function SHOULD do the following:
    ///
    /// Returns all datasets that have been expunged in a prior blueprint, and
    /// which have also been removed from the database and from inventory.
    /// This is our sign that the work of expungement has completed.
    ///
    /// TODO: In reality, however, this function actually implements the
    /// following:
    ///
    /// - It returns an empty BTreeSet, effectively saying "no datasets are
    /// removable from the blueprint".
    pub fn get_removable_datasets(&self) -> BTreeSet<DatasetUuid> {
        let dataset_exists_in =
            |group: &BTreeMap<
                ZpoolUuid,
                BTreeMap<DatasetKind, &DatasetConfig>,
            >,
             zpool_id: ZpoolUuid,
             dataset_id: DatasetUuid| {
                let Some(datasets) = group.get(&zpool_id) else {
                    return false;
                };

                datasets.values().any(|config| config.id == dataset_id)
            };

        let removals = BTreeSet::new();
        for (zpool_id, datasets) in &self.blueprint_datasets {
            for (_kind, config) in datasets {
                if config.disposition == BlueprintDatasetDisposition::Expunged
                    && !dataset_exists_in(
                        &self.database_datasets,
                        *zpool_id,
                        config.id,
                    )
                {
                    info!(self.log, "dataset removable (expunged, not in database)"; "id" => ?config.id);

                    // TODO(https://github.com/oxidecomputer/omicron/issues/6646):
                    // We could call `removals.insert(config.id)` here, but
                    // instead, opt to just log that the dataset is removable
                    // and keep it in the blueprint.
                }
            }
        }
        removals
    }

    fn get_from_bp(
        &self,
        zpool: ZpoolUuid,
        kind: &DatasetKind,
    ) -> Option<&'a BlueprintDatasetConfig> {
        self.blueprint_datasets
            .get(&zpool)
            .and_then(|datasets| datasets.get(kind))
            .copied()
    }

    fn get_from_db(
        &self,
        zpool: ZpoolUuid,
        kind: &DatasetKind,
    ) -> Option<&'a DatasetConfig> {
        self.database_datasets
            .get(&zpool)
            .and_then(|datasets| datasets.get(kind))
            .copied()
    }
}

fn zone_name(zone: &BlueprintZoneConfig) -> String {
    illumos_utils::zone::zone_name(
        zone.zone_type.kind().zone_prefix(),
        Some(zone.id),
    )
}
