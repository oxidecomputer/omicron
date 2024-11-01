// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blueprint_builder::BlueprintZonesBuilder;
use crate::blueprint_builder::EnsureMultiple;
use illumos_utils::zpool::ZpoolName;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::deployment::BlueprintDatasetsConfig;
use nexus_types::deployment::BlueprintPhysicalDiskConfig;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::ZpoolFilter;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_common::disk::CompressionAlgorithm;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid as _;
use omicron_uuid_kinds::PhysicalDiskUuid;
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

pub struct BlueprintDisksEditor<'a> {
    disks: BlueprintDisksBuilder<'a>,
    datasets: BlueprintDatasetsBuilder<'a>,
}

pub type PhysicalDisksMap = BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>;
pub type DatasetsMap = BTreeMap<SledUuid, BlueprintDatasetsConfig>;

impl<'a> BlueprintDisksEditor<'a> {
    pub fn new(parent_blueprint: &'a Blueprint) -> Self {
        Self {
            disks: BlueprintDisksBuilder::new(parent_blueprint),
            datasets: BlueprintDatasetsBuilder::new(parent_blueprint),
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
        zones_builder: &BlueprintZonesBuilder,
        log: &Logger,
    ) -> Result<EnsureMultiple, EditDatasetsError> {
        const DEBUG_QUOTA_SIZE_GB: u32 = 100;

        let (mut additions, mut updates, mut expunges, removals) = {
            let mut datasets_builder = BlueprintSledDatasetsBuilder::new(
                log,
                sled_id,
                &self.datasets,
                resources,
            );

            // Ensure each zpool has a "Debug" and "Zone Root" dataset.
            let bp_zpools = self
                .disks
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
            for (zone, _) in zones_builder.current_sled_zones(
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

        let datasets =
            &mut self.datasets.change_sled_datasets(sled_id).datasets;

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

    /// Ensures that the blueprint contains disks for a sled which already
    /// exists in the database.
    ///
    /// This operation must perform the following:
    /// - Ensure that any disks / zpools that exist in the database
    ///   are propagated into the blueprint.
    /// - Ensure that any disks that are expunged from the database are
    ///   removed from the blueprint.
    pub fn sled_ensure_disks(
        &mut self,
        sled_id: SledUuid,
        resources: &SledResources,
    ) -> EnsureMultiple {
        let (mut additions, removals) = {
            // These are the disks known to our (last?) blueprint
            let blueprint_disks: BTreeMap<_, _> = self
                .disks
                .current_sled_disks(sled_id)
                .map(|disk| {
                    (PhysicalDiskUuid::from_untyped_uuid(disk.id), disk)
                })
                .collect();

            // These are the in-service disks as we observed them in the database,
            // during the planning phase
            let database_disks: BTreeMap<_, _> = resources
                .all_disks(DiskFilter::InService)
                .map(|(zpool, disk)| (disk.disk_id, (zpool, disk)))
                .collect();

            // Add any disks that appear in the database, but not the blueprint
            let additions = database_disks
                .iter()
                .filter_map(|(disk_id, (zpool, disk))| {
                    if !blueprint_disks.contains_key(disk_id) {
                        Some(BlueprintPhysicalDiskConfig {
                            identity: disk.disk_identity.clone(),
                            id: disk_id.into_untyped_uuid(),
                            pool_id: **zpool,
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<BlueprintPhysicalDiskConfig>>();

            // Remove any disks that appear in the blueprint, but not the database
            let removals: BTreeSet<PhysicalDiskUuid> = blueprint_disks
                .keys()
                .filter_map(|disk_id| {
                    if !database_disks.contains_key(disk_id) {
                        Some(*disk_id)
                    } else {
                        None
                    }
                })
                .collect();

            (additions, removals)
        };

        if additions.is_empty() && removals.is_empty() {
            return EnsureMultiple::NotNeeded;
        }
        let added = additions.len();
        let removed = removals.len();

        let disks = &mut self.disks.change_sled_disks(sled_id).disks;

        disks.append(&mut additions);
        disks.retain(|config| {
            !removals.contains(&PhysicalDiskUuid::from_untyped_uuid(config.id))
        });

        EnsureMultiple::Changed { added, updated: 0, expunged: 0, removed }
    }

    pub fn build(
        self,
        sled_ids: impl Iterator<Item = SledUuid> + Clone,
    ) -> (PhysicalDisksMap, DatasetsMap) {
        (
            self.disks.into_disks_map(sled_ids.clone()),
            self.datasets.into_datasets_map(sled_ids),
        )
    }
}

/// Helper for working with sets of disks on each sled
///
/// Tracking the set of disks is slightly non-trivial because we need to
/// bump the per-sled generation number iff the disks are changed.  So
/// we need to keep track of whether we've changed the disks relative
/// to the parent blueprint.  We do this by keeping a copy of any
/// [`BlueprintPhysicalDisksConfig`] that we've changed and a _reference_ to
/// the parent blueprint's disks.  This struct makes it easy for callers iterate
/// over the right set of disks.
struct BlueprintDisksBuilder<'a> {
    changed_disks: BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
    parent_disks: &'a BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,
}

impl<'a> BlueprintDisksBuilder<'a> {
    fn new(parent_blueprint: &'a Blueprint) -> Self {
        Self {
            changed_disks: BTreeMap::new(),
            parent_disks: &parent_blueprint.blueprint_disks,
        }
    }

    /// Returns a mutable reference to a sled's Omicron disks *because* we're
    /// going to change them.
    ///
    /// Unlike [`BlueprintZonesBuilder::change_sled_zones`], it is essential
    /// that the caller _does_ change them, because constructing this bumps the
    /// generation number unconditionally.
    fn change_sled_disks(
        &mut self,
        sled_id: SledUuid,
    ) -> &mut BlueprintPhysicalDisksConfig {
        self.changed_disks.entry(sled_id).or_insert_with(|| {
            if let Some(old_sled_disks) = self.parent_disks.get(&sled_id) {
                BlueprintPhysicalDisksConfig {
                    generation: old_sled_disks.generation.next(),
                    disks: old_sled_disks.disks.clone(),
                }
            } else {
                // No requests have been sent to the disk previously,
                // we should be able to use the first generation.
                BlueprintPhysicalDisksConfig {
                    generation: Generation::new(),
                    disks: vec![],
                }
            }
        })
    }

    /// Iterates over the list of Omicron disks currently configured for this
    /// sled in the blueprint that's being built
    fn current_sled_disks(
        &self,
        sled_id: SledUuid,
    ) -> Box<dyn Iterator<Item = &BlueprintPhysicalDiskConfig> + '_> {
        if let Some(sled_disks) = self
            .changed_disks
            .get(&sled_id)
            .or_else(|| self.parent_disks.get(&sled_id))
        {
            Box::new(sled_disks.disks.iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Produces an owned map of disks for the requested sleds
    fn into_disks_map(
        mut self,
        sled_ids: impl Iterator<Item = SledUuid>,
    ) -> PhysicalDisksMap {
        sled_ids
            .map(|sled_id| {
                // Start with self.changed_disks, which contains entries for any
                // sled whose disks config is changing in this blueprint.
                let mut disks = self
                    .changed_disks
                    .remove(&sled_id)
                    // If it's not there, use the config from the parent
                    // blueprint.
                    .or_else(|| self.parent_disks.get(&sled_id).cloned())
                    // If it's not there either, then this must be a new sled
                    // and we haven't added any disks to it yet.  Use the
                    // standard initial config.
                    .unwrap_or_else(|| BlueprintPhysicalDisksConfig {
                        generation: Generation::new(),
                        disks: vec![],
                    });
                disks.disks.sort_unstable_by_key(|d| d.id);

                (sled_id, disks)
            })
            .collect()
    }
}

/// Helper for working with sets of datasets on each sled
struct BlueprintDatasetsBuilder<'a> {
    changed_datasets: BTreeMap<SledUuid, BlueprintDatasetsConfig>,
    parent_datasets: &'a BTreeMap<SledUuid, BlueprintDatasetsConfig>,
}

impl<'a> BlueprintDatasetsBuilder<'a> {
    fn new(parent_blueprint: &'a Blueprint) -> BlueprintDatasetsBuilder {
        BlueprintDatasetsBuilder {
            changed_datasets: BTreeMap::new(),
            parent_datasets: &parent_blueprint.blueprint_datasets,
        }
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
    fn into_datasets_map(
        mut self,
        sled_ids: impl Iterator<Item = SledUuid>,
    ) -> DatasetsMap {
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
    fn new(
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
    fn ensure(
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
    fn get_expungeable_datasets(&self) -> BTreeSet<DatasetUuid> {
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
    fn get_removable_datasets(&self) -> BTreeSet<DatasetUuid> {
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

pub(crate) fn zone_name(zone: &BlueprintZoneConfig) -> String {
    illumos_utils::zone::zone_name(
        zone.zone_type.kind().zone_prefix(),
        Some(zone.id),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blueprint_builder::test::verify_blueprint;
    use crate::blueprint_builder::BlueprintBuilder;
    use crate::example::example;
    use crate::example::ExampleSystemBuilder;
    use crate::system::SledBuilder;
    use nexus_types::deployment::SledFilter;
    use omicron_test_utils::dev::test_setup_log;

    #[test]
    fn test_add_physical_disks() {
        static TEST_NAME: &str =
            "blueprint_disks_builder_test_add_physical_disks";
        let logctx = test_setup_log(TEST_NAME);

        // Start with an empty system (sleds with no zones). However, we leave
        // the disks around so that `sled_ensure_disks` can add them.
        let (example, parent) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
                .create_zones(false)
                .create_disks_in_blueprint(false)
                .build();
        let input = example.input;

        {
            // We start empty, and can add a disk
            let mut editor = BlueprintDisksEditor::new(&parent);

            assert!(editor.disks.changed_disks.is_empty());
            // In the parent_disks map, we expect entries to be present for
            // each sled, but not have any disks in them.
            for (sled_id, disks) in editor.disks.parent_disks {
                assert_eq!(
                    disks.disks,
                    Vec::new(),
                    "for sled {}, expected no disks present in parent, \
                     but found some",
                    sled_id
                );
            }

            for (sled_id, sled_resources) in
                input.all_sled_resources(SledFilter::InService)
            {
                assert_eq!(
                    editor.sled_ensure_disks(sled_id, &sled_resources),
                    EnsureMultiple::Changed {
                        added: usize::from(SledBuilder::DEFAULT_NPOOLS),
                        updated: 0,
                        expunged: 0,
                        removed: 0
                    },
                );
            }

            assert!(!editor.disks.changed_disks.is_empty());
            // In the parent_disks map, we expect entries to be present for
            // each sled, but not have any disks in them.
            for (sled_id, disks) in editor.disks.parent_disks {
                assert_eq!(
                    disks.disks,
                    Vec::new(),
                    "for sled {}, expected no disks present in parent, \
                     but found some",
                    sled_id
                );
            }
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_datasets_for_zpools_and_zones() {
        static TEST_NAME: &str =
            "blueprint_datasets_builder_test_datasets_for_zpools_and_zones";
        let logctx = test_setup_log(TEST_NAME);
        let (collection, input, blueprint) = example(&logctx.log, TEST_NAME);

        // Creating the "example" blueprint should already invoke
        // `sled_ensure_datasets`.
        //
        // Verify that it has created the datasets we expect to exist.
        verify_blueprint(&blueprint);

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint,
            &input,
            &collection,
            "test",
        )
        .expect("failed to create builder");
        let mut disks_editor = BlueprintDisksEditor::new(&blueprint);

        // Before we make any modifications, there should be no work to do.
        //
        // If we haven't changed inputs, the output should be the same!
        for (sled_id, resources) in
            input.all_sled_resources(SledFilter::Commissioned)
        {
            let r = disks_editor
                .sled_ensure_datasets(
                    sled_id,
                    resources,
                    &builder.zones,
                    &logctx.log,
                )
                .unwrap();
            assert_eq!(r, EnsureMultiple::NotNeeded);
        }

        // Expunge a zone from the blueprint, observe that the dataset is
        // removed.
        let sled_id = input
            .all_sled_ids(SledFilter::Commissioned)
            .next()
            .expect("at least one sled present");
        let sled_details =
            input.sled_lookup(SledFilter::Commissioned, sled_id).unwrap();
        let crucible_zone_id = builder
            .zones
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
            .find_map(|(zone_config, _)| {
                if zone_config.zone_type.is_crucible() {
                    return Some(zone_config.id);
                }
                None
            })
            .expect("at least one crucible must be present");
        let change = builder.zones.change_sled_zones(sled_id);
        println!("Expunging crucible zone: {crucible_zone_id}");
        change.expunge_zones(BTreeSet::from([crucible_zone_id])).unwrap();

        // In the case of Crucible, we have a durable dataset and a transient
        // zone filesystem, so we expect two datasets to be expunged.
        let r = disks_editor
            .sled_ensure_datasets(
                sled_id,
                &sled_details.resources,
                &builder.zones,
                &logctx.log,
            )
            .unwrap();
        assert_eq!(
            r,
            EnsureMultiple::Changed {
                added: 0,
                updated: 0,
                expunged: 2,
                removed: 0
            }
        );
        // Once the datasets are expunged, no further changes will be proposed.
        let r = disks_editor
            .sled_ensure_datasets(
                sled_id,
                &sled_details.resources,
                &builder.zones,
                &logctx.log,
            )
            .unwrap();
        assert_eq!(r, EnsureMultiple::NotNeeded);

        let blueprint = builder.build(disks_editor);
        verify_blueprint(&blueprint);

        let builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint,
            &input,
            &collection,
            "test",
        )
        .expect("failed to create builder");
        let mut disks_editor = BlueprintDisksEditor::new(&blueprint);

        // While the datasets still exist in the input (effectively, the db) we
        // cannot remove them.
        let r = disks_editor
            .sled_ensure_datasets(
                sled_id,
                &sled_details.resources,
                &builder.zones,
                &logctx.log,
            )
            .unwrap();
        assert_eq!(r, EnsureMultiple::NotNeeded);

        let blueprint = builder.build(disks_editor);
        verify_blueprint(&blueprint);

        // Find the datasets we've expunged in the blueprint
        let expunged_datasets = blueprint
            .blueprint_datasets
            .get(&sled_id)
            .unwrap()
            .datasets
            .values()
            .filter_map(|dataset_config| {
                if dataset_config.disposition
                    == BlueprintDatasetDisposition::Expunged
                {
                    Some(dataset_config.id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        // We saw two datasets being expunged earlier when we called
        // `sled_ensure_datasets` -- validate that this is true when inspecting
        // the blueprint too.
        assert_eq!(expunged_datasets.len(), 2);

        // Remove these two datasets from the input.
        let mut input_builder = input.into_builder();
        let zpools = &mut input_builder
            .sleds_mut()
            .get_mut(&sled_id)
            .unwrap()
            .resources
            .zpools;
        for (_, (_, datasets)) in zpools {
            datasets.retain(|dataset| !expunged_datasets.contains(&dataset.id));
        }
        let input = input_builder.build();

        let builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint,
            &input,
            &collection,
            "test",
        )
        .expect("failed to create builder");
        let mut disks_editor = BlueprintDisksEditor::new(&blueprint);

        // Now, we should see the datasets "removed" from the blueprint, since
        // we no longer need to keep around records of their expungement.
        let sled_details =
            input.sled_lookup(SledFilter::Commissioned, sled_id).unwrap();
        let r = disks_editor
            .sled_ensure_datasets(
                sled_id,
                &sled_details.resources,
                &builder.zones,
                &logctx.log,
            )
            .unwrap();

        // TODO(https://github.com/oxidecomputer/omicron/issues/6646):
        // Because of the workaround for #6646, we don't actually remove
        // datasets yet.
        //
        // In the future, however, we will.
        assert_eq!(r, EnsureMultiple::NotNeeded);

        logctx.cleanup_successful();
    }
}
