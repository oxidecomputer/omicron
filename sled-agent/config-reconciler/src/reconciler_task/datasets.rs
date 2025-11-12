// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module for Omicron datasets.
//!
//! This module does not spawn a separate tokio task: our parent reconciler task
//! owns an [`OmicronDatasets`] and is able to mutate it in place during
//! reconciliation. However, we do need a [`DatasetTaskHandle`] to perform some
//! operations. This handle is shared with other "needs to perform dataset
//! operations" consumers (e.g., inventory requests perform operations to check
//! the live state of datasets directly from ZFS).

use super::CurrentlyManagedZpools;
use crate::dataset_serialization_task::DatasetEnsureError;
use crate::dataset_serialization_task::DatasetEnsureResult;
use crate::dataset_serialization_task::DatasetTaskHandle;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use illumos_utils::zpool::PathInPool;
use illumos_utils::zpool::ZpoolOrRamdisk;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use nexus_sled_agent_shared::inventory::OrphanedDataset;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_uuid_kinds::DatasetUuid;
use sled_storage::config::MountConfig;
use sled_storage::dataset::ZONE_DATASET;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub(super) enum ZoneDatasetDependencyError {
    #[error("zone config is missing a filesystem pool")]
    MissingFilesystemPool,
    #[error(
        "zone's transient root dataset is not available: {}", .0.full_name(),
    )]
    TransientZoneDatasetNotAvailable(DatasetName),
    #[error("zone's durable dataset is not available: {}", .0.full_name())]
    DurableDatasetNotAvailable(DatasetName),
}

#[derive(Debug)]
pub(super) struct OmicronDatasets {
    datasets: IdOrdMap<OmicronDataset>,
    orphaned_datasets: IdOrdMap<OrphanedDataset>,
    dataset_task: DatasetTaskHandle,
}

impl OmicronDatasets {
    #[cfg(test)]
    pub(super) fn with_datasets<I>(datasets: I) -> Self
    where
        I: Iterator<Item = (DatasetConfig, Result<(), DatasetEnsureError>)>,
    {
        let dataset_task = DatasetTaskHandle::spawn_noop();
        let datasets = datasets
            .map(|(config, result)| OmicronDataset {
                config,
                state: match result {
                    Ok(()) => DatasetState::Ensured,
                    Err(err) => DatasetState::FailedToEnsure(Arc::new(err)),
                },
            })
            .collect();
        Self { datasets, orphaned_datasets: IdOrdMap::new(), dataset_task }
    }

    pub(super) fn new(dataset_task: DatasetTaskHandle) -> Self {
        Self {
            datasets: IdOrdMap::default(),
            orphaned_datasets: IdOrdMap::new(),
            dataset_task,
        }
    }

    /// Confirm that any dataset dependencies of `zone` have been ensured
    /// successfully, returning the path for the zone's filesystem root.
    pub(super) fn validate_zone_storage(
        &self,
        zone: &OmicronZoneConfig,
        mount_config: &MountConfig,
    ) -> Result<PathInPool, ZoneDatasetDependencyError> {
        // Confirm that there's an ensured `TransientZoneRoot` dataset on this
        // zone's filesystem pool.
        let Some(filesystem_pool) = zone.filesystem_pool else {
            // This should never happen: Reconfigurator guarantees all zones
            // have filesystem pools. `filesystem_pool` is non-optional in
            // blueprints; we should make it non-optional in `OmicronZoneConfig`
            // too: https://github.com/oxidecomputer/omicron/issues/8216
            return Err(ZoneDatasetDependencyError::MissingFilesystemPool);
        };

        let transient_dataset_name = DatasetName::new(
            filesystem_pool,
            DatasetKind::TransientZone { name: zone.zone_name() },
        );

        // TODO-cleanup It would be nicer if the zone included the filesystem
        // dataset ID, so we could just do a lookup here instead of searching.
        // https://github.com/oxidecomputer/omicron/issues/7214
        if !self.datasets.iter().any(|d| {
            matches!(d.state, DatasetState::Ensured)
                && d.config.name == transient_dataset_name
        }) {
            return Err(
                ZoneDatasetDependencyError::TransientZoneDatasetNotAvailable(
                    transient_dataset_name,
                ),
            );
        }

        let zone_root_path = PathInPool {
            pool: ZpoolOrRamdisk::Zpool(filesystem_pool),
            // TODO-cleanup Should we get this path from the dataset we found
            // above?
            path: filesystem_pool
                .dataset_mountpoint(&mount_config.root, ZONE_DATASET),
        };

        // Confirm that the durable dataset for this zone has been ensured, if
        // it has one.
        let Some(durable_dataset) = zone.dataset_name() else {
            return Ok(zone_root_path);
        };

        // TODO-cleanup As above, if we had an ID we could look up instead of
        // searching.
        if !self.datasets.iter().any(|d| {
            matches!(d.state, DatasetState::Ensured)
                && d.config.name == durable_dataset
        }) {
            return Err(
                ZoneDatasetDependencyError::DurableDatasetNotAvailable(
                    durable_dataset,
                ),
            );
        }

        Ok(zone_root_path)
    }

    pub(super) async fn remove_datasets_if_needed(
        &mut self,
        datasets: &IdOrdMap<DatasetConfig>,
        currently_managed_zpools: Arc<CurrentlyManagedZpools>,
        log: &Logger,
    ) {
        let mut datasets_to_remove = Vec::new();

        // Make a pass through our in-memory dataset states:
        //
        // * Remember any that are no longer present in `datasets`
        // * Mark any whose parent zpool has disappeared as failed, unless
        //   they're already in a failed state.
        for mut dataset in &mut self.datasets {
            if !datasets.contains_key(&dataset.config.id) {
                datasets_to_remove.push(dataset.config.id);
                continue;
            }

            match &dataset.state {
                DatasetState::Ensured => {
                    if !currently_managed_zpools
                        .contains(dataset.config.name.pool())
                    {
                        dataset.state = DatasetState::FailedToEnsure(Arc::new(
                            DatasetEnsureError::ZpoolNotFound(
                                *dataset.config.name.pool(),
                            ),
                        ));
                    }
                }
                DatasetState::FailedToEnsure(_) => (),
            }
        }

        // Actually remove the gone-from-config datasets
        for dataset_id in datasets_to_remove {
            self.datasets.remove(&dataset_id);
        }

        // Check against the filesystem for any orphaned datasets and attempt to
        // destroy them.
        match self
            .dataset_task
            .datasets_destroy_orphans(
                datasets.clone(),
                currently_managed_zpools,
            )
            .await
        {
            Ok(Ok(orphaned)) => {
                // Accumulate into our set of orphaned datasets. We never remove
                // entries from this set for monitoring omicron#6177: we want to
                // report any datasets we _might have deleted_ at any point.
                // Once we start actually deleting, we should remove this
                // accumulation and only report the currently-orphaned datasets.
                let mut newly_orphaned = 0_usize;
                for orphan in orphaned {
                    if self.orphaned_datasets.insert_overwrite(orphan).is_none()
                    {
                        newly_orphaned += 1;
                    }
                }
                if newly_orphaned > 0 {
                    info!(
                        log,
                        "found {newly_orphaned} newly-orphaned datasets"
                    );
                }
            }
            Ok(Err(err)) => {
                warn!(
                    log,
                    "failed to check for orphaned datasets";
                    InlineErrorChain::new(err.as_ref()),
                );
            }
            Err(err) => {
                warn!(
                    log, "failed to contact dataset task";
                    InlineErrorChain::new(&err),
                );
            }
        }
    }

    pub(super) async fn ensure_datasets_if_needed(
        &mut self,
        datasets: IdOrdMap<DatasetConfig>,
        currently_managed_zpools: Arc<CurrentlyManagedZpools>,
        log: &Logger,
    ) {
        let results = match self
            .dataset_task
            .datasets_ensure(datasets, currently_managed_zpools)
            .await
        {
            Ok(results) => results,
            Err(err) => {
                // If we can't contact the dataset task, we leave
                // `self.datasets` untouched (i.e., reuse whatever datasets we
                // have from the last time we successfully contacted the dataset
                // task).
                warn!(
                    log, "failed to contact dataset task";
                    InlineErrorChain::new(&err),
                );
                return;
            }
        };

        for DatasetEnsureResult { config, result } in results {
            let state = match result {
                Ok(()) => DatasetState::Ensured,
                Err(err) => DatasetState::FailedToEnsure(err),
            };
            self.datasets.insert_overwrite(OmicronDataset { config, state });
        }
    }

    pub(super) fn has_retryable_error(&self) -> bool {
        self.datasets.iter().any(|d| match &d.state {
            DatasetState::Ensured => false,
            DatasetState::FailedToEnsure(err) => err.is_retryable(),
        })
    }

    pub(crate) fn to_inventory(
        &self,
    ) -> BTreeMap<DatasetUuid, ConfigReconcilerInventoryResult> {
        self.datasets
            .iter()
            .map(|dataset| {
                let result = match &dataset.state {
                    DatasetState::Ensured => {
                        ConfigReconcilerInventoryResult::Ok
                    }
                    DatasetState::FailedToEnsure(err) => {
                        ConfigReconcilerInventoryResult::Err {
                            message: InlineErrorChain::new(err).to_string(),
                        }
                    }
                };
                (dataset.config.id, result)
            })
            .collect()
    }

    pub(crate) fn orphaned_datasets(&self) -> &IdOrdMap<OrphanedDataset> {
        &self.orphaned_datasets
    }
}

#[derive(Debug)]
struct OmicronDataset {
    config: DatasetConfig,
    state: DatasetState,
}

impl IdOrdItem for OmicronDataset {
    type Key<'a> = DatasetUuid;

    fn key(&self) -> Self::Key<'_> {
        self.config.id
    }

    id_upcast!();
}

#[derive(Debug)]
enum DatasetState {
    Ensured,
    FailedToEnsure(Arc<DatasetEnsureError>),
}
