// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module for Omicron datasets.
//!
//! There is no separate tokio task here; our parent reconciler task owns this
//! set of datasets and is able to mutate it in place during reconciliation.

use crate::dataset_serialization_task::DatasetEnsureError;
use crate::dataset_serialization_task::DatasetEnsureResult;
use crate::dataset_serialization_task::DatasetTaskHandle;
use id_map::IdMap;
use id_map::IdMappable;
use illumos_utils::zpool::PathInPool;
use illumos_utils::zpool::ZpoolOrRamdisk;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_uuid_kinds::DatasetUuid;
use sled_storage::config::MountConfig;
use sled_storage::dataset::ZONE_DATASET;
use slog::Logger;
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
    datasets: IdMap<OmicronDataset>,
    dataset_task: DatasetTaskHandle,
}

impl OmicronDatasets {
    #[cfg(any(test, feature = "testing"))]
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
        Self { datasets, dataset_task }
    }

    pub(super) fn new(dataset_task: DatasetTaskHandle) -> Self {
        Self { datasets: IdMap::default(), dataset_task }
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

        let filesystem_dataset_name = DatasetName::new(
            filesystem_pool,
            DatasetKind::TransientZone { name: zone.zone_name() },
        );

        // TODO-cleanup It would be nicer if the zone included the filesystem
        // dataset ID, so we could just do a lookup here instead of searching.
        // https://github.com/oxidecomputer/omicron/issues/7214
        if !self.datasets.iter().any(|d| {
            matches!(d.state, DatasetState::Ensured)
                && d.config.name == filesystem_dataset_name
        }) {
            return Err(
                ZoneDatasetDependencyError::TransientZoneDatasetNotAvailable(
                    filesystem_dataset_name,
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

        // TODO-correctness Before we moved the logic of this method here, it
        // was performed by
        // `ServiceManager::validate_storage_and_pick_mountpoint()`. That method
        // did not have access to `self.datasets`, so it issued an explicit
        // `zfs` command to check the `zoned`, `canmount`, and `encryption`
        // properties of the durable dataset. Should we:
        //
        // * Do that here
        // * Do nothing here, as the dataset task should have have already done
        //   this
        // * Check the dataset config properties? (All three of the properties
        //   are implied by the dataset kind, so this is probably equivalent to
        //   "do nothing")

        Ok(zone_root_path)
    }

    pub(super) fn remove_datasets_if_needed(
        &mut self,
        datasets: &IdMap<DatasetConfig>,
        log: &Logger,
    ) {
        let mut datasets_to_remove = Vec::new();

        for dataset in &self.datasets {
            if !datasets.contains_key(&dataset.config.id) {
                datasets_to_remove.push(dataset.config.id);
            }
        }

        for dataset_id in datasets_to_remove {
            // TODO We should delete these datasets! (We should also delete any
            // on-disk Omicron datasets that aren't present in `config`).
            //
            // https://github.com/oxidecomputer/omicron/issues/6177
            let dataset = self.datasets.remove(&dataset_id).expect(
                "datasets_to_remove only has existing datasets by construction",
            );
            warn!(
                log, "leaking ZFS dataset (should be deleted: omicron#6177)";
                "id" => %dataset_id,
                "name" => dataset.config.name.full_name(),
            )
        }
    }

    pub(super) async fn ensure_datasets_if_needed(
        &mut self,
        datasets: IdMap<DatasetConfig>,
        log: &Logger,
    ) {
        let results = match self.dataset_task.datasets_ensure(datasets).await {
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
            self.datasets.insert(OmicronDataset { config, state });
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
}

#[derive(Debug)]
struct OmicronDataset {
    config: DatasetConfig,
    state: DatasetState,
}

impl IdMappable for OmicronDataset {
    type Id = DatasetUuid;

    fn id(&self) -> Self::Id {
        self.config.id
    }
}

#[derive(Debug)]
enum DatasetState {
    Ensured,
    FailedToEnsure(Arc<DatasetEnsureError>),
}
