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
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use omicron_common::disk::DatasetConfig;
use omicron_uuid_kinds::DatasetUuid;
use slog::Logger;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct OmicronDatasets {
    datasets: IdMap<OmicronDataset>,
    dataset_task: DatasetTaskHandle,
}

impl OmicronDatasets {
    pub(super) fn new(dataset_task: DatasetTaskHandle) -> Self {
        Self { datasets: IdMap::default(), dataset_task }
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
