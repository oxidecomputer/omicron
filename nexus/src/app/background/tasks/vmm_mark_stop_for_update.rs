// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that marks the VMMs on sleds that are evacuating for an
//! update as needing to be stopped.
//!
//! The marker (`stop_for_update_disposition_generation` on the `vmm` table)
//! records the sled's `update_disposition` generation and signals that the VMM
//! must be stopped in order to update its sled. The set of evacuating sleds is
//! read from the `rendezvous_sled_bp_availability` table. See RFD 739 for the
//! full implementation of instance restart during live update.

use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::datastore::VmmBulkMarkStopForUpdateResult;
use nexus_types::internal_api::background::VmmMarkStopForUpdateStatus;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;

pub struct VmmMarkStopForUpdate {
    datastore: Arc<DataStore>,
    tx: watch::Sender<usize>,
}

impl VmmMarkStopForUpdate {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        let (tx, _rx) = watch::channel(0);
        Self { datastore, tx }
    }

    pub fn watcher(&self) -> watch::Receiver<usize> {
        self.tx.subscribe()
    }

    pub(crate) async fn actually_activate(
        &mut self,
        opctx: &OpContext,
    ) -> VmmMarkStopForUpdateStatus {
        let VmmBulkMarkStopForUpdateResult { vmms_marked, batches, batch_size } =
            match self.datastore.vmm_bulk_mark_stop_for_update(opctx).await {
                Ok(result) => result,
                Err(err) => {
                    let err = InlineErrorChain::new(&err);
                    slog::error!(
                        &opctx.log,
                        "failed to mark VMMs to stop for a sled update";
                        &err,
                    );
                    return VmmMarkStopForUpdateStatus {
                        vmms_marked: 0,
                        batches: 0,
                        batch_size: SQL_BATCH_SIZE.get(),
                        error: Some(err.to_string()),
                    };
                }
            };

        if vmms_marked > 0 {
            slog::info!(
                &opctx.log,
                "marked {vmms_marked} VMMs to stop for a sled update";
                "batches" => batches,
                "batch_size" => batch_size.get(),
            );
        } else {
            slog::debug!(
                &opctx.log,
                "no VMMs need to be marked to stop for a sled update";
            );
        }

        // Trigger anything waiting on this task to run.
        self.tx.send_modify(|count| *count = *count + 1);

        VmmMarkStopForUpdateStatus {
            vmms_marked,
            batches,
            batch_size: batch_size.get(),
            error: None,
        }
    }
}

impl BackgroundTask for VmmMarkStopForUpdate {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status = self.actually_activate(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => {
                    json!({ "error": format!("failed to serialize status: {err}") })
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use iddqd::IdOrdMap;
    use nexus_db_model::ActiveSledBpAvailability;
    use nexus_db_model::Generation;
    use nexus_db_model::SledBlueprintAvailabilityInput;
    use nexus_db_model::SledBpAvailabilityState;
    use nexus_db_model::Vmm;
    use nexus_db_model::VmmCpuPlatform;
    use nexus_db_model::VmmState;
    use nexus_db_queries::db::DataStore;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use omicron_generation_kinds::UpdateDispositionGeneration;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::PropolisUuid;
    use omicron_uuid_kinds::SledUuid;
    use uuid::Uuid;

    async fn insert_vmm(
        datastore: &DataStore,
        opctx: &OpContext,
        sled_id: SledUuid,
        state: VmmState,
    ) -> Vmm {
        datastore
            .vmm_insert(
                opctx,
                Vmm {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: Uuid::new_v4(),
                    sled_id: sled_id.into(),
                    propolis_ip: "10.1.9.32".parse().unwrap(),
                    propolis_port: 420.into(),
                    cpu_platform: VmmCpuPlatform::SledDefault,
                    time_state_updated: Utc::now(),
                    generation: Generation::new(),
                    state,
                    failure_reason: None,
                    stop_for_update_disposition_generation: None,
                },
            )
            .await
            .expect("VMM should be inserted")
    }

    async fn get_marker_from_vmm(
        datastore: &DataStore,
        opctx: &OpContext,
        vmm: &Vmm,
    ) -> Option<UpdateDispositionGeneration> {
        datastore
            .vmm_fetch(opctx, &PropolisUuid::from_untyped_uuid(vmm.id))
            .await
            .expect("VMM should be fetched")
            .stop_for_update_disposition_generation
            .map(Into::into)
    }

    #[tokio::test]
    async fn test_vmm_mark_stop_for_update_activation() {
        let logctx =
            dev::test_setup_log("test_vmm_mark_stop_for_update_activation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled_evacuating = SledUuid::new_v4();
        let sled_available = SledUuid::new_v4();
        let generation = UpdateDispositionGeneration::from(1);
        let blueprint_id = BlueprintUuid::new_v4();

        // A stoppable VMM on an evacuating sled: should be marked.
        let should_mark =
            insert_vmm(datastore, opctx, sled_evacuating, VmmState::Running)
                .await;
        // A VMM on the evacuating sled that is not stoppable: should be skipped.
        let already_stopped =
            insert_vmm(datastore, opctx, sled_evacuating, VmmState::Stopped)
                .await;
        // A stoppable VMM on a sled that is not evacuating: should be skipped.
        let other_sled =
            insert_vmm(datastore, opctx, sled_available, VmmState::Running)
                .await;

        datastore
            .rendezvous_sled_bp_availability_write(
                opctx,
                blueprint_id,
                IdOrdMap::from_iter_unique([
                    SledBlueprintAvailabilityInput {
                        sled_id: sled_evacuating,
                        state: SledBpAvailabilityState::Active {
                            availability: ActiveSledBpAvailability::Unavailable,
                            update_disposition_generation: generation,
                        },
                    },
                    SledBlueprintAvailabilityInput {
                        sled_id: sled_available,
                        state: SledBpAvailabilityState::Active {
                            availability: ActiveSledBpAvailability::Available,
                            update_disposition_generation: generation,
                        },
                    },
                ])
                .expect("sled inputs are unique"),
            )
            .await
            .expect("sled availability should be written");

        // Create a new task
        let mut task = VmmMarkStopForUpdate::new(datastore.clone());

        // The first activation marks the single eligible VMM at its sled's
        // update disposition generation.
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.vmms_marked, 1);
        assert!(status.error.is_none());
        assert_eq!(
            get_marker_from_vmm(datastore, opctx, &should_mark).await,
            Some(generation)
        );
        assert_eq!(
            get_marker_from_vmm(datastore, opctx, &already_stopped).await,
            None
        );
        assert_eq!(
            get_marker_from_vmm(datastore, opctx, &other_sled).await,
            None
        );

        // Running again is a no-op: the eligible VMM is already marked.
        let status = task.actually_activate(opctx).await;
        assert_eq!(status.vmms_marked, 0);
        assert!(status.error.is_none());
        assert_eq!(
            get_marker_from_vmm(datastore, opctx, &should_mark).await,
            Some(generation)
        );
        assert_eq!(
            get_marker_from_vmm(datastore, opctx, &already_stopped).await,
            None
        );
        assert_eq!(
            get_marker_from_vmm(datastore, opctx, &other_sled).await,
            None
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
