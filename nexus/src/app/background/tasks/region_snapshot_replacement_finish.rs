// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting when a region snapshot replacement has all its
//! steps done, and finishing it.
//!
//! Once all related region snapshot replacement steps are done, the region
//! snapshot replacement can be completed.

use crate::app::authn;
use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas;
use crate::app::sagas::region_snapshot_replacement_finish::*;
use crate::app::sagas::NexusSaga;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::RegionSnapshotReplacement;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::RegionSnapshotReplacementFinishStatus;
use serde_json::json;
use std::sync::Arc;

pub struct RegionSnapshotReplacementFinishDetector {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
}

impl RegionSnapshotReplacementFinishDetector {
    pub fn new(datastore: Arc<DataStore>, sagas: Arc<dyn StartSaga>) -> Self {
        RegionSnapshotReplacementFinishDetector { datastore, sagas }
    }

    async fn send_finish_request(
        &self,
        opctx: &OpContext,
        request: RegionSnapshotReplacement,
    ) -> Result<(), omicron_common::api::external::Error> {
        let params = sagas::region_snapshot_replacement_finish::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            request,
        };

        let saga_dag = SagaRegionSnapshotReplacementFinish::prepare(&params)?;

        // We only care that the saga was started, and don't wish to wait for it
        // to complete, so use `StartSaga::saga_start`, rather than `saga_run`.
        self.sagas.saga_start(saga_dag).await?;

        Ok(())
    }

    async fn transition_requests_to_done(
        &self,
        opctx: &OpContext,
        status: &mut RegionSnapshotReplacementFinishStatus,
    ) {
        let log = &opctx.log;

        // Find all region snapshot replacement requests in state "Running"
        let requests = match self
            .datastore
            .get_running_region_snapshot_replacements(opctx)
            .await
        {
            Ok(requests) => requests,

            Err(e) => {
                let s = format!(
                    "query for region snapshot replacement requests \
                        failed: {e}",
                );
                error!(&log, "{s}");
                status.errors.push(s);

                return;
            }
        };

        for request in requests {
            // Count associated region snapshot replacement steps that are not
            // completed.
            let count = match self
                .datastore
                .in_progress_region_snapshot_replacement_steps(
                    opctx, request.id,
                )
                .await
            {
                Ok(count) => count,

                Err(e) => {
                    let s = format!(
                        "counting incomplete region snapshot replacement \
                        steps failed: {e}",
                    );
                    error!(&log, "{s}");
                    status.errors.push(s);

                    continue;
                }
            };

            if count == 0 {
                // If the region snapshot or read-only region has been deleted,
                // then the snapshot replacement is done: the reference number
                // went to zero and it was deleted, therefore there aren't any
                // volumes left that reference it!

                let request_id = request.id;

                match self.datastore.read_only_target_deleted(&request).await {
                    Ok(true) => {
                        // gone!
                    }

                    Ok(false) => {
                        // not deleted yet
                        info!(
                            &log,
                            "read-only target still exists";
                            "request_id" => %request_id,
                        );
                        continue;
                    }

                    Err(e) => {
                        let s = format!(
                            "error querying for read-only target deletion: {e}",
                        );
                        error!(
                            &log,
                            "{s}";
                            "request_id" => %request_id,
                        );
                        status.errors.push(s);
                        continue;
                    }
                }

                match self.send_finish_request(opctx, request).await {
                    Ok(()) => {
                        let s = format!(
                            "region snapshot replacement finish invoked ok for \
                            {request_id}"
                        );

                        info!(&log, "{s}");
                        status.finish_invoked_ok.push(s);
                    }

                    Err(e) => {
                        let s = format!(
                            "invoking region snapshot replacement finish for \
                            {request_id} failed: {e}",
                        );
                        error!(&log, "{s}");
                        status.errors.push(s);
                    }
                }
            }
        }
    }
}

impl BackgroundTask for RegionSnapshotReplacementFinishDetector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            let mut status = RegionSnapshotReplacementFinishStatus::default();

            self.transition_requests_to_done(opctx, &mut status).await;

            json!(status)
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use nexus_db_model::RegionSnapshotReplacement;
    use nexus_db_model::RegionSnapshotReplacementStep;
    use nexus_db_model::RegionSnapshotReplacementStepState;
    use nexus_db_queries::db::datastore::region_snapshot_replacement;
    use nexus_db_queries::db::datastore::NewRegionVolumeId;
    use nexus_db_queries::db::datastore::OldSnapshotVolumeId;
    use nexus_test_utils_macros::nexus_test;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::VolumeUuid;
    use sled_agent_client::VolumeConstructionRequest;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_done_region_snapshot_replacement_causes_finish(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let mut task = RegionSnapshotReplacementFinishDetector::new(
            datastore.clone(),
            nexus.sagas.clone(),
        );

        // Noop test
        let result: RegionSnapshotReplacementFinishStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(result, RegionSnapshotReplacementFinishStatus::default());

        // Add a region snapshot replacement request for a fake region snapshot.

        let dataset_id = DatasetUuid::new_v4();
        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        // Do not add the fake region snapshot to the database, as it should
        // have been deleted by the time the request transitions to "Running"

        let request = RegionSnapshotReplacement::new_from_region_snapshot(
            dataset_id,
            region_id,
            snapshot_id,
        );

        let request_id = request.id;

        let volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(nexus_db_model::Volume::new(
                volume_id,
                serde_json::to_string(&VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                })
                .unwrap(),
            ))
            .await
            .unwrap();

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx, request, volume_id,
            )
            .await
            .unwrap();

        // Transition that to Allocating -> ReplacementDone -> DeletingOldVolume
        // -> Running

        let operating_saga_id = Uuid::new_v4();

        datastore
            .set_region_snapshot_replacement_allocating(
                &opctx,
                request_id,
                operating_saga_id,
            )
            .await
            .unwrap();

        let new_region_id = Uuid::new_v4();
        let new_region_volume_id = VolumeUuid::new_v4();
        let old_snapshot_volume_id = VolumeUuid::new_v4();

        datastore
            .set_region_snapshot_replacement_replacement_done(
                &opctx,
                request_id,
                operating_saga_id,
                new_region_id,
                NewRegionVolumeId(new_region_volume_id),
                OldSnapshotVolumeId(old_snapshot_volume_id),
            )
            .await
            .unwrap();

        datastore
            .set_region_snapshot_replacement_deleting_old_volume(
                &opctx,
                request_id,
                operating_saga_id,
            )
            .await
            .unwrap();

        datastore
            .set_region_snapshot_replacement_running(
                &opctx,
                request_id,
                operating_saga_id,
            )
            .await
            .unwrap();

        // Insert a few steps, not all finished yet

        let operating_saga_id = Uuid::new_v4();

        let step_volume_id = VolumeUuid::new_v4();
        datastore
            .volume_create(nexus_db_model::Volume::new(
                step_volume_id,
                serde_json::to_string(&VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                })
                .unwrap(),
            ))
            .await
            .unwrap();

        let mut step_1 =
            RegionSnapshotReplacementStep::new(request_id, step_volume_id);
        step_1.replacement_state = RegionSnapshotReplacementStepState::Complete;
        step_1.operating_saga_id = Some(operating_saga_id);
        let step_1_id = step_1.id;

        let step_volume_id = VolumeUuid::new_v4();
        datastore
            .volume_create(nexus_db_model::Volume::new(
                step_volume_id,
                serde_json::to_string(&VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                })
                .unwrap(),
            ))
            .await
            .unwrap();

        let mut step_2 =
            RegionSnapshotReplacementStep::new(request_id, step_volume_id);
        step_2.replacement_state = RegionSnapshotReplacementStepState::Complete;
        step_2.operating_saga_id = Some(operating_saga_id);
        let step_2_id = step_2.id;

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, step_1)
            .await
            .unwrap();

        assert!(matches!(
            result,
            region_snapshot_replacement::InsertStepResult::Inserted { .. }
        ));

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, step_2)
            .await
            .unwrap();

        assert!(matches!(
            result,
            region_snapshot_replacement::InsertStepResult::Inserted { .. }
        ));

        // Activate the task, it should do nothing yet

        let result: RegionSnapshotReplacementFinishStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(result, RegionSnapshotReplacementFinishStatus::default());

        // Transition one record to Complete, the task should still do nothing

        datastore
            .set_region_snapshot_replacement_step_volume_deleted(
                &opctx, step_1_id,
            )
            .await
            .unwrap();

        let result: RegionSnapshotReplacementFinishStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(result, RegionSnapshotReplacementFinishStatus::default());

        // Transition the other record to Complete

        datastore
            .set_region_snapshot_replacement_step_volume_deleted(
                &opctx, step_2_id,
            )
            .await
            .unwrap();

        // Activate the task - it should pick the request up, change the state,
        // and try to run the region snapshot replacement finish saga
        let result: RegionSnapshotReplacementFinishStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        assert_eq!(
            result,
            RegionSnapshotReplacementFinishStatus {
                finish_invoked_ok: vec![format!(
                    "region snapshot replacement finish invoked ok for \
                    {request_id}"
                )],
                errors: vec![],
            },
        );
    }
}
