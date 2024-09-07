// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting when a region snapshot replacement has all its
//! steps done, and finishing it.
//!
//! Once all related region snapshot replacement steps are done, the region
//! snapshot replacement can be completed.

use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::RegionSnapshotReplacementFinishStatus;
use serde_json::json;
use std::sync::Arc;

pub struct RegionSnapshotReplacementFinishDetector {
    datastore: Arc<DataStore>,
}

impl RegionSnapshotReplacementFinishDetector {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        RegionSnapshotReplacementFinishDetector { datastore }
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
                // If the region snapshot has been deleted, then the snapshot
                // replacement is done: the reference number went to zero and it
                // was deleted, therefore there aren't any volumes left that
                // reference it!
                match self
                    .datastore
                    .region_snapshot_get(
                        request.old_dataset_id,
                        request.old_region_id,
                        request.old_snapshot_id,
                    )
                    .await
                {
                    Ok(Some(_)) => {
                        info!(
                            &log,
                            "region snapshot still exists";
                            "request.old_dataset_id" => %request.old_dataset_id,
                            "request.old_region_id" => %request.old_region_id,
                            "request.old_snapshot_id" => %request.old_snapshot_id,
                        );
                        continue;
                    }

                    Ok(None) => {
                        // gone!
                    }

                    Err(e) => {
                        let s = format!(
                            "error querying for region snapshot {} {} {}: {e}",
                            request.old_dataset_id,
                            request.old_region_id,
                            request.old_snapshot_id,
                        );
                        error!(&log, "{s}");
                        status.errors.push(s);

                        continue;
                    }
                };

                // Transition region snapshot replacement to Complete
                match self
                    .datastore
                    .set_region_snapshot_replacement_complete(opctx, request.id)
                    .await
                {
                    Ok(()) => {
                        let s = format!("set request {} to done", request.id);
                        info!(&log, "{s}");
                        status.records_set_to_done.push(s);
                    }

                    Err(e) => {
                        let s = format!(
                            "marking snapshot replacement as done failed: {e}"
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
    use nexus_test_utils_macros::nexus_test;
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

        let mut task =
            RegionSnapshotReplacementFinishDetector::new(datastore.clone());

        // Noop test
        let result: RegionSnapshotReplacementFinishStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(result, RegionSnapshotReplacementFinishStatus::default());

        // Add a region snapshot replacement request for a fake region snapshot.

        let dataset_id = Uuid::new_v4();
        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        // Do not add the fake region snapshot to the database, as it should
        // have been deleted by the time the request transitions to "Running"

        let request =
            RegionSnapshotReplacement::new(dataset_id, region_id, snapshot_id);

        let request_id = request.id;

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx,
                request,
                Uuid::new_v4(),
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
        let old_snapshot_volume_id = Uuid::new_v4();

        datastore
            .set_region_snapshot_replacement_replacement_done(
                &opctx,
                request_id,
                operating_saga_id,
                new_region_id,
                old_snapshot_volume_id,
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

        let mut step_1 =
            RegionSnapshotReplacementStep::new(request_id, Uuid::new_v4());
        step_1.replacement_state = RegionSnapshotReplacementStepState::Complete;
        step_1.operating_saga_id = Some(operating_saga_id);
        let step_1_id = step_1.id;

        let mut step_2 =
            RegionSnapshotReplacementStep::new(request_id, Uuid::new_v4());
        step_2.replacement_state = RegionSnapshotReplacementStepState::Complete;
        step_2.operating_saga_id = Some(operating_saga_id);
        let step_2_id = step_2.id;

        datastore
            .insert_region_snapshot_replacement_step(&opctx, step_1)
            .await
            .unwrap();
        datastore
            .insert_region_snapshot_replacement_step(&opctx, step_2)
            .await
            .unwrap();

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
                records_set_to_done: vec![format!(
                    "set request {request_id} to done"
                )],
                errors: vec![],
            },
        );
    }
}
