// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for deleting volumes that stash a replaced region snapshot

use crate::app::authn;
use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas;
use crate::app::sagas::region_snapshot_replacement_garbage_collect::*;
use crate::app::sagas::NexusSaga;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::RegionSnapshotReplacement;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::RegionSnapshotReplacementGarbageCollectStatus;
use serde_json::json;
use std::sync::Arc;

pub struct RegionSnapshotReplacementGarbageCollect {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
}

impl RegionSnapshotReplacementGarbageCollect {
    pub fn new(datastore: Arc<DataStore>, sagas: Arc<dyn StartSaga>) -> Self {
        RegionSnapshotReplacementGarbageCollect { datastore, sagas }
    }

    async fn send_garbage_collect_request(
        &self,
        opctx: &OpContext,
        request: RegionSnapshotReplacement,
    ) -> Result<(), omicron_common::api::external::Error> {
        let Some(old_snapshot_volume_id) = request.old_snapshot_volume_id
        else {
            // This state is illegal!
            let s = format!(
                "request {} old snapshot volume id is None!",
                request.id,
            );

            return Err(omicron_common::api::external::Error::internal_error(
                &s,
            ));
        };

        let params =
            sagas::region_snapshot_replacement_garbage_collect::Params {
                serialized_authn: authn::saga::Serialized::for_opctx(opctx),
                old_snapshot_volume_id,
                request,
            };

        let saga_dag =
            SagaRegionSnapshotReplacementGarbageCollect::prepare(&params)?;
        // We only care that the saga was started, and don't wish to wait for it
        // to complete, so use `StartSaga::saga_start`, rather than `saga_run`.
        self.sagas.saga_start(saga_dag).await
    }

    async fn clean_up_region_snapshot_replacement_volumes(
        &self,
        opctx: &OpContext,
        status: &mut RegionSnapshotReplacementGarbageCollectStatus,
    ) {
        let log = &opctx.log;

        let requests = match self
            .datastore
            .get_replacement_done_region_snapshot_replacements(opctx)
            .await
        {
            Ok(requests) => requests,

            Err(e) => {
                let s = format!("querying for requests to collect failed! {e}");
                error!(&log, "{s}");
                status.errors.push(s);
                return;
            }
        };

        for request in requests {
            let request_id = request.id;

            let result =
                self.send_garbage_collect_request(opctx, request.clone()).await;

            match result {
                Ok(()) => {
                    let s = format!(
                        "region snapshot replacement garbage collect request \
                        ok for {request_id}"
                    );

                    info!(
                        &log,
                        "{s}";
                        "request.snapshot_id" => %request.old_snapshot_id,
                        "request.region_id" => %request.old_region_id,
                        "request.dataset_id" => %request.old_dataset_id,
                    );
                    status.garbage_collect_requested.push(s);
                }

                Err(e) => {
                    let s = format!(
                        "sending region snapshot replacement garbage collect \
                        request failed: {e}",
                    );
                    error!(
                        &log,
                        "{s}";
                        "request.snapshot_id" => %request.old_snapshot_id,
                        "request.region_id" => %request.old_region_id,
                        "request.dataset_id" => %request.old_dataset_id,
                    );
                    status.errors.push(s);
                }
            }
        }
    }
}

impl BackgroundTask for RegionSnapshotReplacementGarbageCollect {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            let mut status =
                RegionSnapshotReplacementGarbageCollectStatus::default();

            self.clean_up_region_snapshot_replacement_volumes(
                opctx,
                &mut status,
            )
            .await;

            json!(status)
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::init::test::NoopStartSaga;
    use nexus_db_model::RegionSnapshotReplacement;
    use nexus_db_model::RegionSnapshotReplacementState;
    use nexus_test_utils_macros::nexus_test;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_region_snapshot_replacement_garbage_collect_task(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let starter = Arc::new(NoopStartSaga::new());
        let mut task = RegionSnapshotReplacementGarbageCollect::new(
            datastore.clone(),
            starter.clone(),
        );

        // Noop test
        let result: RegionSnapshotReplacementGarbageCollectStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(
            result,
            RegionSnapshotReplacementGarbageCollectStatus::default()
        );
        assert_eq!(starter.count_reset(), 0);

        // Add two region snapshot requests that need garbage collection

        let mut request = RegionSnapshotReplacement::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        request.replacement_state =
            RegionSnapshotReplacementState::ReplacementDone;
        request.old_snapshot_volume_id = Some(Uuid::new_v4());

        let request_1_id = request.id;

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx,
                request,
                Uuid::new_v4(),
            )
            .await
            .unwrap();

        let mut request = RegionSnapshotReplacement::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        request.replacement_state =
            RegionSnapshotReplacementState::ReplacementDone;
        request.old_snapshot_volume_id = Some(Uuid::new_v4());

        let request_2_id = request.id;

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx,
                request,
                Uuid::new_v4(),
            )
            .await
            .unwrap();

        // Activate the task - it should pick up the two requests

        let result: RegionSnapshotReplacementGarbageCollectStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        for error in &result.errors {
            eprintln!("{error}");
        }

        assert_eq!(result.garbage_collect_requested.len(), 2);

        let s = format!(
            "region snapshot replacement garbage collect request ok for \
            {request_1_id}"
        );
        assert!(result.garbage_collect_requested.contains(&s));

        let s = format!(
            "region snapshot replacement garbage collect request ok for \
            {request_2_id}"
        );
        assert!(result.garbage_collect_requested.contains(&s));

        assert_eq!(result.errors.len(), 0);

        assert_eq!(starter.count_reset(), 2);
    }
}
