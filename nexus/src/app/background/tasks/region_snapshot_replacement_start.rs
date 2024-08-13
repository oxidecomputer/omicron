// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting region snapshots that need replacing and
//! beginning that process
//!
//! This task's responsibility is to create region snapshot replacement requests
//! when physical disks are expunged, and trigger the region snapshot
//! replacement start saga for any requests that are in state "Requested". See
//! the documentation in that saga's docstring for more information.

use crate::app::authn;
use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas;
use crate::app::sagas::region_snapshot_replacement_start::*;
use crate::app::sagas::NexusSaga;
use crate::app::RegionAllocationStrategy;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::RegionSnapshotReplacement;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::RegionSnapshotReplacementStartStatus;
use serde_json::json;
use std::sync::Arc;

pub struct RegionSnapshotReplacementDetector {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
}

impl RegionSnapshotReplacementDetector {
    pub fn new(datastore: Arc<DataStore>, sagas: Arc<dyn StartSaga>) -> Self {
        RegionSnapshotReplacementDetector { datastore, sagas }
    }

    async fn send_start_request(
        &self,
        serialized_authn: authn::saga::Serialized,
        request: RegionSnapshotReplacement,
    ) -> Result<(), omicron_common::api::external::Error> {
        let params = sagas::region_snapshot_replacement_start::Params {
            serialized_authn,
            request,
            allocation_strategy:
                RegionAllocationStrategy::RandomWithDistinctSleds { seed: None },
        };

        let saga_dag = SagaRegionSnapshotReplacementStart::prepare(&params)?;
        self.sagas.saga_start(saga_dag).await
    }

    /// Find region snapshots on expunged physical disks and create region
    /// snapshot replacement requests for them.
    async fn create_requests_for_region_snapshots_on_expunged_disks(
        &self,
        opctx: &OpContext,
        status: &mut RegionSnapshotReplacementStartStatus,
    ) {
        let log = &opctx.log;

        // Find region snapshots on expunged physical disks
        let region_snapshots_to_be_replaced = match self
            .datastore
            .find_region_snapshots_on_expunged_physical_disks(opctx)
            .await
        {
            Ok(region_snapshots) => region_snapshots,

            Err(e) => {
                let s = format!(
                    "find_region_snapshots_on_expunged_physical_disks \
                        failed: {e}",
                );

                error!(&log, "{s}");
                status.errors.push(s);
                return;
            }
        };

        for region_snapshot in region_snapshots_to_be_replaced {
            // If no request exists yet, create one.
            let existing_request = match self
                .datastore
                .lookup_region_snapshot_replacement_request(
                    opctx,
                    &region_snapshot,
                )
                .await
            {
                Ok(existing_request) => existing_request,

                Err(e) => {
                    let s =
                        format!("error looking up replacement request: {e}");

                    error!(
                        &log,
                        "{s}";
                        "snapshot_id" => %region_snapshot.snapshot_id,
                        "region_id" => %region_snapshot.region_id,
                        "dataset_id" => %region_snapshot.dataset_id,
                    );
                    status.errors.push(s);
                    continue;
                }
            };

            if existing_request.is_none() {
                match self
                    .datastore
                    .create_region_snapshot_replacement_request(
                        opctx,
                        &region_snapshot,
                    )
                    .await
                {
                    Ok(request_id) => {
                        let s = format!(
                            "created region snapshot replacement request \
                            {request_id}"
                        );

                        info!(
                            &log,
                            "{s}";
                            "snapshot_id" => %region_snapshot.snapshot_id,
                            "region_id" => %region_snapshot.region_id,
                            "dataset_id" => %region_snapshot.dataset_id,
                        );
                        status.requests_created_ok.push(s);
                    }

                    Err(e) => {
                        let s =
                            format!("error creating replacement request: {e}");

                        error!(
                            &log,
                            "{s}";
                            "snapshot_id" => %region_snapshot.snapshot_id,
                            "region_id" => %region_snapshot.region_id,
                            "dataset_id" => %region_snapshot.dataset_id,
                        );
                        status.errors.push(s);
                    }
                }
            }
        }
    }

    /// For each region snapshot replacement request in state "Requested", run
    /// the start saga.
    async fn start_requested_region_snapshot_replacements(
        &self,
        opctx: &OpContext,
        status: &mut RegionSnapshotReplacementStartStatus,
    ) {
        let log = &opctx.log;

        let requests = match self
            .datastore
            .get_requested_region_snapshot_replacements(opctx)
            .await
        {
            Ok(requests) => requests,

            Err(e) => {
                let s = format!(
                    "query for region snapshot replacement requests failed: {e}"
                );

                error!(&log, "{s}");
                status.errors.push(s);
                return;
            }
        };

        for request in requests {
            let request_id = request.id;

            let result = self
                .send_start_request(
                    authn::saga::Serialized::for_opctx(opctx),
                    request.clone(),
                )
                .await;

            match result {
                Ok(()) => {
                    let s = format!(
                        "region snapshot replacement start invoked ok for \
                        {request_id}"
                    );

                    info!(
                        &log,
                        "{s}";
                        "request.snapshot_id" => %request.old_snapshot_id,
                        "request.region_id" => %request.old_region_id,
                        "request.dataset_id" => %request.old_dataset_id,
                    );
                    status.start_invoked_ok.push(s);
                }

                Err(e) => {
                    let s = format!(
                        "invoking region snapshot replacement start for \
                        {request_id} failed: {e}",
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

impl BackgroundTask for RegionSnapshotReplacementDetector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;
            info!(&log, "region snapshot replacement start task started");

            let mut status = RegionSnapshotReplacementStartStatus::default();

            self.create_requests_for_region_snapshots_on_expunged_disks(
                opctx,
                &mut status,
            )
            .await;

            self.start_requested_region_snapshot_replacements(
                opctx,
                &mut status,
            )
            .await;

            info!(&log, "region snapshot replacement start task done");

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
    use nexus_test_utils_macros::nexus_test;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_add_region_snapshot_replacement_causes_start(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let starter = Arc::new(NoopStartSaga::new());
        let mut task = RegionSnapshotReplacementDetector::new(
            datastore.clone(),
            starter.clone(),
        );

        // Noop test
        let result: RegionSnapshotReplacementStartStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();
        assert_eq!(result, RegionSnapshotReplacementStartStatus::default());
        assert_eq!(starter.count_reset(), 0);

        // Add a region snapshot replacement request for a fake region snapshot

        let request = RegionSnapshotReplacement::new(
            Uuid::new_v4(), // dataset id
            Uuid::new_v4(), // region id
            Uuid::new_v4(), // snapshot id
        );

        let request_id = request.id;

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx,
                request,
                Uuid::new_v4(),
            )
            .await
            .unwrap();

        // Activate the task - it should pick that up and try to run the
        // region snapshot replacement start saga
        let result: RegionSnapshotReplacementStartStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        assert_eq!(
            result,
            RegionSnapshotReplacementStartStatus {
                requests_created_ok: vec![],
                start_invoked_ok: vec![format!(
                    "start invoked ok for {request_id}"
                )],
                errors: vec![],
            },
        );

        assert_eq!(starter.count_reset(), 1);
    }
}
