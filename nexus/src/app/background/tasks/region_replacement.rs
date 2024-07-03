// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting regions that need replacing and beginning that
//! process
//!
//! This task's responsibility is to create region replacement requests when
//! physical disks are expunged, and trigger the region replacement start saga
//! for any requests that are in state "Requested". See the documentation there
//! for more information.

use crate::app::authn;
use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas;
use crate::app::sagas::region_replacement_start::SagaRegionReplacementStart;
use crate::app::sagas::NexusSaga;
use crate::app::RegionAllocationStrategy;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::RegionReplacement;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::TypedUuid;
use serde_json::json;
use std::sync::Arc;

pub struct RegionReplacementDetector {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
}

impl RegionReplacementDetector {
    pub fn new(datastore: Arc<DataStore>, sagas: Arc<dyn StartSaga>) -> Self {
        RegionReplacementDetector { datastore, sagas }
    }

    async fn send_start_request(
        &self,
        serialized_authn: authn::saga::Serialized,
        request: RegionReplacement,
    ) -> Result<(), omicron_common::api::external::Error> {
        let params = sagas::region_replacement_start::Params {
            serialized_authn,
            request,
            allocation_strategy:
                RegionAllocationStrategy::RandomWithDistinctSleds { seed: None },
        };

        let saga_dag = SagaRegionReplacementStart::prepare(&params)?;
        self.sagas.saga_start(saga_dag).await
    }
}

impl BackgroundTask for RegionReplacementDetector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;
            warn!(&log, "region replacement task started");

            let mut ok = 0;
            let mut err = 0;

            // Find regions on expunged physical disks
            let regions_to_be_replaced = match self
                .datastore
                .find_regions_on_expunged_physical_disks(opctx)
                .await
            {
                Ok(regions) => regions,

                Err(e) => {
                    error!(
                        &log,
                        "find_regions_on_expunged_physical_disks failed: {e}"
                    );
                    err += 1;

                    return json!({
                        "region_replacement_started_ok": ok,
                        "region_replacement_started_err": err,
                    });
                }
            };

            // Then create replacement requests for those if one doesn't exist
            // yet.
            for region in regions_to_be_replaced {
                let maybe_request = match self
                    .datastore
                    .lookup_region_replacement_request_by_old_region_id(
                        opctx,
                        TypedUuid::from_untyped_uuid(region.id()),
                    )
                    .await
                {
                    Ok(v) => v,

                    Err(e) => {
                        error!(
                            &log,
                            "error looking for existing region replacement \
                             requests for {}: {e}",
                            region.id(),
                        );
                        continue;
                    }
                };

                if maybe_request.is_none() {
                    match self
                        .datastore
                        .create_region_replacement_request_for_region(
                            opctx, &region,
                        )
                        .await
                    {
                        Ok(request_id) => {
                            info!(
                                &log,
                                "added region replacement request \
                                 {request_id} for {} volume {}",
                                region.id(),
                                region.volume_id(),
                            );
                        }

                        Err(e) => {
                            error!(
                                &log,
                                "error adding region replacement request for \
                                 region {} volume id {}: {e}",
                                region.id(),
                                region.volume_id(),
                            );
                            continue;
                        }
                    }
                }
            }

            // Next, for each region replacement request in state "Requested",
            // run the start saga.
            match self.datastore.get_requested_region_replacements(opctx).await
            {
                Ok(requests) => {
                    for request in requests {
                        let result = self
                            .send_start_request(
                                authn::saga::Serialized::for_opctx(opctx),
                                request,
                            )
                            .await;

                        match result {
                            Ok(()) => {
                                ok += 1;
                            }

                            Err(e) => {
                                error!(
                                    &log,
                                    "sending region replacement start request \
                                     failed: {e}",
                                );
                                err += 1;
                            }
                        };
                    }
                }

                Err(e) => {
                    error!(
                        &log,
                        "query for region replacement requests failed: {e}",
                    );
                }
            }

            warn!(&log, "region replacement task done");

            json!({
                "region_replacement_started_ok": ok,
                "region_replacement_started_err": err,
            })
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::init::test::NoopStartSaga;
    use nexus_db_model::RegionReplacement;
    use nexus_test_utils_macros::nexus_test;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_add_region_replacement_causes_start(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let starter = Arc::new(NoopStartSaga::new());
        let mut task =
            RegionReplacementDetector::new(datastore.clone(), starter.clone());

        // Noop test
        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_started_ok": 0,
                "region_replacement_started_err": 0,
            })
        );

        // Add a region replacement request for a fake region
        let request = RegionReplacement::new(Uuid::new_v4(), Uuid::new_v4());

        datastore
            .insert_region_replacement_request(&opctx, request)
            .await
            .unwrap();

        // Activate the task - it should pick that up and try to run the region
        // replacement start saga
        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_started_ok": 1,
                "region_replacement_started_err": 0,
            })
        );

        assert_eq!(starter.count_reset(), 1);
    }
}
