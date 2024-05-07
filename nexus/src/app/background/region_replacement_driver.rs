// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for driving region replacement progress
//!
//! Region replacements will have been requested by the
//! `region_replacement_start` saga, but that will not trigger the necessary
//! live repair or reconciliation required on its own: the Volume is left in a
//! degraded state (less than a three way mirror) until either of those complete
//! successfully.
//!
//! For each region replacement request that is in state `Running`, this
//! background task will call a saga that drives that forward: namely, get an
//! Upstairs working on either the repair or reconcilation. If an Upstairs *was*
//! running one of these and for some reason was stopped, start it again.
//!
//! Basically, keep starting either repair or reconcilation until they complete
//! successfully, then "finish" the region replacement.

use super::common::BackgroundTask;
use crate::app::authn;
use crate::app::sagas;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub struct RegionReplacementDriver {
    datastore: Arc<DataStore>,
    saga_request: Sender<sagas::SagaRequest>,
}

impl RegionReplacementDriver {
    pub fn new(
        datastore: Arc<DataStore>,
        saga_request: Sender<sagas::SagaRequest>,
    ) -> Self {
        RegionReplacementDriver { datastore, saga_request }
    }
}

impl BackgroundTask for RegionReplacementDriver {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;
            warn!(&log, "region replacement driver task started");

            let mut ok = 0;
            let mut err = 0;

            // Drive running region replacements forward
            match self.datastore.get_running_region_replacements(opctx).await {
                Ok(requests) => {
                    for request in requests {
                        // If a successful finish notification was received, change the
                        // state here: don't drive requests forward where the replacement
                        // is done.

                        let has_matching_finish_notification =
                            match self.datastore.request_has_matching_successful_finish_notification(opctx, &request).await {
                                Ok(has_matching_finish_notification) => has_matching_finish_notification,

                                Err(e) => {
                                    error!(
                                        &log,
                                        "checking for a finish notification for {} failed: {e}",
                                        request.id,
                                    );

                                    err += 1;

                                    // Nexus may determine the request is
                                    // `ReplacementDone` via the drive saga polling an
                                    // Upstairs, so return false here to invoke that saga.
                                    false
                                }
                            };

                        if has_matching_finish_notification {
                            if let Err(e) = self.datastore.mark_region_replacement_as_done(opctx, request.id).await {
                                error!(
                                    &log,
                                    "error marking {} as ReplacementDone: {e}",
                                    request.id,
                                );

                                err += 1;
                            }
                        } else {
                            // Otherwise attempt to drive the replacement's progress
                            // forward (or determine if it is complete).

                            let result = self.saga_request.send(sagas::SagaRequest::RegionReplacementDrive {
                                params: sagas::region_replacement_drive::Params {
                                    serialized_authn: authn::saga::Serialized::for_opctx(opctx),
                                    request,
                                },
                            }).await;

                            match result {
                                Ok(()) => {
                                    ok += 1;
                                }

                                Err(e) => {
                                    error!(&log, "sending region replacement drive request failed: {e}");
                                    err += 1;
                                }
                            };
                        }
                    }
                }

                Err(e) => {
                    error!(&log, "query for running region replacement requests failed: {e}");
                }
            }

            // Complete region replacements that are done
            match self.datastore.get_done_region_replacements(opctx).await {
                Ok(requests) => {
                    for request in requests {
                        let region = match self.datastore.get_region_optional(request.old_region_id).await {
                            Ok(region) => region,

                            Err(e) => {
                                error!(
                                    &log,
                                    "error getting old region: {e}";
                                    "request" => ?request,
                                );

                                err += 1;

                                continue;
                            }
                        };

                        let Some(region) = region else {
                            // This can occur if this background task is interleaving with
                            // an execution of the finish saga: `get_region` will return
                            // nothing because that execution could have deleted the
                            // volume that pointed to the old region. This is a race, not
                            // an error, continue to the next request.
                            continue;
                        };

                        let result = self.saga_request.send(sagas::SagaRequest::RegionReplacementFinish {
                            params: sagas::region_replacement_finish::Params {
                                serialized_authn: authn::saga::Serialized::for_opctx(opctx),
                                region_volume_id: region.volume_id(),
                                request,
                            },
                        }).await;

                        match result {
                            Ok(()) => {
                                ok += 1;
                            }

                            Err(e) => {
                                error!(&log, "sending region replacement finish request failed: {e}");
                                err += 1;
                            }
                        };
                    }
                }

                Err(e) => {
                    error!(&log, "query for done region replacement requests failed: {e}");
                }
            }

            warn!(&log, "region replacement driver task done");

            json!({
                "region_replacement_driven_ok": ok,
                "region_replacement_driven_err": err,
            })
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use nexus_db_model::Region;
    use nexus_db_model::RegionReplacement;
    use nexus_db_model::RegionReplacementState;
    use nexus_db_model::UpstairsRepairNotification;
    use nexus_db_model::UpstairsRepairNotificationType;
    use nexus_db_model::UpstairsRepairType;
    use nexus_test_utils_macros::nexus_test;
    use omicron_uuid_kinds::DownstairsRegionKind;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::TypedUuid;
    use omicron_uuid_kinds::UpstairsKind;
    use omicron_uuid_kinds::UpstairsRepairKind;
    use omicron_uuid_kinds::UpstairsSessionKind;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_running_region_replacement_causes_drive(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let (saga_request_tx, mut saga_request_rx) = mpsc::channel(1);
        let mut task =
            RegionReplacementDriver::new(datastore.clone(), saga_request_tx);

        // Noop test
        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_driven_ok": 0,
                "region_replacement_driven_err": 0,
            })
        );

        // Add a region replacement request for a fake region, and change it to
        // state Running.
        let region_id = Uuid::new_v4();
        let new_region_id = Uuid::new_v4();
        let volume_id = Uuid::new_v4();

        let request = {
            let mut request = RegionReplacement::new(region_id, volume_id);
            request.replacement_state = RegionReplacementState::Running;
            request.new_region_id = Some(new_region_id);
            request
        };

        datastore
            .insert_region_replacement_request(&opctx, request)
            .await
            .unwrap();

        // Activate the task - it should pick that up and try to run the region
        // replacement drive saga
        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_driven_ok": 1,
                "region_replacement_driven_err": 0,
            })
        );

        let request = saga_request_rx.try_recv().unwrap();

        assert!(matches!(
            request,
            sagas::SagaRequest::RegionReplacementDrive { .. }
        ));
    }

    #[nexus_test(server = crate::Server)]
    async fn test_done_region_replacement_causes_finish(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let (saga_request_tx, mut saga_request_rx) = mpsc::channel(1);
        let mut task =
            RegionReplacementDriver::new(datastore.clone(), saga_request_tx);

        // Noop test
        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_driven_ok": 0,
                "region_replacement_driven_err": 0,
            })
        );

        // Insert some region records
        let old_region = {
            let dataset_id = Uuid::new_v4();
            let volume_id = Uuid::new_v4();
            Region::new(
                dataset_id,
                volume_id,
                512_i64.try_into().unwrap(),
                10,
                10,
            )
        };

        let new_region = {
            let dataset_id = Uuid::new_v4();
            let volume_id = Uuid::new_v4();
            Region::new(
                dataset_id,
                volume_id,
                512_i64.try_into().unwrap(),
                10,
                10,
            )
        };

        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();

            use nexus_db_model::schema::region::dsl;
            diesel::insert_into(dsl::region)
                .values(old_region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();

            diesel::insert_into(dsl::region)
                .values(new_region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();
        }

        // Add a region replacement request for that region, and change it to state
        // ReplacementDone. Set the new_region_id to the region created above.
        let request = {
            let mut request =
                RegionReplacement::new(old_region.id(), old_region.volume_id());
            request.replacement_state = RegionReplacementState::ReplacementDone;
            request.new_region_id = Some(new_region.id());
            request
        };

        datastore
            .insert_region_replacement_request(&opctx, request)
            .await
            .unwrap();

        // Activate the task - it should pick that up and try to run the region
        // replacement finish saga
        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_driven_ok": 1,
                "region_replacement_driven_err": 0,
            })
        );

        let request = saga_request_rx.try_recv().unwrap();

        assert!(matches!(
            request,
            sagas::SagaRequest::RegionReplacementFinish { .. }
        ));
    }

    #[nexus_test(server = crate::Server)]
    async fn test_mark_region_replacement_done_after_notification(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let (saga_request_tx, mut saga_request_rx) = mpsc::channel(1);
        let mut task =
            RegionReplacementDriver::new(datastore.clone(), saga_request_tx);

        // Noop test
        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_driven_ok": 0,
                "region_replacement_driven_err": 0,
            })
        );

        // Insert some region records
        let old_region = {
            let dataset_id = Uuid::new_v4();
            let volume_id = Uuid::new_v4();
            Region::new(
                dataset_id,
                volume_id,
                512_i64.try_into().unwrap(),
                10,
                10,
            )
        };

        let new_region = {
            let dataset_id = Uuid::new_v4();
            let volume_id = Uuid::new_v4();
            Region::new(
                dataset_id,
                volume_id,
                512_i64.try_into().unwrap(),
                10,
                10,
            )
        };

        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();

            use nexus_db_model::schema::region::dsl;
            diesel::insert_into(dsl::region)
                .values(old_region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();

            diesel::insert_into(dsl::region)
                .values(new_region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();
        }

        // Add a region replacement request for that region, and change it to state
        // Running. Set the new_region_id to the region created above.
        let request = {
            let mut request =
                RegionReplacement::new(old_region.id(), old_region.volume_id());
            request.replacement_state = RegionReplacementState::Running;
            request.new_region_id = Some(new_region.id());
            request
        };

        datastore
            .insert_region_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // Activate the task - it should pick that up and try to run the region
        // replacement drive saga
        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_driven_ok": 1,
                "region_replacement_driven_err": 0,
            })
        );

        let saga_request = saga_request_rx.try_recv().unwrap();

        assert!(matches!(
            saga_request,
            sagas::SagaRequest::RegionReplacementDrive { .. }
        ));

        // Now, pretend that an Upstairs sent a notification that it successfully finished
        // a repair

        {
            datastore
                .upstairs_repair_notification(
                    &opctx,
                    UpstairsRepairNotification::new(
                        Utc::now(), // client time
                        TypedUuid::<UpstairsRepairKind>::from_untyped_uuid(
                            Uuid::new_v4(),
                        ),
                        UpstairsRepairType::Live,
                        TypedUuid::<UpstairsKind>::from_untyped_uuid(
                            Uuid::new_v4(),
                        ),
                        TypedUuid::<UpstairsSessionKind>::from_untyped_uuid(
                            Uuid::new_v4(),
                        ),
                        TypedUuid::<DownstairsRegionKind>::from_untyped_uuid(
                            new_region.id(),
                        ), // downstairs that was repaired
                        "[fd00:1122:3344:101::2]:12345".parse().unwrap(),
                        UpstairsRepairNotificationType::Succeeded,
                    ),
                )
                .await
                .unwrap();
        }

        // Activating the task now should
        // 1) switch the state to ReplacementDone
        // 2) start the finish saga

        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_driven_ok": 1,
                "region_replacement_driven_err": 0,
            })
        );

        {
            let request_in_db = datastore
                .get_region_replacement_request(&opctx, request.id)
                .await
                .unwrap();
            assert_eq!(
                request_in_db.replacement_state,
                RegionReplacementState::ReplacementDone
            );
        }

        let saga_request = saga_request_rx.try_recv().unwrap();

        assert!(matches!(
            saga_request,
            sagas::SagaRequest::RegionReplacementFinish { .. }
        ));
    }

    #[nexus_test(server = crate::Server)]
    async fn test_no_mark_region_replacement_done_after_failed_notification(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let (saga_request_tx, mut saga_request_rx) = mpsc::channel(1);
        let mut task =
            RegionReplacementDriver::new(datastore.clone(), saga_request_tx);

        // Noop test
        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_driven_ok": 0,
                "region_replacement_driven_err": 0,
            })
        );

        // Insert some region records
        let old_region = {
            let dataset_id = Uuid::new_v4();
            let volume_id = Uuid::new_v4();
            Region::new(
                dataset_id,
                volume_id,
                512_i64.try_into().unwrap(),
                10,
                10,
            )
        };

        let new_region = {
            let dataset_id = Uuid::new_v4();
            let volume_id = Uuid::new_v4();
            Region::new(
                dataset_id,
                volume_id,
                512_i64.try_into().unwrap(),
                10,
                10,
            )
        };

        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();

            use nexus_db_model::schema::region::dsl;
            diesel::insert_into(dsl::region)
                .values(old_region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();

            diesel::insert_into(dsl::region)
                .values(new_region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();
        }

        // Add a region replacement request for that region, and change it to state
        // Running. Set the new_region_id to the region created above.
        let request = {
            let mut request =
                RegionReplacement::new(old_region.id(), old_region.volume_id());
            request.replacement_state = RegionReplacementState::Running;
            request.new_region_id = Some(new_region.id());
            request
        };

        datastore
            .insert_region_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // Activate the task - it should pick that up and try to run the region
        // replacement drive saga
        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_driven_ok": 1,
                "region_replacement_driven_err": 0,
            })
        );

        let saga_request = saga_request_rx.try_recv().unwrap();

        assert!(matches!(
            saga_request,
            sagas::SagaRequest::RegionReplacementDrive { .. }
        ));

        // Now, pretend that an Upstairs sent a notification that it failed to finish a
        // repair

        {
            datastore
                .upstairs_repair_notification(
                    &opctx,
                    UpstairsRepairNotification::new(
                        Utc::now(), // client time
                        TypedUuid::<UpstairsRepairKind>::from_untyped_uuid(
                            Uuid::new_v4(),
                        ),
                        UpstairsRepairType::Live,
                        TypedUuid::<UpstairsKind>::from_untyped_uuid(
                            Uuid::new_v4(),
                        ),
                        TypedUuid::<UpstairsSessionKind>::from_untyped_uuid(
                            Uuid::new_v4(),
                        ),
                        TypedUuid::<DownstairsRegionKind>::from_untyped_uuid(
                            new_region.id(),
                        ), // downstairs that was repaired
                        "[fd00:1122:3344:101::2]:12345".parse().unwrap(),
                        UpstairsRepairNotificationType::Failed,
                    ),
                )
                .await
                .unwrap();
        }

        // Activating the task now should start the drive saga

        let result = task.activate(&opctx).await;
        assert_eq!(
            result,
            json!({
                "region_replacement_driven_ok": 1,
                "region_replacement_driven_err": 0,
            })
        );

        let saga_request = saga_request_rx.try_recv().unwrap();

        assert!(matches!(
            saga_request,
            sagas::SagaRequest::RegionReplacementDrive { .. }
        ));
    }
}
