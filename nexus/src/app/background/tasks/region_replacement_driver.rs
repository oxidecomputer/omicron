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

use crate::app::authn;
use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas;
use crate::app::sagas::region_replacement_drive::SagaRegionReplacementDrive;
use crate::app::sagas::region_replacement_finish::SagaRegionReplacementFinish;
use crate::app::sagas::NexusSaga;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::RegionReplacementDriverStatus;
use serde_json::json;
use std::sync::Arc;

pub struct RegionReplacementDriver {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
}

impl RegionReplacementDriver {
    pub fn new(datastore: Arc<DataStore>, sagas: Arc<dyn StartSaga>) -> Self {
        RegionReplacementDriver { datastore, sagas }
    }

    /// Drive running region replacements forward
    pub async fn drive_running_replacements_forward(
        &self,
        opctx: &OpContext,
        status: &mut RegionReplacementDriverStatus,
    ) {
        let log = &opctx.log;

        let running_replacements =
            match self.datastore.get_running_region_replacements(opctx).await {
                Ok(requests) => requests,

                Err(e) => {
                    let s = format!(
                        "query for running region replacement requests \
                        failed: {e}"
                    );

                    error!(&log, "{s}");
                    status.errors.push(s);

                    return;
                }
            };

        for request in running_replacements {
            // If a successful finish notification was received, change the
            // state here: don't drive requests forward where the replacement is
            // done.

            let has_matching_finish_notification = match self
                .datastore
                .request_has_matching_successful_finish_notification(
                    opctx, &request,
                )
                .await
            {
                Ok(has_matching_finish_notification) => {
                    has_matching_finish_notification
                }

                Err(e) => {
                    let s = format!(
                        "checking for a finish notification for {} failed: {e}",
                        request.id
                    );

                    error!(&log, "{s}");
                    status.errors.push(s);

                    // Nexus may determine the request is `ReplacementDone` via
                    // the drive saga polling an Upstairs, so return false here
                    // to invoke that saga.
                    false
                }
            };

            if has_matching_finish_notification {
                if let Err(e) = self
                    .datastore
                    .mark_region_replacement_as_done(opctx, request.id)
                    .await
                {
                    let s = format!(
                        "error marking {} as ReplacementDone: {e}",
                        request.id
                    );

                    error!(&log, "{s}");
                    status.errors.push(s);
                }
            } else {
                // Otherwise attempt to drive the replacement's progress forward
                // (or determine if it is complete).

                let request_id = request.id;
                let params = sagas::region_replacement_drive::Params {
                    serialized_authn: authn::saga::Serialized::for_opctx(opctx),
                    request,
                };
                let result = async {
                    let saga_dag =
                        SagaRegionReplacementDrive::prepare(&params)?;
                    self.sagas.saga_start(saga_dag).await
                }
                .await;
                match result {
                    Ok(_) => {
                        let s = format!("{request_id}: drive saga started ok");
                        info!(&log, "{s}");
                        status.drive_invoked_ok.push(s);
                    }
                    Err(e) => {
                        let s = format!(
                            "starting region replacement drive saga for \
                            {request_id} failed: {e}",
                        );

                        error!(&log, "{s}");
                        status.errors.push(s);
                    }
                }
            }
        }
    }

    /// Complete region replacements that are done
    pub async fn complete_done_replacements(
        &self,
        opctx: &OpContext,
        status: &mut RegionReplacementDriverStatus,
    ) {
        let log = &opctx.log;

        let done_replacements =
            match self.datastore.get_done_region_replacements(opctx).await {
                Ok(requests) => requests,

                Err(e) => {
                    let s = format!(
                        "query for done region replacement requests failed: {e}"
                    );

                    error!(&log, "{s}");
                    status.errors.push(s);

                    return;
                }
            };

        for request in done_replacements {
            let Some(old_region_volume_id) = request.old_region_volume_id
            else {
                // This state is illegal!
                let s = format!(
                    "request {} old region volume id is None!",
                    request.id,
                );

                error!(&log, "{s}");
                status.errors.push(s);

                continue;
            };

            let request_id = request.id;
            let params = sagas::region_replacement_finish::Params {
                serialized_authn: authn::saga::Serialized::for_opctx(opctx),
                region_volume_id: old_region_volume_id,
                request,
            };
            let result = async {
                let saga_dag = SagaRegionReplacementFinish::prepare(&params)?;
                self.sagas.saga_start(saga_dag).await
            }
            .await;
            match result {
                Ok(_) => {
                    let s = format!("{request_id}: finish saga started ok");

                    info!(&log, "{s}");
                    status.finish_invoked_ok.push(s);
                }

                Err(e) => {
                    let s = format!(
                        "starting region replacement finish saga for \
                        {request_id} failed: {e}"
                    );

                    error!(&log, "{s}");
                    status.errors.push(s);
                }
            }
        }
    }
}

impl BackgroundTask for RegionReplacementDriver {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let mut status = RegionReplacementDriverStatus::default();

            self.drive_running_replacements_forward(opctx, &mut status).await;
            self.complete_done_replacements(opctx, &mut status).await;

            json!(status)
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::background::init::test::NoopStartSaga;
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
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_running_region_replacement_causes_drive(
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
            RegionReplacementDriver::new(datastore.clone(), starter.clone());

        // Noop test
        let result = task.activate(&opctx).await;
        assert_eq!(result, json!(RegionReplacementDriverStatus::default()));

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

        let request_id = request.id;

        datastore
            .insert_region_replacement_request(&opctx, request)
            .await
            .unwrap();

        // Activate the task - it should pick that up and try to run the region
        // replacement drive saga
        let result: RegionReplacementDriverStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        assert_eq!(
            result.drive_invoked_ok,
            vec![format!("{request_id}: drive saga started ok")]
        );
        assert!(result.finish_invoked_ok.is_empty());
        assert!(result.errors.is_empty());

        assert_eq!(starter.count_reset(), 1);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_done_region_replacement_causes_finish(
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
            RegionReplacementDriver::new(datastore.clone(), starter.clone());

        // Noop test
        let result = task.activate(&opctx).await;
        assert_eq!(result, json!(RegionReplacementDriverStatus::default()));

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
                27015,
                false,
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
                27016,
                false,
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

        // Add a region replacement request for that region, and change it to
        // state ReplacementDone. Set the new_region_id to the region created
        // above.
        let request = {
            let mut request =
                RegionReplacement::new(old_region.id(), old_region.volume_id());
            request.replacement_state = RegionReplacementState::ReplacementDone;
            request.new_region_id = Some(new_region.id());
            request.old_region_volume_id = Some(Uuid::new_v4());
            request
        };

        let request_id = request.id;

        datastore
            .insert_region_replacement_request(&opctx, request)
            .await
            .unwrap();

        // Activate the task - it should pick that up and try to run the region
        // replacement finish saga
        let result: RegionReplacementDriverStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        assert!(result.drive_invoked_ok.is_empty());
        assert_eq!(
            result.finish_invoked_ok,
            vec![format!("{request_id}: finish saga started ok")]
        );
        assert!(result.errors.is_empty());

        assert_eq!(starter.count_reset(), 1);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_mark_region_replacement_done_after_notification(
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
            RegionReplacementDriver::new(datastore.clone(), starter.clone());

        // Noop test
        let result = task.activate(&opctx).await;
        assert_eq!(result, json!(RegionReplacementDriverStatus::default()));

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
                27015,
                false,
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
                27016,
                false,
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

        // Add a region replacement request for that region, and change it to
        // state Running. Set the new_region_id to the region created above.
        let request = {
            let mut request =
                RegionReplacement::new(old_region.id(), old_region.volume_id());
            request.replacement_state = RegionReplacementState::Running;
            request.new_region_id = Some(new_region.id());
            request.old_region_volume_id = Some(Uuid::new_v4());
            request
        };

        let request_id = request.id;

        datastore
            .insert_region_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // Activate the task - it should pick that up and try to run the region
        // replacement drive saga
        let result: RegionReplacementDriverStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        assert_eq!(
            result.drive_invoked_ok,
            vec![format!("{request_id}: drive saga started ok")]
        );
        assert!(result.finish_invoked_ok.is_empty());
        assert!(result.errors.is_empty());

        assert_eq!(starter.count_reset(), 1);

        // Now, pretend that an Upstairs sent a notification that it
        // successfully finished a repair

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
        let result: RegionReplacementDriverStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        assert_eq!(result.finish_invoked_ok.len(), 1);

        {
            let request_in_db = datastore
                .get_region_replacement_request_by_id(&opctx, request.id)
                .await
                .unwrap();
            assert_eq!(
                request_in_db.replacement_state,
                RegionReplacementState::ReplacementDone
            );
        }

        assert_eq!(starter.count_reset(), 1);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_no_mark_region_replacement_done_after_failed_notification(
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
            RegionReplacementDriver::new(datastore.clone(), starter.clone());

        // Noop test
        let result = task.activate(&opctx).await;
        assert_eq!(result, json!(RegionReplacementDriverStatus::default()));

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
                27015,
                false,
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
                27016,
                false,
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

        // Add a region replacement request for that region, and change it to
        // state Running. Set the new_region_id to the region created above.
        let request = {
            let mut request =
                RegionReplacement::new(old_region.id(), old_region.volume_id());
            request.replacement_state = RegionReplacementState::Running;
            request.new_region_id = Some(new_region.id());
            request
        };

        let request_id = request.id;

        datastore
            .insert_region_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // Activate the task - it should pick that up and try to run the region
        // replacement drive saga
        let result: RegionReplacementDriverStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        assert_eq!(
            result.drive_invoked_ok,
            vec![format!("{request_id}: drive saga started ok")]
        );
        assert!(result.finish_invoked_ok.is_empty());
        assert!(result.errors.is_empty());

        assert_eq!(starter.count_reset(), 1);

        // Now, pretend that an Upstairs sent a notification that it failed to
        // finish a repair

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
        let result: RegionReplacementDriverStatus =
            serde_json::from_value(task.activate(&opctx).await).unwrap();

        assert_eq!(
            result.drive_invoked_ok,
            vec![format!("{request_id}: drive saga started ok")]
        );
        assert!(result.finish_invoked_ok.is_empty());
        assert!(result.errors.is_empty());

        assert_eq!(starter.count_reset(), 1);
    }
}
