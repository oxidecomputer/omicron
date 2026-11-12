// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::LookupPath;
use nexus_db_model::SledExpungeRequest;
use nexus_db_model::SledExpungeRequestState;
use nexus_db_schema::schema::sled_expunge_request::dsl;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use uuid::Uuid;

impl DataStore {
    pub async fn sled_expunge_request_get(
        &self,
        opctx: &OpContext,
        id: SledExpungeRequestUuid,
    ) -> LookupResult<SledExpungeRequest> {
        let (.., request) = LookupPath::new(opctx, self)
            .sled_expunge_request(id)
            .fetch()
            .await?;
        Ok(request)
    }

    // TODO-RAINCLAUDE: pages by time_created and then id, like support_bundle_list, using the lookup_sled_expunge_request_by_creation index.
    pub async fn sled_expunge_request_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (chrono::DateTime<chrono::Utc>, Uuid)>,
    ) -> ListResultVec<SledExpungeRequest> {
        opctx
            .authorize(
                authz::Action::ListChildren,
                &authz::SLED_EXPUNGE_REQUEST_LIST,
            )
            .await?;
        use crate::db::pagination::paginated_multicolumn;
        let conn = self.pool_connection_authorized(opctx).await?;
        paginated_multicolumn(
            dsl::sled_expunge_request,
            (dsl::time_created, dsl::id),
            pagparams,
        )
        .select(SledExpungeRequest::as_select())
        .load_async(&*conn)
        .await
        .map_err(|error| public_error_from_diesel(error, ErrorHandler::Server))
    }

    // TODO-RAINCLAUDE: every Nexus's sled_expunge task reads this each activation; batched like other background-task list-all helpers, across all racks; the batches are not one snapshot, so callers must compare-and-swap and tolerate one activation of lag.
    pub async fn sled_expunge_request_list_unfinished(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<SledExpungeRequest> {
        opctx
            .authorize(
                authz::Action::ListChildren,
                &authz::SLED_EXPUNGE_REQUEST_LIST,
            )
            .await?;
        opctx.check_complex_operations_allowed()?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let mut requests = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = paginated(
                dsl::sled_expunge_request,
                dsl::id,
                &p.current_pagparams(),
            )
            .filter(dsl::state.eq_any(SledExpungeRequestState::UNFINISHED))
            .select(SledExpungeRequest::as_select())
            .load_async(&*conn)
            .await
            .map_err(|error| {
                public_error_from_diesel(error, ErrorHandler::Server)
            })?;
            paginator = p
                .found_batch(&batch, &|request: &SledExpungeRequest| {
                    request.id.into_untyped_uuid()
                });
            requests.extend(batch);
        }
        Ok(requests)
    }
}

#[cfg(test)]
mod test {
    use super::SQL_BATCH_SIZE;
    use crate::authn;
    use crate::authz;
    use crate::context::OpContext;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::sled_expunge_request::TEST_FAILURE_REASON;
    use crate::db::pub_test_utils::sled_expunge_request::insert_sled_expunge_request;
    use crate::db::pub_test_utils::sled_expunge_request::new_sled_expunge_request;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::prelude::*;
    use diesel::result::DatabaseErrorKind;
    use diesel::result::Error as DieselError;
    use itertools::iproduct;
    use nexus_db_model::DbTypedUuid;
    use nexus_db_model::SledExpungePowerOffOutcome;
    use nexus_db_model::SledExpungeRequest;
    use nexus_db_model::SledExpungeRequestState;
    use nexus_db_schema::schema::sled_expunge_request::dsl;
    use omicron_common::api::external::DataPageParams;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::LookupType;
    use omicron_common::api::external::ResourceType;
    use omicron_common::now_db_precision;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::RackUuid;
    use omicron_uuid_kinds::SledExpungeRequestKind;
    use omicron_uuid_kinds::SledUuid;
    use sled_hardware_types::BaseboardId;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use uuid::Uuid;

    const TQ_EPOCH_IS_POSITIVE: &str = "tq_epoch_is_positive";
    const TIME_COMPLETED_IFF_FINISHED: &str = "time_completed_iff_finished";
    const FAILURE_REASON_IFF_FAILED: &str = "failure_reason_iff_failed";
    const POWER_OFF_OUTCOME_MATCHES_STATE: &str =
        "power_off_outcome_matches_state";
    const LAST_ERROR_ONLY_IF_UNFINISHED: &str = "last_error_only_if_unfinished";
    const UNFINISHED_BY_SLED_INDEX: &str =
        "lookup_unfinished_sled_expunge_request_by_sled";
    const UNFINISHED_BY_BASEBOARD_INDEX: &str =
        "lookup_unfinished_sled_expunge_request_by_baseboard";
    const BY_RACK_AND_EPOCH_INDEX: &str =
        "lookup_sled_expunge_request_by_rack_and_epoch";

    type Conn = async_bb8_diesel::Connection<nexus_db_lookup::DbConnection>;

    fn unique_baseboard() -> BaseboardId {
        BaseboardId {
            part_number: "913-0000019".to_string(),
            serial_number: format!("serial-{}", Uuid::new_v4()),
        }
    }

    fn request_in_valid_state(
        sled_id: SledUuid,
        baseboard_id: BaseboardId,
        state: SledExpungeRequestState,
    ) -> SledExpungeRequest {
        new_sled_expunge_request(
            RackUuid::new_v4(),
            sled_id,
            baseboard_id,
            2,
            state,
        )
    }

    async fn insert(
        conn: &Conn,
        request: &SledExpungeRequest,
    ) -> Result<usize, DieselError> {
        diesel::insert_into(dsl::sled_expunge_request)
            .values(request.clone())
            .execute_async(conn)
            .await
    }

    async fn read_back(
        conn: &Conn,
        id: DbTypedUuid<SledExpungeRequestKind>,
    ) -> SledExpungeRequest {
        dsl::sled_expunge_request
            .filter(dsl::id.eq(id))
            .select(SledExpungeRequest::as_select())
            .get_result_async(conn)
            .await
            .expect("read back the inserted row")
    }

    fn expect_violation(
        result: &Result<usize, DieselError>,
        kind: DatabaseErrorKind,
        context: &str,
    ) -> String {
        let Err(DieselError::DatabaseError(actual_kind, info)) = result else {
            panic!("expected {kind:?} ({context})\nresult: {result:#?}");
        };
        assert_eq!(
            std::mem::discriminant(actual_kind),
            std::mem::discriminant(&kind),
            "expected {kind:?} ({context}), got {actual_kind:?}: {info:?}",
        );
        info.constraint_name()
            .unwrap_or_else(|| {
                panic!("{kind:?} names its constraint ({context}): {info:?}")
            })
            .to_string()
    }

    #[tokio::test]
    async fn test_sled_expunge_request_check_constraints() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_check_constraints");
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let conn = db.datastore().pool_connection_for_tests().await.unwrap();
        let now = now_db_precision();

        use SledExpungePowerOffOutcome::PoweredOff;
        use SledExpungeRequestState::*;
        let valid = [
            (WaitingForTrustQuorum, None, false, false, false),
            (WaitingForTrustQuorum, None, false, false, true),
            (PoweringOff, None, false, false, false),
            (PoweringOff, None, false, false, true),
            (Expunging, Some(PoweredOff), false, false, false),
            (Expunging, Some(PoweredOff), false, false, true),
            (Done, Some(PoweredOff), false, true, false),
            (Failed, None, true, true, false),
            (Failed, Some(PoweredOff), true, true, false),
        ];

        let outcomes: Vec<Option<SledExpungePowerOffOutcome>> =
            std::iter::once(None)
                .chain(
                    SledExpungePowerOffOutcome::ALL.iter().copied().map(Some),
                )
                .collect();

        let mut n_valid_inserted = 0;
        let mut reported = BTreeSet::new();
        for (
            &state,
            &power_off_outcome,
            has_failure_reason,
            has_time_completed,
            has_last_error,
        ) in iproduct!(
            SledExpungeRequestState::ALL,
            &outcomes,
            [false, true],
            [false, true],
            [false, true]
        ) {
            let request = SledExpungeRequest {
                state,
                power_off_outcome,
                last_error: has_last_error
                    .then(|| "test last error".to_string()),
                failure_reason: has_failure_reason
                    .then(|| TEST_FAILURE_REASON.to_string()),
                time_completed: has_time_completed.then_some(now),
                time_created: now,
                time_modified: now,
                ..request_in_valid_state(
                    SledUuid::new_v4(),
                    unique_baseboard(),
                    WaitingForTrustQuorum,
                )
            };
            let result = insert(&conn, &request).await;

            let key = (
                state,
                power_off_outcome,
                has_failure_reason,
                has_time_completed,
                has_last_error,
            );
            if valid.contains(&key) {
                if let Err(error) = &result {
                    panic!(
                        "expected a valid row to be inserted\n\
                         row: {request:#?}\nerror: {error:#?}"
                    );
                }
                assert_eq!(read_back(&conn, request.id).await, request);
                n_valid_inserted += 1;
            } else {
                reported.insert(expect_violation(
                    &result,
                    DatabaseErrorKind::CheckViolation,
                    &format!("{key:?}"),
                ));
            }
        }
        assert_eq!(n_valid_inserted, valid.len());
        assert_eq!(
            reported,
            [
                TIME_COMPLETED_IFF_FINISHED,
                FAILURE_REASON_IFF_FAILED,
                POWER_OFF_OUTCOME_MATCHES_STATE,
                LAST_ERROR_ONLY_IF_UNFINISHED,
            ]
            .into_iter()
            .map(String::from)
            .collect::<BTreeSet<_>>(),
        );

        for tq_epoch in [0, -1, i64::MIN] {
            let request = SledExpungeRequest {
                tq_epoch,
                ..request_in_valid_state(
                    SledUuid::new_v4(),
                    unique_baseboard(),
                    WaitingForTrustQuorum,
                )
            };
            let constraint = expect_violation(
                &insert(&conn, &request).await,
                DatabaseErrorKind::CheckViolation,
                &format!("tq_epoch {tq_epoch}"),
            );
            assert_eq!(constraint, TQ_EPOCH_IS_POSITIVE);
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_unique_indexes() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_unique_indexes");
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let conn = db.datastore().pool_connection_for_tests().await.unwrap();

        for (&existing_state, &new_state) in iproduct!(
            SledExpungeRequestState::ALL,
            SledExpungeRequestState::ALL
        ) {
            let context = format!("{existing_state:?} then {new_state:?}");
            let both_unfinished =
                !existing_state.is_finished() && !new_state.is_finished();

            let sled_id = SledUuid::new_v4();
            insert(
                &conn,
                &request_in_valid_state(
                    sled_id,
                    unique_baseboard(),
                    existing_state,
                ),
            )
            .await
            .expect("inserted the first request for a sled");
            let result = insert(
                &conn,
                &request_in_valid_state(sled_id, unique_baseboard(), new_state),
            )
            .await;
            if both_unfinished {
                let constraint = expect_violation(
                    &result,
                    DatabaseErrorKind::UniqueViolation,
                    &format!("same sled, {context}"),
                );
                assert_eq!(constraint, UNFINISHED_BY_SLED_INDEX);
            } else {
                result.unwrap_or_else(|error| {
                    panic!("same sled, {context}: {error:#?}")
                });
            }

            let baseboard_id = unique_baseboard();
            insert(
                &conn,
                &request_in_valid_state(
                    SledUuid::new_v4(),
                    baseboard_id.clone(),
                    existing_state,
                ),
            )
            .await
            .expect("inserted the first request for a baseboard");
            let result = insert(
                &conn,
                &request_in_valid_state(
                    SledUuid::new_v4(),
                    baseboard_id,
                    new_state,
                ),
            )
            .await;
            if both_unfinished {
                let constraint = expect_violation(
                    &result,
                    DatabaseErrorKind::UniqueViolation,
                    &format!("same baseboard, {context}"),
                );
                assert_eq!(constraint, UNFINISHED_BY_BASEBOARD_INDEX);
            } else {
                result.unwrap_or_else(|error| {
                    panic!("same baseboard, {context}: {error:#?}")
                });
            }
        }

        let first = request_in_valid_state(
            SledUuid::new_v4(),
            unique_baseboard(),
            SledExpungeRequestState::Failed,
        );
        insert(&conn, &first).await.expect("inserted the first request");
        let same_epoch = SledExpungeRequest {
            rack_id: first.rack_id,
            tq_epoch: first.tq_epoch,
            ..request_in_valid_state(
                SledUuid::new_v4(),
                unique_baseboard(),
                SledExpungeRequestState::Failed,
            )
        };
        let constraint = expect_violation(
            &insert(&conn, &same_epoch).await,
            DatabaseErrorKind::UniqueViolation,
            "same rack and epoch",
        );
        assert_eq!(constraint, BY_RACK_AND_EPOCH_INDEX);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    fn opctx_as(db: &TestDatabase, authn: authn::Context) -> OpContext {
        let log = db.opctx().log.clone();
        OpContext::for_background(
            log.clone(),
            Arc::new(authz::Authz::new(&log)),
            authn,
            Arc::clone(db.datastore()) as Arc<dyn nexus_auth::storage::Storage>,
        )
    }

    async fn insert_in_state(
        db: &TestDatabase,
        rack_id: RackUuid,
        tq_epoch: i64,
        state: SledExpungeRequestState,
    ) -> SledExpungeRequest {
        insert_sled_expunge_request(
            db.datastore(),
            rack_id,
            &unique_baseboard(),
            tq_epoch,
            state,
        )
        .await
    }

    #[tokio::test]
    async fn test_sled_expunge_request_list_unfinished() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_list_unfinished");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // TODO-RAINCLAUDE: cycling through every state gives more than SQL_BATCH_SIZE unfinished rows, so the listing spans two batches and skips the finished rows in both.
        let batch_size = usize::try_from(SQL_BATCH_SIZE.get()).unwrap();
        let rows: Vec<_> = SledExpungeRequestState::ALL
            .iter()
            .cycle()
            .take(2 * batch_size)
            .map(|&state| {
                new_sled_expunge_request(
                    RackUuid::new_v4(),
                    SledUuid::new_v4(),
                    unique_baseboard(),
                    1,
                    state,
                )
            })
            .collect();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let inserted: Vec<SledExpungeRequest> =
            diesel::insert_into(dsl::sled_expunge_request)
                .values(rows)
                .returning(SledExpungeRequest::as_returning())
                .get_results_async(&*conn)
                .await
                .expect("bulk-inserted requests in every state");
        let mut expected: Vec<_> = inserted
            .into_iter()
            .filter(|request| !request.state.is_finished())
            .collect();
        expected.sort_by_key(|request| request.id.into_untyped_uuid());
        assert!(
            expected.len() > batch_size,
            "{} unfinished rows span more than one batch of {batch_size}",
            expected.len(),
        );

        let listed = datastore
            .sled_expunge_request_list_unfinished(opctx)
            .await
            .expect("listed unfinished requests");
        assert_eq!(listed, expected);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_viewer_denied() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_viewer_denied");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = opctx_as(&db, authn::Context::internal_read());

        let request = insert_in_state(
            &db,
            RackUuid::new_v4(),
            1,
            SledExpungeRequestState::WaitingForTrustQuorum,
        )
        .await;
        let not_found = Error::ObjectNotFound {
            type_name: ResourceType::SledExpungeRequest,
            lookup_type: LookupType::ById(request.id().into_untyped_uuid()),
        };

        assert_eq!(
            datastore
                .sled_expunge_request_get(&opctx, request.id())
                .await
                .expect_err("a fleet viewer is denied get"),
            not_found,
        );
        assert_eq!(
            datastore
                .sled_expunge_request_list(&opctx, &DataPageParams::max_page())
                .await
                .expect_err("a fleet viewer is denied list"),
            Error::Forbidden,
        );
        assert_eq!(
            datastore
                .sled_expunge_request_list_unfinished(&opctx)
                .await
                .expect_err("a fleet viewer is denied list_unfinished"),
            Error::Forbidden,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
