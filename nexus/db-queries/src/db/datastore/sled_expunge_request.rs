// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::pagination::paginated_multicolumn;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::LookupPath;
use nexus_db_model::DbSledExpungeRequestState;
use nexus_db_model::SledExpungeRequest as DbSledExpungeRequest;
use nexus_db_schema::schema::sled_expunge_request::dsl;
use nexus_types::sled_expunge::SledExpungeRequest;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use std::num::NonZeroU32;
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
        decode_sled_expunge_request(request)
    }

    pub async fn sled_expunge_request_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (chrono::DateTime<chrono::Utc>, Uuid)>,
    ) -> ListResultVec<SledExpungeRequest> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let rows = paginated_multicolumn(
            dsl::sled_expunge_request,
            (dsl::time_created, dsl::id),
            pagparams,
        )
        .select(DbSledExpungeRequest::as_select())
        .load_async(&*conn)
        .await
        .map_err(|error| {
            public_error_from_diesel(error, ErrorHandler::Server)
        })?;
        rows.into_iter().map(decode_sled_expunge_request).collect()
    }

    /// Lists every unfinished sled expunge request, across all
    /// racks, for the `sled_expunge` background task.
    ///
    /// The read is paginated and not inside a transaction so it isn't
    /// necessarily a consistent snapshot. Callers must always compare-and-set
    /// against the current values.
    pub async fn sled_expunge_request_list_unfinished(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<SledExpungeRequest> {
        self.sled_expunge_request_list_unfinished_batched(opctx, SQL_BATCH_SIZE)
            .await
    }

    async fn sled_expunge_request_list_unfinished_batched(
        &self,
        opctx: &OpContext,
        batch_size: NonZeroU32,
    ) -> ListResultVec<SledExpungeRequest> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        opctx.check_complex_operations_allowed()?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let mut requests = Vec::new();
        let mut paginator =
            Paginator::new(batch_size, dropshot::PaginationOrder::Ascending);
        while let Some(p) = paginator.next() {
            let batch = paginated(
                dsl::sled_expunge_request,
                // There's a unique index on sled IDs contingent on unfinished
                // requests.
                dsl::sled_id,
                &p.current_pagparams(),
            )
            .filter(dsl::state.eq_any(DbSledExpungeRequestState::UNFINISHED))
            .select(DbSledExpungeRequest::as_select())
            .load_async(&*conn)
            .await
            .map_err(|error| {
                public_error_from_diesel(error, ErrorHandler::Server)
            })?;
            paginator =
                p.found_batch(&batch, &|request: &DbSledExpungeRequest| {
                    request.sled_id.into_untyped_uuid()
                });
            for row in batch {
                requests.push(decode_sled_expunge_request(row)?);
            }
        }
        Ok(requests)
    }
}

fn decode_sled_expunge_request(
    row: DbSledExpungeRequest,
) -> Result<SledExpungeRequest, Error> {
    SledExpungeRequest::try_from(row)
        .map_err(|error| Error::internal_error(format!("{error:#}")))
}

#[cfg(test)]
mod tests {
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
    use nexus_db_model::DbSledExpungePowerOffOutcome;
    use nexus_db_model::DbSledExpungeRequestState;
    use nexus_db_model::DbSledExpungeSpSlotSource;
    use nexus_db_model::SledExpungeRequest as DbSledExpungeRequest;
    use nexus_db_model::SpMgsSlot;
    use nexus_db_schema::schema::sled_expunge_request::dsl;
    use nexus_types::sled_expunge::SledExpungeRequest;
    use nexus_types::sled_expunge::SledExpungeRequestState;
    use nexus_types::sled_expunge::SledExpungeRequestStateKind;
    use omicron_common::api::external::DataPageParams;
    use omicron_common::now_db_precision;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::RackUuid;
    use omicron_uuid_kinds::SledUuid;
    use sled_hardware_types::BaseboardId;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use strum::VariantArray;
    use trust_quorum_types::types::Epoch;
    use uuid::Uuid;

    // The names of the CHECK constraints on the `sled_expunge_request` table.
    const SP_SLOT_SOURCE_IFF_SP_SLOT: &str = "sp_slot_source_iff_sp_slot";
    const SP_SLOT_IS_SLED_CUBBY: &str = "sp_slot_is_sled_cubby";
    const POWER_OFF_OUTCOME_MATCHES_SP_SLOT: &str =
        "power_off_outcome_matches_sp_slot";
    const TIME_COMPLETED_IFF_FINISHED: &str = "time_completed_iff_finished";
    const FAILURE_REASON_IFF_FAILED: &str = "failure_reason_iff_failed";
    const POWER_OFF_OUTCOME_MATCHES_STATE: &str =
        "power_off_outcome_matches_state";
    const LAST_ERROR_ONLY_IF_UNFINISHED: &str = "last_error_only_if_unfinished";
    const UNFINISHED_BY_SLED_INDEX: &str =
        "lookup_unfinished_sled_expunge_request_by_sled";
    const UNFINISHED_BY_BASEBOARD_INDEX: &str =
        "lookup_unfinished_sled_expunge_request_by_baseboard";

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
        state: SledExpungeRequestStateKind,
    ) -> DbSledExpungeRequest {
        new_sled_expunge_request(
            RackUuid::new_v4(),
            sled_id,
            baseboard_id,
            Epoch(2),
            state,
        )
    }

    async fn insert(
        conn: &Conn,
        request: &DbSledExpungeRequest,
    ) -> Result<usize, DieselError> {
        diesel::insert_into(dsl::sled_expunge_request)
            .values(request.clone())
            .execute_async(conn)
            .await
    }

    fn expect_violation<T: std::fmt::Debug>(
        result: &Result<T, DieselError>,
        kind: DatabaseErrorKind,
        context: &str,
    ) -> String {
        let Err(DieselError::DatabaseError(actual_kind, info)) = result else {
            panic!("expected {kind:?} ({context})\nresult: {result:#?}");
        };
        assert_eq!(
            *actual_kind, kind,
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
        let valid = |state| {
            request_in_valid_state(
                SledUuid::new_v4(),
                unique_baseboard(),
                state,
            )
        };

        // Each row here breaks exactly one CHECK, so that the constraint the
        // database reports can only be that one.
        let cases = [
            (
                TIME_COMPLETED_IFF_FINISHED,
                DbSledExpungeRequest {
                    time_completed: Some(now_db_precision()),
                    ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
                },
            ),
            (
                FAILURE_REASON_IFF_FAILED,
                DbSledExpungeRequest {
                    failure_reason: Some(TEST_FAILURE_REASON.to_string()),
                    ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
                },
            ),
            (
                POWER_OFF_OUTCOME_MATCHES_STATE,
                DbSledExpungeRequest {
                    power_off_outcome: Some(
                        DbSledExpungePowerOffOutcome::PoweredOff,
                    ),
                    ..valid(SledExpungeRequestStateKind::PoweringOff)
                },
            ),
            (
                LAST_ERROR_ONLY_IF_UNFINISHED,
                DbSledExpungeRequest {
                    last_error: Some("test last error".to_string()),
                    ..valid(SledExpungeRequestStateKind::Done)
                },
            ),
            (
                SP_SLOT_SOURCE_IFF_SP_SLOT,
                DbSledExpungeRequest {
                    sp_slot_source: None,
                    ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
                },
            ),
            (
                SP_SLOT_SOURCE_IFF_SP_SLOT,
                DbSledExpungeRequest {
                    sp_slot: None,
                    ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
                },
            ),
            (
                SP_SLOT_IS_SLED_CUBBY,
                DbSledExpungeRequest {
                    sp_slot: Some(SpMgsSlot::from(32)),
                    ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
                },
            ),
            (
                POWER_OFF_OUTCOME_MATCHES_SP_SLOT,
                DbSledExpungeRequest {
                    sp_slot: None,
                    sp_slot_source: None,
                    power_off_outcome: Some(
                        DbSledExpungePowerOffOutcome::PoweredOff,
                    ),
                    ..valid(SledExpungeRequestStateKind::Expunging)
                },
            ),
            (
                POWER_OFF_OUTCOME_MATCHES_SP_SLOT,
                DbSledExpungeRequest {
                    power_off_outcome: Some(
                        DbSledExpungePowerOffOutcome::AbsentAtCreation,
                    ),
                    ..valid(SledExpungeRequestStateKind::Done)
                },
            ),
        ];
        for (expected_constraint, request) in cases {
            let constraint = expect_violation(
                &insert(&conn, &request).await,
                DatabaseErrorKind::CheckViolation,
                expected_constraint,
            );
            assert_eq!(constraint, expected_constraint);
        }

        insert(
            &conn,
            &DbSledExpungeRequest {
                sp_slot: None,
                sp_slot_source: None,
                ..valid(SledExpungeRequestStateKind::WaitingForTrustQuorum)
            },
        )
        .await
        .expect("inserted a request with neither sp_slot nor sp_slot_source");

        // These rows show that the same CHECKs don't reject valid states
        let allowed = [
            DbSledExpungeRequest {
                sp_slot: Some(SpMgsSlot::from(31)),
                ..valid(SledExpungeRequestStateKind::Expunging)
            },
            DbSledExpungeRequest {
                sp_slot: None,
                sp_slot_source: None,
                power_off_outcome: Some(
                    DbSledExpungePowerOffOutcome::AbsentAtCreation,
                ),
                ..valid(SledExpungeRequestStateKind::Done)
            },
            DbSledExpungeRequest {
                power_off_outcome: Some(
                    DbSledExpungePowerOffOutcome::OtherBoardPresent,
                ),
                ..valid(SledExpungeRequestStateKind::Failed)
            },
        ];
        for request in &allowed {
            insert(&conn, request).await.unwrap_or_else(|error| {
                panic!("inserted {request:?}: {error:#?}")
            });
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Exhaustively ensure that Rust columns enforce the correct set of CHECK
    /// constraints, using the database as an oracle.
    #[tokio::test]
    async fn test_sled_expunge_request_decode_matches_database() {
        let logctx = dev::test_setup_log(
            "test_sled_expunge_request_decode_matches_database",
        );
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let conn = db.datastore().pool_connection_for_tests().await.unwrap();

        // Only in-range slots are tried, because `decode` doesn't check
        // `sp_slot_is_sled_cubby` (we don't have a range-checked cubby newtype
        // yet).
        let slot = SpMgsSlot::from(3);
        let source = DbSledExpungeSpSlotSource::Inventory;
        let cubby_columns = [
            (None, None),
            (Some(slot), Some(source)),
            (Some(slot), None),
            (None, Some(source)),
        ];
        let outcomes: Vec<_> = std::iter::once(None)
            .chain(
                DbSledExpungePowerOffOutcome::VARIANTS
                    .iter()
                    .copied()
                    .map(Some),
            )
            .collect();
        let time_completed_values = [None, Some(now_db_precision())];

        let mut combination_count = 0;
        let mut decoded_states: Vec<SledExpungeRequestState> = Vec::new();
        for (
            &state,
            &(sp_slot, sp_slot_source),
            &power_off_outcome,
            last_error,
            failure_reason,
            &time_completed,
        ) in iproduct!(
            DbSledExpungeRequestState::VARIANTS,
            &cubby_columns,
            &outcomes,
            [None, Some("test last error".to_string())],
            [None, Some(TEST_FAILURE_REASON.to_string())],
            &time_completed_values
        ) {
            combination_count += 1;
            // Each row gets its own ID, sled and baseboard so that the unique
            // indexes don't get in the way.
            let row = DbSledExpungeRequest {
                state,
                sp_slot,
                sp_slot_source,
                power_off_outcome,
                last_error,
                failure_reason,
                time_completed,
                ..DbSledExpungeRequest::new(
                    RackUuid::new_v4(),
                    SledUuid::new_v4(),
                    unique_baseboard(),
                    None,
                    Epoch(2),
                )
                .expect("epoch 2 fits in the database")
            };
            let result = diesel::insert_into(dsl::sled_expunge_request)
                .values(row.clone())
                .returning(DbSledExpungeRequest::as_returning())
                .get_result_async(&*conn)
                .await;
            match &result {
                Ok(read_back) => {
                    let decoded =
                        SledExpungeRequest::try_from(read_back.clone())
                            .unwrap_or_else(|error| {
                                panic!(
                                    "a row the database accepted decodes: \
                                     {row:?}: {error:#}"
                                )
                            });
                    decoded_states.push(decoded.state);
                }
                Err(_) => {
                    expect_violation(
                        &result,
                        DatabaseErrorKind::CheckViolation,
                        &format!("{row:?}"),
                    );
                    let error = SledExpungeRequest::try_from(row.clone())
                        .expect_err(&format!(
                            "a row the database rejected fails to decode: \
                             {row:?}"
                        ));
                    let expected_prefix = format!(
                        "illegal database state (CHECK constraint broken?!): \
                         sled expunge request {} has ",
                        row.id(),
                    );
                    assert!(
                        error.to_string().starts_with(&expected_prefix),
                        "decoding {row:?} fails with the illegal database \
                         state error, got: {error:#}",
                    );
                }
            }
        }
        assert_eq!(combination_count, 960);

        for (index, state) in decoded_states.iter().enumerate() {
            for other in &decoded_states[index + 1..] {
                assert_ne!(
                    state, other,
                    "two different accepted rows decode to the same state",
                );
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that the `sled_expunge_request` unique indexes reject duplicate
    /// rows.
    #[tokio::test]
    async fn test_sled_expunge_request_unique_indexes() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_unique_indexes");
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let conn = db.datastore().pool_connection_for_tests().await.unwrap();

        for (&existing_state, &new_state) in iproduct!(
            SledExpungeRequestStateKind::ALL,
            SledExpungeRequestStateKind::ALL
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

    /// Ensure that pagination on `sled_expunge_request` works correctly.
    #[tokio::test]
    async fn test_sled_expunge_request_list_unfinished() {
        use SledExpungeRequestStateKind::*;

        let logctx =
            dev::test_setup_log("test_sled_expunge_request_list_unfinished");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // The sleds are listed in `sled_id` order. At batch sizes 1, 2 and 3,
        // some page boundaries end up just before runs of finished rows, and
        // some sleds have finished requests alongside their one unfinished
        // request. The 5 unfinished rows also make the last page short at sizes
        // 2 and 3.
        let states_by_sled: [&[SledExpungeRequestStateKind]; 11] = [
            &[Done],
            &[WaitingForTrustQuorum],
            &[Failed],
            &[PoweringOff, Done, Failed],
            &[Done],
            &[Failed],
            &[Expunging, Done],
            &[WaitingForTrustQuorum],
            &[Failed, Done],
            &[PoweringOff],
            &[Done],
        ];
        let rows: Vec<_> = (0..)
            .zip(states_by_sled)
            .flat_map(|(index, states)| {
                let sled_id = SledUuid::from_u128(
                    0x5ed00000_0000_4000_8000_000000000000 + index,
                );
                states.iter().map(move |&state| {
                    request_in_valid_state(sled_id, unique_baseboard(), state)
                })
            })
            .collect();
        let expected: Vec<_> = rows
            .iter()
            .filter(|request| {
                !SledExpungeRequestStateKind::from(request.state).is_finished()
            })
            .map(|request| {
                SledExpungeRequest::try_from(request.clone())
                    .expect("decoded a test request")
            })
            .collect();
        assert_eq!(expected.len(), 5);

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        diesel::insert_into(dsl::sled_expunge_request)
            .values(rows)
            .execute_async(&*conn)
            .await
            .expect("bulk-inserted the test requests");

        for batch_size in [1, 2, 3] {
            let listed = datastore
                .sled_expunge_request_list_unfinished_batched(
                    opctx,
                    NonZeroU32::new(batch_size).expect("batch size is nonzero"),
                )
                .await
                .expect("listed unfinished requests");
            assert_eq!(listed, expected, "batch size {batch_size}");
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_expunge_request_viewer_can_read() {
        let logctx =
            dev::test_setup_log("test_sled_expunge_request_viewer_can_read");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let opctx = opctx_as(&db, authn::Context::internal_read());

        let request = insert_sled_expunge_request(
            datastore,
            RackUuid::new_v4(),
            unique_baseboard(),
            Epoch(1),
            SledExpungeRequestStateKind::WaitingForTrustQuorum,
        )
        .await;

        assert_eq!(
            datastore
                .sled_expunge_request_get(&opctx, request.id)
                .await
                .expect("a fleet viewer can get a request"),
            request,
        );
        assert_eq!(
            datastore
                .sled_expunge_request_list(&opctx, &DataPageParams::max_page())
                .await
                .expect("a fleet viewer can list requests"),
            vec![request.clone()],
        );
        assert_eq!(
            datastore
                .sled_expunge_request_list_unfinished(&opctx)
                .await
                .expect("a fleet viewer can list unfinished requests"),
            vec![request],
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
