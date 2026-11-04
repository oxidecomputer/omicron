// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[cfg(test)]
mod test {
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::sled_expunge_request::TEST_FAILURE_REASON;
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
    use omicron_common::now_db_precision;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::RackUuid;
    use omicron_uuid_kinds::SledExpungeRequestKind;
    use omicron_uuid_kinds::SledUuid;
    use sled_hardware_types::BaseboardId;
    use std::collections::BTreeSet;
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
                send_uncertain: false,
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

}
