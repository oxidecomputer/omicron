// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use crate::context::OpContext;
use crate::db::DataStore;
use crate::db::datastore::SledExpungeRequestUpdateOutcome;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_lookup::LookupPath;
use nexus_db_model::SledExpungePowerOffOutcome;
use nexus_db_model::SledExpungeRequest;
use nexus_db_model::SledExpungeRequestAdvance;
use nexus_db_model::SledExpungeRequestState;
use nexus_db_model::SledExpungeRequestTransition;
use nexus_db_model::SledExpungeRequestUnfinishedState;
use nexus_db_schema::schema::sled_expunge_request::dsl;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;

pub const TEST_FAILURE_REASON: &str = "test failure";

// TODO-RAINCLAUDE: walks the real transition column mapping forward from request's state to state, failing straight from the current unfinished state; the power-off step always records PoweredOff, and a backward move or a move out of a finished state panics.
fn sled_expunge_request_with_state(
    mut request: SledExpungeRequest,
    state: SledExpungeRequestState,
) -> SledExpungeRequest {
    let now = now_db_precision();
    let original_state = request.state;
    while request.state != state {
        let from = request.state.as_unfinished().unwrap_or_else(|| {
            panic!(
                "test moves a sled expunge request forward from an unfinished \
                 state (requested {original_state} -> {state})",
            )
        });
        let transition = match state {
            SledExpungeRequestState::Failed => {
                SledExpungeRequestTransition::Failed {
                    from,
                    failure_reason: TEST_FAILURE_REASON.to_string(),
                }
            }
            SledExpungeRequestState::WaitingForTrustQuorum
            | SledExpungeRequestState::PoweringOff
            | SledExpungeRequestState::Expunging
            | SledExpungeRequestState::Done => match from {
                SledExpungeRequestUnfinishedState::WaitingForTrustQuorum => {
                    SledExpungeRequestTransition::Advance(
                        SledExpungeRequestAdvance::ToPoweringOff,
                    )
                }
                SledExpungeRequestUnfinishedState::PoweringOff => {
                    SledExpungeRequestTransition::Advance(
                        SledExpungeRequestAdvance::ToExpunging {
                            power_off_outcome:
                                SledExpungePowerOffOutcome::PoweredOff,
                        },
                    )
                }
                SledExpungeRequestUnfinishedState::Expunging => {
                    SledExpungeRequestTransition::Done
                }
            },
        };
        request = transition.columns(now).apply_to(request);
    }
    request
}

pub fn new_sled_expunge_request(
    rack_id: RackUuid,
    sled_id: SledUuid,
    baseboard_id: BaseboardId,
    tq_epoch: i64,
    state: SledExpungeRequestState,
) -> SledExpungeRequest {
    sled_expunge_request_with_state(
        SledExpungeRequest::new(rack_id, sled_id, baseboard_id, tq_epoch),
        state,
    )
}

pub async fn insert_sled_expunge_request(
    datastore: &DataStore,
    rack_id: RackUuid,
    baseboard_id: &BaseboardId,
    tq_epoch: i64,
    state: SledExpungeRequestState,
) -> SledExpungeRequest {
    insert_sled_expunge_request_for_sled(
        datastore,
        rack_id,
        SledUuid::new_v4(),
        baseboard_id,
        tq_epoch,
        state,
    )
    .await
}

pub async fn insert_sled_expunge_request_for_sled(
    datastore: &DataStore,
    rack_id: RackUuid,
    sled_id: SledUuid,
    baseboard_id: &BaseboardId,
    tq_epoch: i64,
    state: SledExpungeRequestState,
) -> SledExpungeRequest {
    let request = new_sled_expunge_request(
        rack_id,
        sled_id,
        baseboard_id.clone(),
        tq_epoch,
        state,
    );
    let conn = datastore
        .pool_connection_for_tests()
        .await
        .expect("got a database connection");
    diesel::insert_into(dsl::sled_expunge_request)
        .values(request)
        .returning(SledExpungeRequest::as_returning())
        .get_result_async(&*conn)
        .await
        .expect("inserted a sled expunge request")
}

// TODO-RAINCLAUDE: test-only forward move that may skip states, for tests that need a finished request without the real transitions (Done needs a sled and a trust quorum that pass the gate).
pub async fn set_sled_expunge_request_state(
    datastore: &DataStore,
    request: &SledExpungeRequest,
    state: SledExpungeRequestState,
) {
    let from = request.state;
    assert_ne!(
        from, state,
        "test moves a sled expunge request to a different state",
    );
    let updated = sled_expunge_request_with_state(request.clone(), state);
    let conn = datastore
        .pool_connection_for_tests()
        .await
        .expect("got a database connection");
    let n_updated = diesel::update(dsl::sled_expunge_request)
        .filter(dsl::id.eq(updated.id))
        .filter(dsl::state.eq(from))
        .set((
            dsl::state.eq(updated.state),
            dsl::power_off_outcome.eq(updated.power_off_outcome),
            dsl::failure_reason.eq(updated.failure_reason),
            dsl::last_error.eq(updated.last_error),
            dsl::time_completed.eq(updated.time_completed),
            dsl::time_modified.eq(updated.time_modified),
        ))
        .execute_async(&*conn)
        .await
        .expect("updated the sled expunge request's state");
    assert_eq!(
        n_updated, 1,
        "updated exactly one sled expunge request, still in state {from}",
    );
}

// TODO-RAINCLAUDE: moves time_modified an hour into the past, so a following write must strictly increase it whatever the clock resolution.
pub async fn backdate_sled_expunge_request(
    datastore: &DataStore,
    request: &SledExpungeRequest,
) -> SledExpungeRequest {
    let conn = datastore
        .pool_connection_for_tests()
        .await
        .expect("got a database connection");
    diesel::update(dsl::sled_expunge_request)
        .filter(dsl::id.eq(request.id))
        .set(
            dsl::time_modified
                .eq(request.time_modified - chrono::Duration::hours(1)),
        )
        .returning(SledExpungeRequest::as_returning())
        .get_result_async(&*conn)
        .await
        .expect("backdated time_modified")
}

pub async fn fail_sled_expunge_request(
    opctx: &OpContext,
    datastore: &DataStore,
    request: &SledExpungeRequest,
) {
    let from = request.state.as_unfinished().unwrap_or_else(|| {
        panic!(
            "test fails a sled expunge request that is unfinished (it is {})",
            request.state,
        )
    });
    let (authz_request,) = LookupPath::new(opctx, datastore)
        .sled_expunge_request(request.id())
        .lookup_for(authz::Action::Modify)
        .await
        .expect("looked up the sled expunge request");
    let outcome = datastore
        .sled_expunge_request_fail(
            opctx,
            &authz_request,
            from,
            TEST_FAILURE_REASON.to_string(),
        )
        .await
        .expect("failed the sled expunge request");
    let SledExpungeRequestUpdateOutcome::Updated { request: failed } = outcome
    else {
        panic!("failed the sled expunge request from {from}: {outcome:?}");
    };
    assert_eq!(failed.state, SledExpungeRequestState::Failed);
}
