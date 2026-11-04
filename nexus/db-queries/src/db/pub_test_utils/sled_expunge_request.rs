// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::DataStore;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_model::SledExpungePowerOffOutcome;
use nexus_db_model::SledExpungeRequest;
use nexus_db_model::SledExpungeRequestState;
use nexus_db_schema::schema::sled_expunge_request::dsl;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;

pub const TEST_FAILURE_REASON: &str = "test failure";

// TODO-RAINCLAUDE: step 10b moves this per-state column mapping into one db-model function shared with the real transitions.
fn sled_expunge_request_with_state(
    request: SledExpungeRequest,
    state: SledExpungeRequestState,
) -> SledExpungeRequest {
    let now = now_db_precision();
    let power_off_outcome = match state {
        SledExpungeRequestState::WaitingForTrustQuorum
        | SledExpungeRequestState::PoweringOff => None,
        SledExpungeRequestState::Expunging => Some(
            request
                .power_off_outcome
                .unwrap_or(SledExpungePowerOffOutcome::PoweredOff),
        ),
        SledExpungeRequestState::Done => Some(
            request
                .power_off_outcome
                .unwrap_or(SledExpungePowerOffOutcome::PoweredOff),
        ),
        SledExpungeRequestState::Failed => request.power_off_outcome,
    };
    let failure_reason = match state {
        SledExpungeRequestState::WaitingForTrustQuorum
        | SledExpungeRequestState::PoweringOff
        | SledExpungeRequestState::Expunging
        | SledExpungeRequestState::Done => None,
        SledExpungeRequestState::Failed => {
            Some(TEST_FAILURE_REASON.to_string())
        }
    };
    let finished = state.is_finished();
    SledExpungeRequest {
        time_modified: now,
        state,
        power_off_outcome,
        failure_reason,
        time_completed: finished.then_some(now),
        last_error: if finished { None } else { request.last_error.clone() },
        ..request
    }
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
    let request = new_sled_expunge_request(
        rack_id,
        SledUuid::new_v4(),
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

fn state_order(state: SledExpungeRequestState) -> u8 {
    match state {
        SledExpungeRequestState::WaitingForTrustQuorum => 0,
        SledExpungeRequestState::PoweringOff => 1,
        SledExpungeRequestState::Expunging => 2,
        SledExpungeRequestState::Done | SledExpungeRequestState::Failed => 3,
    }
}

// TODO-RAINCLAUDE: test-only forward move that may skip states; the real compare-and-swap transitions land in step 10b.
pub async fn set_sled_expunge_request_state(
    datastore: &DataStore,
    request: &SledExpungeRequest,
    state: SledExpungeRequestState,
) {
    let from = request.state;
    assert!(
        !from.is_finished() && state_order(from) < state_order(state),
        "test moves a sled expunge request forward from an unfinished state \
         (requested {from} -> {state})",
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
