// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::DataStore;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_model::DbSledExpungePowerOffOutcome;
use nexus_db_model::SledExpungeRequest as DbSledExpungeRequest;
use nexus_db_schema::schema::sled_expunge_request::dsl;
use nexus_types::sled_expunge::SledExpungeRecordedCubby;
use nexus_types::sled_expunge::SledExpungeRequest;
use nexus_types::sled_expunge::SledExpungeRequestStateKind;
use nexus_types::sled_expunge::SledExpungeSpSlotSource;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use trust_quorum_types::types::Epoch;

pub const TEST_FAILURE_REASON: &str = "test failure";
pub const TEST_SP_SLOT: u16 = 0;

fn sled_expunge_request_with_state(
    request: DbSledExpungeRequest,
    state: SledExpungeRequestStateKind,
) -> DbSledExpungeRequest {
    let now = now_db_precision();
    let default_outcome = match request.sp_slot {
        Some(_) => DbSledExpungePowerOffOutcome::PoweredOff,
        None => DbSledExpungePowerOffOutcome::AbsentAtCreation,
    };
    let power_off_outcome = match state {
        SledExpungeRequestStateKind::WaitingForTrustQuorum
        | SledExpungeRequestStateKind::PoweringOff => None,
        SledExpungeRequestStateKind::Expunging
        | SledExpungeRequestStateKind::Done => {
            Some(request.power_off_outcome.unwrap_or(default_outcome))
        }
        SledExpungeRequestStateKind::Failed => request.power_off_outcome,
    };
    let failure_reason = match state {
        SledExpungeRequestStateKind::WaitingForTrustQuorum
        | SledExpungeRequestStateKind::PoweringOff
        | SledExpungeRequestStateKind::Expunging
        | SledExpungeRequestStateKind::Done => None,
        SledExpungeRequestStateKind::Failed => {
            Some(TEST_FAILURE_REASON.to_string())
        }
    };
    let finished = state.is_finished();
    DbSledExpungeRequest {
        time_modified: now,
        state: state.into(),
        power_off_outcome,
        failure_reason,
        time_completed: finished.then_some(now),
        last_error: if finished { None } else { request.last_error },
        ..request
    }
}

// TODO-RAINCLAUDE: Tests pick only a slot, so the request records it as found
// in inventory, which is where request creation looks first.
fn new_request_with_inventory_slot(
    rack_id: RackUuid,
    sled_id: SledUuid,
    baseboard_id: BaseboardId,
    sp_slot: Option<u16>,
    tq_epoch: Epoch,
) -> DbSledExpungeRequest {
    let cubby = sp_slot.map(|slot| SledExpungeRecordedCubby {
        slot,
        source: SledExpungeSpSlotSource::Inventory,
    });
    DbSledExpungeRequest::new(rack_id, sled_id, baseboard_id, cubby, tq_epoch)
        .expect("test epoch fits in the database")
}

pub fn new_sled_expunge_request(
    rack_id: RackUuid,
    sled_id: SledUuid,
    baseboard_id: BaseboardId,
    tq_epoch: Epoch,
    state: SledExpungeRequestStateKind,
) -> DbSledExpungeRequest {
    sled_expunge_request_with_state(
        new_request_with_inventory_slot(
            rack_id,
            sled_id,
            baseboard_id,
            Some(TEST_SP_SLOT),
            tq_epoch,
        ),
        state,
    )
}

fn decode(request: DbSledExpungeRequest) -> SledExpungeRequest {
    SledExpungeRequest::try_from(request)
        .unwrap_or_else(|error| panic!("decoded the request row: {error:#}"))
}

pub async fn insert_sled_expunge_request(
    datastore: &DataStore,
    rack_id: RackUuid,
    baseboard_id: BaseboardId,
    tq_epoch: Epoch,
    state: SledExpungeRequestStateKind,
) -> SledExpungeRequest {
    let request = new_sled_expunge_request(
        rack_id,
        SledUuid::new_v4(),
        baseboard_id,
        tq_epoch,
        state,
    );
    let conn = datastore
        .pool_connection_for_tests()
        .await
        .expect("got a database connection");
    let row = diesel::insert_into(dsl::sled_expunge_request)
        .values(request)
        .returning(DbSledExpungeRequest::as_returning())
        .get_result_async(&*conn)
        .await
        .expect("inserted a sled expunge request");
    decode(row)
}

fn state_order(state: SledExpungeRequestStateKind) -> u8 {
    match state {
        SledExpungeRequestStateKind::WaitingForTrustQuorum => 0,
        SledExpungeRequestStateKind::PoweringOff => 1,
        SledExpungeRequestStateKind::Expunging => 2,
        SledExpungeRequestStateKind::Done
        | SledExpungeRequestStateKind::Failed => 3,
    }
}

// TODO-RAINCLAUDE: The row is read back and compared with the caller's copy,
// so a stale copy trips an assert. The compare-and-swap then covers every
// column this writes or derives from (sp_slot picks the default outcome), so
// a write that lands between the read and the update trips the other assert
// instead of being overwritten. Returns the request as written.
pub async fn set_sled_expunge_request_state(
    datastore: &DataStore,
    request: &SledExpungeRequest,
    state: SledExpungeRequestStateKind,
) -> SledExpungeRequest {
    let from = request.state.kind();
    assert!(
        state_order(from) < state_order(state),
        "test moves a sled expunge request forward from an unfinished state \
         (requested {from} -> {state})",
    );
    let conn = datastore
        .pool_connection_for_tests()
        .await
        .expect("got a database connection");
    let current: DbSledExpungeRequest = dsl::sled_expunge_request
        .filter(dsl::id.eq(request.id.into_untyped_uuid()))
        .select(DbSledExpungeRequest::as_select())
        .get_result_async(&*conn)
        .await
        .expect("read the sled expunge request");
    assert_eq!(
        &decode(current.clone()),
        request,
        "the test's copy of the sled expunge request is current",
    );
    let updated = sled_expunge_request_with_state(current.clone(), state);
    let mut rows = diesel::update(dsl::sled_expunge_request)
        .filter(dsl::id.eq(current.id))
        .filter(dsl::state.eq(current.state))
        .filter(dsl::sp_slot.is_not_distinct_from(current.sp_slot))
        .filter(
            dsl::power_off_outcome
                .is_not_distinct_from(current.power_off_outcome),
        )
        .filter(
            dsl::failure_reason
                .is_not_distinct_from(current.failure_reason.clone()),
        )
        .filter(
            dsl::last_error.is_not_distinct_from(current.last_error.clone()),
        )
        .filter(
            dsl::time_completed.is_not_distinct_from(current.time_completed),
        )
        .set((
            dsl::state.eq(updated.state),
            dsl::power_off_outcome.eq(updated.power_off_outcome),
            dsl::failure_reason.eq(updated.failure_reason),
            dsl::last_error.eq(updated.last_error),
            dsl::time_completed.eq(updated.time_completed),
            dsl::time_modified.eq(updated.time_modified),
        ))
        .returning(DbSledExpungeRequest::as_returning())
        .get_results_async(&*conn)
        .await
        .expect("updated the sled expunge request's state");
    assert_eq!(
        rows.len(),
        1,
        "updated exactly one sled expunge request, still as the test last \
         saw it in state {from}",
    );
    decode(rows.pop().expect("exactly one row was updated"))
}
