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

/// Build a sled expunge request row, optionally recording
/// the given slot as the sled's cubby.
///
/// A real request also records the location the cubby number came from. But
/// that is only for debugging and not used as part of any decision making, so
/// this helper always records the source as inventory (which is the most common
/// situation).
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
