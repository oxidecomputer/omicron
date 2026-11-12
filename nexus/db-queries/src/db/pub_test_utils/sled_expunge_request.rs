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
use nexus_db_model::SledExpungeRequest as DbSledExpungeRequest;
use nexus_db_model::SledExpungeRequestTransitionColumns;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_schema::schema::sled_expunge_request::dsl;
use nexus_types::sled_expunge::SledExpungeCubbyPowerOffOutcome;
use nexus_types::sled_expunge::SledExpungePowerOff;
use nexus_types::sled_expunge::SledExpungeRecordedCubby;
use nexus_types::sled_expunge::SledExpungeRequest;
use nexus_types::sled_expunge::SledExpungeRequestAdvance;
use nexus_types::sled_expunge::SledExpungeRequestStateKind;
use nexus_types::sled_expunge::SledExpungeRequestTransition;
use nexus_types::sled_expunge::SledExpungeRequestUnfinishedState;
use nexus_types::sled_expunge::SledExpungeSpSlotSource;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use trust_quorum_types::types::Epoch;

pub const TEST_FAILURE_REASON: &str = "test failure";
pub const TEST_SP_SLOT: u16 = 0;

// TODO-RAINCLAUDE: Returns the real transitions that take a request from `from`
// to `state`. The power-off step records `PoweredOff` at `cubby`, or
// `AbsentAtCreation` when there is no `cubby`.
fn sled_expunge_request_transitions(
    from: SledExpungeRequestStateKind,
    cubby: Option<SledExpungeRecordedCubby>,
    state: SledExpungeRequestStateKind,
) -> Vec<SledExpungeRequestTransition> {
    let mut transitions = Vec::new();
    let mut current = from;
    while current != state {
        let unfinished = current.as_unfinished().unwrap_or_else(|| {
            panic!(
                "test moves a sled expunge request forward from an unfinished \
                 state (requested {from} -> {state})",
            )
        });
        let (transition, next) = match state {
            SledExpungeRequestStateKind::Failed => (
                SledExpungeRequestTransition::Failed {
                    from: unfinished,
                    failure_reason: TEST_FAILURE_REASON.to_string(),
                },
                SledExpungeRequestStateKind::Failed,
            ),
            SledExpungeRequestStateKind::WaitingForTrustQuorum
            | SledExpungeRequestStateKind::PoweringOff
            | SledExpungeRequestStateKind::Expunging
            | SledExpungeRequestStateKind::Done => match unfinished {
                SledExpungeRequestUnfinishedState::WaitingForTrustQuorum => (
                    SledExpungeRequestTransition::Advance(
                        SledExpungeRequestAdvance::ToPoweringOff,
                    ),
                    SledExpungeRequestStateKind::PoweringOff,
                ),
                SledExpungeRequestUnfinishedState::PoweringOff => {
                    let power_off = match cubby {
                        Some(cubby) => SledExpungePowerOff::Cubby {
                            cubby,
                            outcome:
                                SledExpungeCubbyPowerOffOutcome::PoweredOff,
                        },
                        None => SledExpungePowerOff::AbsentAtCreation,
                    };
                    (
                        SledExpungeRequestTransition::Advance(
                            SledExpungeRequestAdvance::ToExpunging {
                                power_off,
                            },
                        ),
                        SledExpungeRequestStateKind::Expunging,
                    )
                }
                SledExpungeRequestUnfinishedState::Expunging => (
                    SledExpungeRequestTransition::Done,
                    SledExpungeRequestStateKind::Done,
                ),
            },
        };
        transitions.push(transition);
        current = next;
    }
    transitions
}

fn sled_expunge_request_with_state(
    request: DbSledExpungeRequest,
    cubby: Option<SledExpungeRecordedCubby>,
    state: SledExpungeRequestStateKind,
) -> DbSledExpungeRequest {
    let now = now_db_precision();
    sled_expunge_request_transitions(request.state.into(), cubby, state)
        .into_iter()
        .fold(request, |request, transition| {
            SledExpungeRequestTransitionColumns::new(transition, now)
                .apply_to(request)
        })
}

pub(crate) fn new_sled_expunge_request(
    rack_id: RackUuid,
    sled_id: SledUuid,
    baseboard_id: BaseboardId,
    tq_epoch: Epoch,
    state: SledExpungeRequestStateKind,
) -> DbSledExpungeRequest {
    new_sled_expunge_request_with_sp_slot(
        rack_id,
        sled_id,
        baseboard_id,
        Some(TEST_SP_SLOT),
        tq_epoch,
        state,
    )
}

// TODO-RAINCLAUDE: Tests pick only a slot, so the request records it as found
// in inventory, which is where request creation looks first.
pub(crate) fn new_sled_expunge_request_with_sp_slot(
    rack_id: RackUuid,
    sled_id: SledUuid,
    baseboard_id: BaseboardId,
    sp_slot: Option<u16>,
    tq_epoch: Epoch,
    state: SledExpungeRequestStateKind,
) -> DbSledExpungeRequest {
    let cubby = sp_slot.map(|slot| SledExpungeRecordedCubby {
        slot,
        source: SledExpungeSpSlotSource::Inventory,
    });
    let request = DbSledExpungeRequest::new(
        rack_id,
        sled_id,
        baseboard_id,
        cubby,
        tq_epoch,
    )
    .expect("test epoch fits in the database");
    sled_expunge_request_with_state(request, cubby, state)
}

pub(crate) fn decode(request: DbSledExpungeRequest) -> SledExpungeRequest {
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
    insert_request(
        datastore,
        new_sled_expunge_request(
            rack_id,
            SledUuid::new_v4(),
            baseboard_id,
            tq_epoch,
            state,
        ),
    )
    .await
}

pub async fn insert_sled_expunge_request_for_sled(
    datastore: &DataStore,
    rack_id: RackUuid,
    sled_id: SledUuid,
    baseboard_id: &BaseboardId,
    tq_epoch: Epoch,
    state: SledExpungeRequestStateKind,
) -> SledExpungeRequest {
    insert_request(
        datastore,
        new_sled_expunge_request(
            rack_id,
            sled_id,
            baseboard_id.clone(),
            tq_epoch,
            state,
        ),
    )
    .await
}

pub async fn insert_sled_expunge_request_with_sp_slot(
    datastore: &DataStore,
    rack_id: RackUuid,
    baseboard_id: &BaseboardId,
    sp_slot: Option<u16>,
    tq_epoch: Epoch,
    state: SledExpungeRequestStateKind,
) -> SledExpungeRequest {
    insert_request(
        datastore,
        new_sled_expunge_request_with_sp_slot(
            rack_id,
            SledUuid::new_v4(),
            baseboard_id.clone(),
            sp_slot,
            tq_epoch,
            state,
        ),
    )
    .await
}

async fn insert_request(
    datastore: &DataStore,
    request: DbSledExpungeRequest,
) -> SledExpungeRequest {
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

// TODO-RAINCLAUDE: Moves a request forward to `state` without going through the
// datastore's gated paths. Tests use this to get a finished request without
// setting up a sled and a trust quorum that pass the task gate.
pub async fn set_sled_expunge_request_state(
    datastore: &DataStore,
    request: &SledExpungeRequest,
    state: SledExpungeRequestStateKind,
) -> SledExpungeRequest {
    let from = request.state.kind();
    assert_ne!(
        from, state,
        "test moves a sled expunge request to a different state",
    );
    let transitions = sled_expunge_request_transitions(
        from,
        request.state.recorded_cubby(),
        state,
    );
    let conn = datastore
        .pool_connection_for_tests()
        .await
        .expect("got a database connection");
    let mut updated = request.clone();
    for transition in transitions {
        let step_from = transition.from_state();
        let row = DataStore::sled_expunge_request_transition_conn(
            &conn,
            request.id,
            transition,
            now_db_precision(),
        )
        .await
        .expect("updated the sled expunge request's state")
        .unwrap_or_else(|| {
            panic!(
                "updated the sled expunge request, still in state {step_from} \
                 with the cubby the test last saw (requested {from} -> \
                 {state})",
            )
        });
        updated = decode(row);
    }
    updated
}

// TODO-RAINCLAUDE: Moves `time_modified` an hour into the past, so that a test
// can assert that a later write strictly increases it, whatever the clock's
// resolution.
pub async fn backdate_sled_expunge_request(
    datastore: &DataStore,
    request: &SledExpungeRequest,
) -> SledExpungeRequest {
    let conn = datastore
        .pool_connection_for_tests()
        .await
        .expect("got a database connection");
    let row = diesel::update(dsl::sled_expunge_request)
        .filter(dsl::id.eq(to_db_typed_uuid(request.id)))
        .set(
            dsl::time_modified
                .eq(request.time_modified - chrono::Duration::hours(1)),
        )
        .returning(DbSledExpungeRequest::as_returning())
        .get_result_async(&*conn)
        .await
        .expect("backdated time_modified");
    decode(row)
}

pub async fn fail_sled_expunge_request(
    opctx: &OpContext,
    datastore: &DataStore,
    request: &SledExpungeRequest,
) {
    let from = request.state.kind().as_unfinished().unwrap_or_else(|| {
        panic!(
            "test fails a sled expunge request that is unfinished (it is {})",
            request.state.kind(),
        )
    });
    let (authz_request,) = LookupPath::new(opctx, datastore)
        .sled_expunge_request(request.id)
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
    assert_eq!(failed.state.kind(), SledExpungeRequestStateKind::Failed);
}
