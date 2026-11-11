// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::endpoints::HARDWARE_SLED_EXPUNGE_REQUESTS_URL;
use super::sleds::TrustQuorumConfigDriver;
use dropshot::test_util::ClientTestContext;
use gateway_test_utils::setup::GatewayTestConfigFiles;
use gateway_types::component::PowerState;
use gateway_types::component::SpType;
use http::Method;
use http::StatusCode;
use nexus_db_lookup::LookupPath;
use nexus_db_model::SledExpungePowerOffOutcome;
use nexus_db_queries::context::OpContext;
use nexus_lockstep_client::types::BackgroundTask;
use nexus_lockstep_client::types::LastResult;
use nexus_lockstep_client::types::LastResultCompleted;
use nexus_test_utils::ControlPlaneBuilder;
use nexus_test_utils::SimTrustQuorumMode;
use nexus_test_utils::background::activate_background_task_with_timeout;
use nexus_test_utils::background::run_inventory_collection;
use nexus_test_utils::background::run_inventory_loader;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::object_get;
use nexus_test_utils::trust_quorum::commission_fake_trust_quorum_member;
use nexus_test_utils::trust_quorum::initial_trust_quorum;
use nexus_types::external_api::rack::RackMembershipVersion;
use nexus_types::external_api::sled::SledExpungeParams;
use nexus_types::external_api::sled::SledExpungeRequest;
use nexus_types::external_api::sled::SledExpungeRequestDisposition;
use nexus_types::external_api::sled::SledExpungeRequestState;
use nexus_types::external_api::sled::SledExpungeResponse;
use nexus_types::external_api::sled::SledPolicy;
use nexus_types::internal_api::background::TrustQuorumManagerStatus;
use nexus_types::trust_quorum::TrustQuorumConfigState;
use omicron_sled_agent::sim;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;
use std::time::Duration;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// TODO-RAINCLAUDE: a sled_expunge activation powers a sled off through MGS, which can outlast the 10s default on a loaded machine.
const TASK_ACTIVATION_TIMEOUT: Duration = Duration::from_secs(60);

// TODO-RAINCLAUDE: every periodic activation that could race a test is pushed out an hour; creating a request still activates trust_quorum_manager and sled_expunge, and trust_quorum_manager wakes sled_expunge when a config leaves preparing, so tests assert on the state those activations converge to, after activating the tasks themselves.
fn builder(test_name: &str) -> ControlPlaneBuilder<'_> {
    ControlPlaneBuilder::new(test_name).customize_nexus_config(&|config| {
        let hour = Duration::from_secs(3600);
        let tasks = &mut config.pkg.background_tasks;
        tasks.sled_expunge.period_secs = hour;
        tasks.trust_quorum.period_secs = hour;
    })
}

fn request_url(id: SledExpungeRequestUuid) -> String {
    format!("{HARDWARE_SLED_EXPUNGE_REQUESTS_URL}/{id}")
}

// TODO-RAINCLAUDE: parses the body as the expunge response on 202, or as an error body (status and message) on any other status.
async fn sled_expunge(
    client: &ClientTestContext,
    sled_id: SledUuid,
    serial: &str,
) -> Result<SledExpungeResponse, (StatusCode, String)> {
    let url = format!("/v1/system/hardware/sleds/{sled_id}/expunge");
    let body =
        serde_json::to_value(SledExpungeParams { serial: serial.to_string() })
            .expect("serialized the expunge parameters");
    let response = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&body))
            .expect_status(None),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|error| panic!("sent request to {url}: {error}"));
    if response.status == StatusCode::ACCEPTED {
        Ok(response.parsed_body().unwrap_or_else(|error| {
            panic!("parsed the response from {url}: {error}")
        }))
    } else {
        let error = response
            .parsed_body::<dropshot::HttpErrorResponseBody>()
            .unwrap_or_else(|error| {
                panic!("parsed the error response from {url}: {error}")
            });
        Err((response.status, error.message))
    }
}

async fn view_request(
    client: &ClientTestContext,
    id: SledExpungeRequestUuid,
) -> SledExpungeRequest {
    object_get(client, &request_url(id)).await
}

async fn activate_task(
    cptestctx: &ControlPlaneTestContext,
    name: &str,
) -> LastResultCompleted {
    let task = activate_background_task_with_timeout(
        &cptestctx.lockstep_client,
        name,
        TASK_ACTIVATION_TIMEOUT,
    )
    .await;
    let LastResult::Completed(completed) = task.last else {
        panic!("{name} completed an activation: {:?}", task.last);
    };
    completed
}

// TODO-RAINCLAUDE: waits for the activation after `iteration` to complete, without activating the task; the caller knows what activated it.
async fn wait_for_activation_after(
    cptestctx: &ControlPlaneTestContext,
    name: &str,
    iteration: u64,
) -> LastResultCompleted {
    wait_for_condition(
        || async {
            let task = NexusRequest::object_get(
                &cptestctx.lockstep_client,
                &format!("/bgtasks/view/{name}"),
            )
            .execute_and_parse_unwrap::<BackgroundTask>()
            .await;
            match task.last {
                LastResult::Completed(completed)
                    if completed.iteration > iteration =>
                {
                    Ok(completed)
                }
                LastResult::Completed(_) | LastResult::NeverCompleted => {
                    Err(CondCheckError::<()>::NotYet { status: None })
                }
            }
        },
        &Duration::from_millis(50),
        &TASK_ACTIVATION_TIMEOUT,
    )
    .await
    .unwrap_or_else(|error| {
        panic!("{name} completed an activation after {iteration}: {error:?}")
    })
}

async fn sled_policy(
    cptestctx: &ControlPlaneTestContext,
    sled_id: SledUuid,
) -> SledPolicy {
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let (.., sled) = LookupPath::new(&opctx, datastore)
        .sled_id(sled_id)
        .fetch()
        .await
        .expect("looked up the sled");
    sled.policy()
}

async fn request_row(
    cptestctx: &ControlPlaneTestContext,
    id: SledExpungeRequestUuid,
) -> nexus_db_model::SledExpungeRequest {
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    datastore
        .sled_expunge_request_get(&opctx, id)
        .await
        .expect("read the request row")
}

// TODO-RAINCLAUDE: the cubby's SP power state on each switch's sp-sim, as that switch's MGS reports it.
async fn sp_power_states(
    cptestctx: &ControlPlaneTestContext,
    cubby: u16,
) -> BTreeMap<SocketAddrV6, PowerState> {
    let mut states = BTreeMap::new();
    for gateway in cptestctx.gateway.values() {
        let state = gateway
            .client
            .sp_get(&SpType::Sled, cubby)
            .await
            .expect("MGS answered for the sled cubby")
            .into_inner();
        states.insert(gateway.address(), state.power_state);
    }
    states
}

// TODO-RAINCLAUDE: send_uncertain is not in the view; the row records it for support.
async fn assert_new_request(
    cptestctx: &ControlPlaneTestContext,
    response: &SledExpungeResponse,
    rack_id: RackUuid,
    sled_id: SledUuid,
    baseboard: &BaseboardId,
    version: u64,
    send_uncertain: bool,
) {
    assert_eq!(
        response.disposition,
        SledExpungeRequestDisposition::Created,
        "{response:?}",
    );
    let request = &response.request;
    // TODO-RAINCLAUDE: creating the request activates the tasks, which may move it on before the response is built, so its state and the fields that follow it are carried over.
    assert_eq!(
        *request,
        SledExpungeRequest {
            rack_id,
            sled_id,
            baseboard: baseboard.clone(),
            rack_membership_version: RackMembershipVersion(version),
            ..request.clone()
        },
    );
    assert_eq!(
        request_row(cptestctx, request.id).await.send_uncertain,
        send_uncertain,
        "{response:?}",
    );
}

// TODO-RAINCLAUDE: the standard sp-sim/MGS config with fake trust quorum members: sleds 0 and 1 are scrimlets and sled 2 a gimlet with no SP; no sim sled answers trust quorum requests (405), and configs are driven through the datastore, so the trust quorum coordinator the removal picks is controlled by which members acked the base config's commit.
#[tokio::test]
async fn test_sled_expunge_dispositions() {
    let cptestctx = builder("test_sled_expunge_dispositions")
        .with_extra_sled_agents(2)
        .start::<omicron_nexus::Server>()
        .await;
    let client = &cptestctx.external_client;
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let rack_id = cptestctx.server.server_context().nexus.rack_id();
    let tq = TrustQuorumConfigDriver { opctx: &opctx, datastore, rack_id };

    // TODO-RAINCLAUDE: inserts hw_baseboard_id rows for the sim sleds, so they can become trust quorum members, and gives the scrimlet check a collection.
    run_inventory_collection(&cptestctx.lockstep_client).await;

    let [first, second, target]: [&sim::Server; 3] = cptestctx
        .all_sled_agents()
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|_| panic!("the harness runs three sim sleds"));
    let target_id = target.sled_agent.id;
    let target_bb = target.sled_agent.baseboard_id();
    let first_bb = first.sled_agent.baseboard_id();
    let second_bb = second.sled_agent.baseboard_id();
    let serial = target_bb.serial_number.as_str();

    // TODO-RAINCLAUDE: epoch 1's members are fakes, and only a member with an in-service sled can coordinate epoch 2, so give fake_a a sled row (with no sled agent behind it).
    let fake_a =
        commission_fake_trust_quorum_member(&cptestctx).await.baseboard_id;
    // TODO-RAINCLAUDE: only the two scrimlets and the target ack epoch 2's commit (3 of 4 meets K + Z), so the removal's coordinator is a scrimlet, whose sim sled agent answers 405: the send failed, so the request is created with send_uncertain recorded and its removal stays preparing.
    let with_scrimlets = BTreeSet::from([
        target_bb.clone(),
        first_bb.clone(),
        second_bb.clone(),
        fake_a,
    ]);
    tq.propose(2, 1, &with_scrimlets).await;
    tq.ack_all_prepares(2).await;
    tq.ack_commits(
        2,
        &BTreeSet::from([
            target_bb.clone(),
            first_bb.clone(),
            second_bb.clone(),
        ]),
        TrustQuorumConfigState::Committing,
    )
    .await;
    let uncertain = sled_expunge(client, target_id, serial)
        .await
        .expect("created a request");
    assert_new_request(
        &cptestctx, &uncertain, rack_id, target_id, &target_bb, 3, true,
    )
    .await;
    let removal = tq.config(3).await;
    assert_eq!(removal.state, TrustQuorumConfigState::Preparing);
    assert!(
        [&first_bb, &second_bb].contains(&&removal.coordinator),
        "coordinator {} is a scrimlet",
        removal.coordinator,
    );

    activate_task(&cptestctx, "sled_expunge").await;
    let waiting = view_request(client, uncertain.request.id).await;
    assert_eq!(waiting, uncertain.request, "nothing has changed the request");
    assert_eq!(
        waiting,
        SledExpungeRequest {
            state: SledExpungeRequestState::WaitingForTrustQuorum,
            last_error: None,
            failure_reason: None,
            time_completed: None,
            ..waiting.clone()
        },
    );
    assert_eq!(
        sled_expunge(client, target_id, serial)
            .await
            .expect("replayed the request"),
        SledExpungeResponse {
            request: waiting,
            disposition: SledExpungeRequestDisposition::AlreadyOutstanding,
        },
    );

    cptestctx.teardown().await;
}

// TODO-RAINCLAUDE: end to end on the five-gimlet sp-sim config with the fake sim trust quorum: sim sleds 2-4 are gimlets whose SPs sit in sled cubbies 2-4 on both switches' MGS, each MGS in front of its own sp-sim.
#[tokio::test]
async fn test_sled_expunge_end_to_end() {
    let cptestctx = builder("test_sled_expunge_end_to_end")
        .with_extra_sled_agents(4)
        .with_sim_trust_quorum(SimTrustQuorumMode::Fake)
        .with_sp_sim_config(GatewayTestConfigFiles::five_gimlets())
        .start::<omicron_nexus::Server>()
        .await;
    let client = &cptestctx.external_client;
    let rack_id = initial_trust_quorum(&cptestctx).await.config.rack_id;
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let tq = TrustQuorumConfigDriver { opctx: &opctx, datastore, rack_id };

    // TODO-RAINCLAUDE: waits out the startup activations, and gives the scrimlet check and the power-off a loaded collection that knows every sim sled's role and SP cubby.
    let manager_before =
        activate_task(&cptestctx, "trust_quorum_manager").await;
    activate_task(&cptestctx, "sled_expunge").await;
    run_inventory_collection(&cptestctx.lockstep_client).await;
    run_inventory_loader(&cptestctx.lockstep_client).await;

    let target = cptestctx
        .all_sled_agents()
        .nth(2)
        .expect("the harness runs a sim sled for cubby 2");
    let target_id = target.sled_agent.id;
    let target_bb = target.sled_agent.baseboard_id();
    assert_eq!(target_bb.serial_number, "SimGimlet02");

    let created = sled_expunge(client, target_id, &target_bb.serial_number)
        .await
        .expect("created a request");
    assert_new_request(
        &cptestctx, &created, rack_id, target_id, &target_bb, 2, false,
    )
    .await;

    // TODO-RAINCLAUDE: nothing else activates trust_quorum_manager here, so the activation after manager_before is the one the create started; it alone must take the removal from preparing to committed.
    let manager = wait_for_activation_after(
        &cptestctx,
        "trust_quorum_manager",
        manager_before.iteration,
    )
    .await;
    assert_eq!(manager.iteration, manager_before.iteration + 1);
    match serde_json::from_value(manager.details.clone())
        .expect("parsed the trust_quorum_manager status")
    {
        TrustQuorumManagerStatus::PerRackStatus { errors, .. } => {
            assert_eq!(errors, Vec::<String>::new(), "{}", manager.details);
        }
        TrustQuorumManagerStatus::Error(error) => {
            panic!("trust_quorum_manager failed: {error}");
        }
    }
    assert_eq!(tq.config(2).await.state, TrustQuorumConfigState::Committed);

    activate_task(&cptestctx, "sled_expunge").await;
    let done = view_request(client, created.request.id).await;
    // TODO-RAINCLAUDE: the last transition leaves every column the view shows as before but the state, time_completed and time_modified (set to the same time), with no last_error or failure_reason.
    assert_eq!(
        done,
        SledExpungeRequest {
            state: SledExpungeRequestState::Done,
            failure_reason: None,
            last_error: None,
            time_completed: Some(done.time_modified),
            time_modified: done.time_modified,
            ..created.request.clone()
        },
    );
    assert_eq!(sled_policy(&cptestctx, target_id).await, SledPolicy::Expunged);
    assert_eq!(
        request_row(&cptestctx, created.request.id).await.power_off_outcome,
        Some(SledExpungePowerOffOutcome::PoweredOff),
    );
    // TODO-RAINCLAUDE: A2 goes through one MGS only, so exactly one switch's sp-sim shows cubby 2 in A2.
    let states = sp_power_states(&cptestctx, 2).await;
    let count_in = |power_state: PowerState| {
        states.values().filter(|state| **state == power_state).count()
    };
    assert_eq!(
        (count_in(PowerState::A0), count_in(PowerState::A2)),
        (1, 1),
        "{states:?}",
    );

    // TODO-RAINCLAUDE: one request per page, so the list takes a second page to end.
    let listed = NexusRequest::iter_collection_authn::<SledExpungeRequest>(
        client,
        HARDWARE_SLED_EXPUNGE_REQUESTS_URL,
        "",
        Some(1),
    )
    .await
    .expect("listed sled expunge requests")
    .all_items;
    assert_eq!(listed, [done]);

    cptestctx.teardown().await;
}
