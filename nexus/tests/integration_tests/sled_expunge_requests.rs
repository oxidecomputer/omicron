// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::endpoints::HARDWARE_SLED_EXPUNGE_REQUESTS_URL;
use dropshot::test_util::ClientTestContext;
use gateway_test_utils::setup::sim_gimlet_serial;
use gateway_types::component::PowerState;
use gateway_types::component::SpIdentifier;
use gateway_types::component::SpType;
use gateway_types::ignition::SpIgnition;
use http::Method;
use http::StatusCode;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pub_test_utils::trust_quorum::TrustQuorumConfigDriver;
use nexus_lockstep_client::types::LastResult;
use nexus_lockstep_client::types::LastResultCompleted;
use nexus_test_utils::ControlPlaneBuilder;
use nexus_test_utils::background::activate_background_task_with_timeout;
use nexus_test_utils::background::run_inventory_collection;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::object_get;
use nexus_test_utils::trust_quorum::commission_fake_trust_quorum_member;
use nexus_types::external_api::rack::RackMembershipVersion;
use nexus_types::external_api::sled::SledExpungeCubby;
use nexus_types::external_api::sled::SledExpungeCubbySource;
use nexus_types::external_api::sled::SledExpungeParams;
use nexus_types::external_api::sled::SledExpungePowerOffOutcome;
use nexus_types::external_api::sled::SledExpungeRequest;
use nexus_types::external_api::sled::SledExpungeRequestDisposition;
use nexus_types::external_api::sled::SledExpungeRequestState;
use nexus_types::external_api::sled::SledExpungeResponse;
use nexus_types::external_api::sled::SledPolicy;
use nexus_types::trust_quorum::TrustQuorumConfigState;
use omicron_sled_agent::sim;
use omicron_sled_agent::sim::SledCpuFamily;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use sp_sim::Responsiveness;
use sp_sim::SimulatedSp;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;
use std::time::Duration;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// TODO-RAINCLAUDE: An activation of `sled_expunge` powers a sled off through
// MGS, which can take longer than the default timeout of 10 seconds on a loaded
// machine.
const TASK_ACTIVATION_TIMEOUT: Duration = Duration::from_secs(60);

// TODO-RAINCLAUDE: Returns a builder with every periodic activation that could
// race a test pushed out to an hour. The tests activate the tasks themselves.
fn builder(test_name: &str) -> ControlPlaneBuilder<'_> {
    ControlPlaneBuilder::new(test_name).customize_nexus_config(&|config| {
        let hour = Duration::from_secs(3600);
        let tasks = &mut config.pkg.background_tasks;
        tasks.sled_expunge.period_secs = hour;
        tasks.trust_quorum.period_secs = hour;
        tasks.inventory.period_secs_collect = hour;
    })
}

fn request_url(id: SledExpungeRequestUuid) -> String {
    format!("{HARDWARE_SLED_EXPUNGE_REQUESTS_URL}/{id}")
}

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

// TODO-RAINCLAUDE: Runs an inventory collection, and gives a sled row to a fake
// trust quorum member taken from the epoch 1 config. The tests need the fake
// member because the coordinator of a new config must have committed the last
// committed config, which at this point is epoch 1, and the sim sleds aren't
// members of it.
async fn collect_inventory_and_commission_fake_member(
    cptestctx: &ControlPlaneTestContext,
) -> BaseboardId {
    run_inventory_collection(&cptestctx.lockstep_client).await;
    commission_fake_trust_quorum_member(cptestctx).await.baseboard_id
}

#[derive(Debug, PartialEq, Eq)]
enum CubbyState {
    Powered(PowerState),
    Unpowered,
}

async fn cubby_states(
    cptestctx: &ControlPlaneTestContext,
    cubby: u16,
) -> BTreeMap<SocketAddrV6, CubbyState> {
    let mut states = BTreeMap::new();
    for gateway in cptestctx.gateway.values() {
        let client = &gateway.client;
        let id = SpIdentifier { typ: SpType::Sled, slot: cubby };
        let ignition = client
            .ignition_list()
            .await
            .expect("MGS answered the ignition request")
            .into_inner()
            .into_iter()
            .find(|info| info.id == id)
            .unwrap_or_else(|| panic!("ignition reports sled cubby {cubby}"));
        let state = match ignition.details {
            SpIgnition::Present { power: true, .. } => CubbyState::Powered(
                client
                    .sp_get(&SpType::Sled, cubby)
                    .await
                    .expect("MGS answered for the powered sled cubby")
                    .into_inner()
                    .power_state,
            ),
            SpIgnition::Present { power: false, .. } => {
                let error = client
                    .sp_get(&SpType::Sled, cubby)
                    .await
                    .expect_err("an unpowered SP does not answer");
                assert_eq!(
                    error.status(),
                    Some(StatusCode::SERVICE_UNAVAILABLE),
                    "{error}",
                );
                CubbyState::Unpowered
            }
            SpIgnition::Absent => {
                panic!("ignition reports sled cubby {cubby} absent")
            }
        };
        states.insert(gateway.address(), state);
    }
    states
}

fn assert_new_request(
    response: &SledExpungeResponse,
    rack_id: RackUuid,
    sled_id: SledUuid,
    baseboard: &BaseboardId,
    cubby: Option<SledExpungeCubby>,
    version: u64,
) {
    assert_eq!(
        response.disposition,
        SledExpungeRequestDisposition::Created,
        "{response:?}",
    );
    let request = &response.request;
    assert_eq!(
        *request,
        SledExpungeRequest {
            id: request.id,
            rack_id,
            sled_id,
            baseboard_id: baseboard.clone(),
            status: SledExpungeRequestState::WaitingForTrustQuorum {
                cubby,
                last_error: None,
            },
            rack_membership_version: RackMembershipVersion(version),
            time_created: request.time_created,
            time_modified: request.time_created,
        },
    );
}

// TODO-RAINCLAUDE: Shows that a request is still created when the send to the
// removal's coordinator fails, and that a replay returns the outstanding
// request.
//
// No sim sled answers trust quorum requests (the sim sled agent returns a 405),
// so the tests in this file drive trust quorum configs through the datastore.
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

    let fake_a = collect_inventory_and_commission_fake_member(&cptestctx).await;

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

    // TODO-RAINCLAUDE: Only the two scrimlets and the target ack the commit of
    // epoch 2, so the removal's coordinator is one of the scrimlets. The send
    // to it fails, and the request is created anyway, with its removal left in
    // `Preparing`.
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
        BTreeSet::from([
            target_bb.clone(),
            first_bb.clone(),
            second_bb.clone(),
        ]),
        TrustQuorumConfigState::Committing,
    )
    .await;
    let failed_send = sled_expunge(client, target_id, serial)
        .await
        .expect("created a request");
    assert_new_request(
        &failed_send,
        rack_id,
        target_id,
        &target_bb,
        Some(SledExpungeCubby {
            slot: 2,
            source: SledExpungeCubbySource::Inventory,
        }),
        3,
    );
    let removal = tq.config(3).await;
    assert_eq!(removal.state, TrustQuorumConfigState::Preparing);
    assert!(
        [&first_bb, &second_bb].contains(&&removal.coordinator),
        "coordinator {} is a scrimlet",
        removal.coordinator,
    );

    activate_task(&cptestctx, "sled_expunge").await;
    let waiting = view_request(client, failed_send.request.id).await;
    assert_eq!(waiting, failed_send.request, "nothing has changed the request");
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

// TODO-RAINCLAUDE: Shows a request going from creation, through the power-off,
// to the sled being expunged. The send to the removal's coordinator fails,
// because no member answers trust quorum requests, so the test commits the
// removal itself.
#[tokio::test]
async fn test_sled_expunge_end_to_end() {
    let cptestctx = builder("test_sled_expunge_end_to_end")
        .with_extra_sled_agents(4)
        .start::<omicron_nexus::Server>()
        .await;
    let client = &cptestctx.external_client;
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let rack_id = cptestctx.server.server_context().nexus.rack_id();
    let tq = TrustQuorumConfigDriver { opctx: &opctx, datastore, rack_id };

    let fake_a = collect_inventory_and_commission_fake_member(&cptestctx).await;

    let target = cptestctx
        .all_sled_agents()
        .nth(2)
        .expect("the harness runs a sim sled for cubby 2");
    let target_id = target.sled_agent.id;
    let target_bb = target.sled_agent.baseboard_id();
    assert_eq!(target_bb.serial_number, sim_gimlet_serial(2));

    let members: BTreeSet<_> = cptestctx
        .all_sled_agents()
        .map(|server| server.sled_agent.baseboard_id())
        .chain([fake_a])
        .collect();
    tq.propose(2, 1, &members).await;
    tq.commit(2).await;

    let created = sled_expunge(client, target_id, &target_bb.serial_number)
        .await
        .expect("created a request");
    assert_new_request(
        &created,
        rack_id,
        target_id,
        &target_bb,
        Some(SledExpungeCubby {
            slot: 2,
            source: SledExpungeCubbySource::Inventory,
        }),
        3,
    );
    assert_eq!(tq.config(3).await.state, TrustQuorumConfigState::Preparing);
    tq.commit(3).await;

    activate_task(&cptestctx, "sled_expunge").await;
    let done = view_request(client, created.request.id).await;
    assert_eq!(
        done,
        SledExpungeRequest {
            status: SledExpungeRequestState::Done {
                power_off: SledExpungePowerOffOutcome::PoweredOff {
                    cubby: SledExpungeCubby {
                        slot: 2,
                        source: SledExpungeCubbySource::Inventory,
                    },
                },
                time_completed: done.time_modified,
            },
            time_modified: done.time_modified,
            ..created.request.clone()
        },
    );
    assert_eq!(sled_policy(&cptestctx, target_id).await, SledPolicy::Expunged);
    // TODO-RAINCLAUDE: Both MGSes talk to the same simulated rack, so both see
    // cubby 2 unpowered even though the power-off went through only one of
    // them. Cubby 3 is still powered, which shows that the power-off hit only
    // the target.
    let states = cubby_states(&cptestctx, 2).await;
    assert_eq!(
        states.values().collect::<Vec<_>>(),
        [&CubbyState::Unpowered, &CubbyState::Unpowered],
        "{states:?}",
    );
    let sibling_states = cubby_states(&cptestctx, 3).await;
    assert_eq!(
        sibling_states.values().collect::<Vec<_>>(),
        [
            &CubbyState::Powered(PowerState::A0),
            &CubbyState::Powered(PowerState::A0)
        ],
        "{sibling_states:?}",
    );

    // TODO-RAINCLAUDE: With one request per page, the listing takes a second
    // page to finish, which exercises pagination.
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

// TODO-RAINCLAUDE: Shows that a sled missing from the latest inventory
// collection, whose SP still answers in a powered cubby, is found by the live
// scan at creation, and that the request records the live scan as the cubby's
// source.
#[tokio::test]
async fn test_sled_expunge_live_scan_at_creation() {
    let mut cptestctx = builder("test_sled_expunge_live_scan_at_creation")
        .with_extra_sled_agents(2)
        .start::<omicron_nexus::Server>()
        .await;
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let rack_id = cptestctx.server.server_context().nexus.rack_id();
    let tq = TrustQuorumConfigDriver { opctx: &opctx, datastore, rack_id };

    let fake_a = collect_inventory_and_commission_fake_member(&cptestctx).await;

    let target = cptestctx.sled_agents.remove(2);
    let target_id = target.sled_agent_id();
    let target_bb = target.sled_agent().baseboard_id();
    assert_eq!(target_bb.serial_number, sim_gimlet_serial(2));

    let members: BTreeSet<_> = cptestctx
        .all_sled_agents()
        .map(|server| server.sled_agent.baseboard_id())
        .chain([target_bb.clone(), fake_a])
        .collect();
    tq.propose(2, 1, &members).await;
    tq.commit(2).await;

    // TODO-RAINCLAUDE: Without cubby 2's SP in the next collection, creation
    // can't take the cubby from inventory. If the target's sled agent were
    // still in it, creation would refuse with a 409 instead of running the live
    // scan, so the sled agent is stopped for good. The SP is silent only while
    // the collection runs, so it answers the live scan's `sp_get` afterwards.
    // Nothing activates another collection before the create call, because
    // blueprint execution is disabled in tests and `builder` sets the
    // collection period to an hour.
    target.teardown().await;
    let simrack = &cptestctx
        .gateway
        .values()
        .next()
        .expect("the harness runs an MGS")
        .simrack;
    simrack.gimlets[2].set_responsiveness(Responsiveness::Unresponsive).await;
    run_inventory_collection(&cptestctx.lockstep_client).await;
    simrack.gimlets[2].set_responsiveness(Responsiveness::Responsive).await;

    let created = sled_expunge(
        &cptestctx.external_client,
        target_id,
        &target_bb.serial_number,
    )
    .await
    .expect("created a request");
    assert_new_request(
        &created,
        rack_id,
        target_id,
        &target_bb,
        Some(SledExpungeCubby {
            slot: 2,
            source: SledExpungeCubbySource::LiveScan,
        }),
        3,
    );

    cptestctx.teardown().await;
}

// TODO-RAINCLAUDE: Shows a request for a sled that is in no sled cubby, from
// creation through to the sled being expunged. The sled added here has a sled
// agent but no SP in the simulated rack, so until its sled agent is stopped, it
// also covers the refusal of a sled agent that is in inventory without its SP.
#[tokio::test]
async fn test_sled_expunge_gone_at_creation() {
    let cptestctx = builder("test_sled_expunge_gone_at_creation")
        .with_extra_sled_agents(2)
        .start::<omicron_nexus::Server>()
        .await;
    let client = &cptestctx.external_client;
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let rack_id = cptestctx.server.server_context().nexus.rack_id();
    let tq = TrustQuorumConfigDriver { opctx: &opctx, datastore, rack_id };

    let target_id = SledUuid::new_v4();
    let target = cptestctx
        .add_sled(target_id, sim::SimMode::Explicit, SledCpuFamily::AmdMilan)
        .await;
    let target_bb = target.sled_agent.baseboard_id();
    let serial = target_bb.serial_number.clone();

    let fake_a =
        commission_fake_trust_quorum_member(&cptestctx).await.baseboard_id;
    let members: BTreeSet<_> = cptestctx
        .all_sled_agents()
        .map(|server| server.sled_agent.baseboard_id())
        .chain([target_bb.clone(), fake_a])
        .collect();
    tq.propose(2, 1, &members).await;
    tq.commit(2).await;

    let collection_id =
        run_inventory_collection(&cptestctx.lockstep_client).await;
    assert_eq!(
        sled_expunge(client, target_id, &serial).await,
        Err((
            StatusCode::CONFLICT,
            format!(
                "Cannot expunge sled {target_id}: the latest inventory \
                 collection ({collection_id}) has a record from its sled \
                 agent but none from its service processor, so the cubby it \
                 must be powered off in is unknown. Retry after the next \
                 inventory collection; if this persists, check that the \
                 sled's service processor is reachable, or contact Oxide \
                 support."
            ),
        )),
    );

    target
        .http_server
        .close()
        .await
        .expect("shut down the target's sled agent server");
    let simrack = &cptestctx
        .gateway
        .values()
        .next()
        .expect("the harness runs an MGS")
        .simrack;
    simrack.gimlets[2].set_responsiveness(Responsiveness::Unresponsive).await;
    run_inventory_collection(&cptestctx.lockstep_client).await;
    assert_eq!(
        sled_expunge(client, target_id, &serial).await,
        Err((
            StatusCode::CONFLICT,
            format!(
                "Cannot expunge sled {target_id}: neither its service \
                 processor nor its sled agent is in the latest inventory \
                 collection, but the boards in some sled cubbies could not \
                 be identified, so the sled may still be running in one of \
                 them (cubby 2: its service processor did not answer). Retry \
                 after the next inventory collection; if this persists, \
                 contact Oxide support."
            ),
        )),
    );

    // TODO-RAINCLAUDE: Cubby 2's SP is in the latest collection again, so
    // creation counts cubby 2 as identified from the collection. The SP has
    // stopped answering by the time the request is made, but the request is
    // still created, without a 409.
    simrack.gimlets[2].set_responsiveness(Responsiveness::Responsive).await;
    run_inventory_collection(&cptestctx.lockstep_client).await;
    simrack.gimlets[2].set_responsiveness(Responsiveness::Unresponsive).await;
    let created = sled_expunge(client, target_id, &serial)
        .await
        .expect("created a request");
    assert_new_request(&created, rack_id, target_id, &target_bb, None, 3);

    // TODO-RAINCLAUDE: A replay of the outstanding request never reaches MGS,
    // so it succeeds even with both sidecars silent and neither MGS able to
    // list ignition.
    for sidecar in &simrack.sidecars {
        sidecar.set_responsiveness(Responsiveness::Unresponsive).await;
    }
    for gateway in cptestctx.gateway.values() {
        let error = gateway
            .client
            .ignition_list()
            .await
            .expect_err("MGS can't list ignition with its sidecar silent");
        assert_eq!(
            error.status(),
            Some(StatusCode::SERVICE_UNAVAILABLE),
            "{error}",
        );
    }
    let replayed = sled_expunge(client, target_id, &serial)
        .await
        .expect("replayed the request without MGS");
    assert_eq!(
        replayed,
        SledExpungeResponse {
            request: created.request.clone(),
            disposition: SledExpungeRequestDisposition::AlreadyOutstanding,
        },
    );
    for sidecar in &simrack.sidecars {
        sidecar.set_responsiveness(Responsiveness::Responsive).await;
    }

    tq.commit(3).await;

    // TODO-RAINCLAUDE: The task doesn't use the inventory collection to confirm
    // that the sled is still gone, so the collection that helped at creation
    // doesn't help here. With cubby 2's SP silent, the task can't identify that
    // cubby and waits. Once the SP answers again below, the task finishes.
    activate_task(&cptestctx, "sled_expunge").await;
    let waiting = view_request(client, created.request.id).await;
    assert_eq!(
        waiting.status,
        SledExpungeRequestState::PoweringOff {
            cubby: None,
            last_error: Some(
                "This sled was not found when the request was created, and \
                 Nexus cannot yet confirm that it is still gone: the board \
                 in sled cubby 2 could not be identified, so the sled may \
                 still be running in it. Nexus will keep retrying; if this \
                 persists, contact Oxide support."
                    .to_string()
            ),
        },
    );
    simrack.gimlets[2].set_responsiveness(Responsiveness::Responsive).await;

    activate_task(&cptestctx, "sled_expunge").await;
    let done = view_request(client, created.request.id).await;
    assert_eq!(
        done,
        SledExpungeRequest {
            status: SledExpungeRequestState::Done {
                power_off: SledExpungePowerOffOutcome::AbsentAtCreation,
                time_completed: done.time_modified,
            },
            time_modified: done.time_modified,
            ..created.request.clone()
        },
    );
    assert_eq!(sled_policy(&cptestctx, target_id).await, SledPolicy::Expunged);

    cptestctx.teardown().await;
}
