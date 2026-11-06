// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for APIs against sled-based endpoints.

use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use nexus_db_model::PhysicalDisk as DbPhysicalDisk;
use nexus_db_model::PhysicalDiskKind as DbPhysicalDiskKind;
use nexus_db_model::Sled as DbSled;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::SLED_AGENT_UUID;
use nexus_test_utils::resource_helpers::create_default_ip_pools;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::physical_disk::PhysicalDisk;
use nexus_types::external_api::sled::Sled;
use nexus_types::external_api::sled::SledInstance;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_generation_kinds::GenericGeneration;
use omicron_generation_kinds::SledRegistrationGeneration;
use omicron_sled_agent::sim;
use omicron_sled_agent::sim::SledCpuFamily;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use std::str::FromStr;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

pub(crate) async fn sleds_list(
    client: &ClientTestContext,
    sleds_url: &str,
) -> Vec<Sled> {
    objects_list_page_authz::<Sled>(client, sleds_url).await.items
}

async fn physical_disks_list(
    client: &ClientTestContext,
    url: &str,
) -> Vec<PhysicalDisk> {
    objects_list_page_authz::<PhysicalDisk>(client, url).await.items
}

async fn sled_instance_list(
    client: &ClientTestContext,
    url: &str,
) -> Vec<SledInstance> {
    objects_list_page_authz::<SledInstance>(client, url).await.items
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_sleds_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Verify that there are two sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(sleds_list(&client, &sleds_url).await.len(), 2);

    // Now start a few more sled agents.
    let nsleds = 3;
    let mut sas = Vec::with_capacity(nsleds);
    for _ in 0..nsleds {
        sas.push(
            cptestctx
                .add_sled(
                    SledUuid::new_v4(),
                    sim::SimMode::Explicit,
                    SledCpuFamily::AmdMilan,
                )
                .await,
        );
    }

    // List sleds again.
    let sleds_found = sleds_list(&client, &sleds_url).await;
    assert_eq!(sleds_found.len(), nsleds + 2);

    let sledids_found =
        sleds_found.iter().map(|sv| sv.identity.id).collect::<Vec<Uuid>>();
    let mut sledids_found_sorted = sledids_found.clone();
    sledids_found_sorted.sort();
    assert_eq!(sledids_found, sledids_found_sorted);

    // Tear down the agents.
    for sa in sas {
        sa.http_server.close().await.unwrap();
    }
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_physical_disk_create_list_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    let external_client = &cptestctx.external_client;

    // Verify that there are two sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(sleds_list(&external_client, &sleds_url).await.len(), 2);

    // The test framework may set up some disks initially.
    let disks_url =
        format!("/v1/system/hardware/sleds/{SLED_AGENT_UUID}/disks");
    let disks_initial = physical_disks_list(&external_client, &disks_url).await;

    // Inject a disk into the database, observe it in the external API
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let sled_id = SledUuid::from_str(&SLED_AGENT_UUID).unwrap();
    let physical_disk = DbPhysicalDisk::from_parts(
        PhysicalDiskUuid::new_v4(),
        "v".into(),
        "s".into(),
        "m".into(),
        DbPhysicalDiskKind::U2,
        sled_id,
    );

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let _disk_id = datastore
        .physical_disk_insert(&opctx, physical_disk.clone())
        .await
        .expect("Failed to upsert physical disk");

    let disks = physical_disks_list(&external_client, &disks_url).await;
    assert_eq!(disks.len(), disks_initial.len() + 1);
    let new_disk = disks
        .iter()
        .find(|found_disk| {
            found_disk.vendor == "v"
                && found_disk.serial == "s"
                && found_disk.model == "m"
        })
        .expect("did not find the new disk");

    // Delete that disk using the internal API, observe it in the external API
    datastore
        .physical_disk_delete(
            &opctx,
            PhysicalDiskUuid::from_untyped_uuid(new_disk.identity.id),
        )
        .await
        .expect("Failed to upsert physical disk");

    let list = physical_disks_list(&external_client, &disks_url).await;
    assert_eq!(list, disks_initial, "{:#?}", list,);
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_sled_instance_list(cptestctx: &ControlPlaneTestContext) {
    let external_client = &cptestctx.external_client;

    // Verify that there are two sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    let sleds = sleds_list(&external_client, &sleds_url).await;
    assert_eq!(sleds.len(), 2);

    // Verify that there are no instances on the sleds.
    for sled in &sleds {
        let sled_id = sled.identity.id;
        let instances_url =
            format!("/v1/system/hardware/sleds/{sled_id}/instances");
        assert!(
            sled_instance_list(&external_client, &instances_url)
                .await
                .is_empty()
        );
    }

    // Create an IP pool and project that we'll use for testing.
    create_default_ip_pools(&external_client).await;
    let project = create_project(&external_client, "test-project").await;
    let instance =
        create_instance(&external_client, "test-project", "test-instance")
            .await;

    // Ensure 1 instance was created on a sled
    let sled_instances = wait_for_condition(
        || {
            let sleds = sleds.clone();

            async move {
                let mut total_instances = vec![];

                for sled in &sleds {
                    let sled_id = sled.identity.id;

                    let instances_url = format!(
                        "/v1/system/hardware/sleds/{sled_id}/instances"
                    );

                    let mut sled_instances =
                        sled_instance_list(&external_client, &instances_url)
                            .await;

                    total_instances.append(&mut sled_instances);
                }

                if total_instances.len() == 1 {
                    Ok(total_instances)
                } else {
                    Err(CondCheckError::<()>::NotYet { status: None })
                }
            }
        },
        &std::time::Duration::from_millis(500),
        &std::time::Duration::from_secs(60),
    )
    .await
    .expect("one sled instance");

    assert_eq!(project.identity.name, sled_instances[0].project_name);
    assert_eq!(instance.identity.name, sled_instances[0].name);
}

fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
    let datastore = cptestctx.server.server_context().nexus.datastore();
    OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone())
}

async fn fetch_sled(
    cptestctx: &ControlPlaneTestContext,
    opctx: &OpContext,
    sled_id: SledUuid,
) -> DbSled {
    let nexus = &cptestctx.server.server_context().nexus;
    let (.., sled) = nexus
        .sled_lookup(opctx, &sled_id)
        .expect("built a sled lookup")
        .fetch()
        .await
        .unwrap_or_else(|error| panic!("sled {sled_id} has a row: {error}"));
    sled
}

async fn assert_no_sled_row(
    cptestctx: &ControlPlaneTestContext,
    opctx: &OpContext,
    sled_id: SledUuid,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let result = nexus
        .sled_lookup(opctx, &sled_id)
        .expect("built a sled lookup")
        .fetch()
        .await;
    match result {
        Ok((.., sled)) => {
            panic!("expected no row for sled {sled_id}, found {sled:?}")
        }
        Err(error) => assert_eq!(
            error,
            LookupType::ById(sled_id.into_untyped_uuid())
                .into_not_found(ResourceType::Sled),
        ),
    }
}

fn expect_error_response(
    result: Result<
        nexus_client::ResponseValue<()>,
        nexus_client::Error<nexus_client::types::Error>,
    >,
) -> (StatusCode, nexus_client::types::Error) {
    match result {
        Err(nexus_client::Error::ErrorResponse(response)) => {
            let status = response.status();
            (status, response.into_inner())
        }
        other => panic!("expected an error response from Nexus, got {other:?}"),
    }
}

/// Expunge and decommission a sled that the test registered, so that Nexus
/// background tasks stop contacting its simulated Sled Agent.
///
/// We do this before shutting down the simulated Sled Agent to avoid cross-test
/// contamination where a different service starts up on the same port and makes
/// requests.
async fn expunge_and_decommission(
    cptestctx: &ControlPlaneTestContext,
    opctx: &OpContext,
    sled_id: SledUuid,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let (authz_sled,) = nexus
        .sled_lookup(opctx, &sled_id)
        .expect("built a sled lookup")
        .lookup_for(authz::Action::Modify)
        .await
        .unwrap_or_else(|e| panic!("looked up sled {sled_id}: {e}"));
    datastore
        .sled_set_policy_to_expunged(opctx, &authz_sled)
        .await
        .unwrap_or_else(|e| panic!("expunged sled {sled_id}: {e}"));
    datastore
        .sled_set_state_to_decommissioned(opctx, &authz_sled)
        .await
        .unwrap_or_else(|e| panic!("decommissioned sled {sled_id}: {e}"));
}

#[nexus_test]
async fn test_sled_agent_put_checks_fetched_identity(
    cptestctx: &ControlPlaneTestContext,
) {
    let opctx = test_opctx(cptestctx);
    let nexus = &cptestctx.server.server_context().nexus;
    let internal_client = cptestctx.internal_client();

    let existing_sled_id = cptestctx.first_sled_id();
    let before = fetch_sled(cptestctx, &opctx, existing_sled_id).await;

    // Simulate a Sled Agent that reports another sled's ID.
    let impostor_sled_id = SledUuid::new_v4();
    let impostor = cptestctx
        .start_unregistered_sled(impostor_sled_id, nexus.rack_id())
        .await;
    assert_no_sled_row(cptestctx, &opctx, impostor_sled_id).await;
    let impostor_address = impostor.http_server.local_addr();
    let mut overwriting_body = impostor.sled_agent_info();
    // Bump the generation to avoid this request being filtered out.
    overwriting_body.generation =
        SledRegistrationGeneration::from(before.sled_agent_gen)
            .next()
            .into_untyped_generation();
    let (status, error) = expect_error_response(
        internal_client
            .sled_agent_put(&existing_sled_id, &overwriting_body)
            .await,
    );
    assert_eq!(
        (status, error.error_code.as_deref(), error.message.as_str()),
        (
            StatusCode::BAD_REQUEST,
            Some("InvalidRequest"),
            format!(
                "sled agent at {impostor_address} reports sled ID \
                 {impostor_sled_id}, which does not match sled ID \
                 {existing_sled_id} in the request path; refusing to \
                 register sled {existing_sled_id} with another sled's \
                 information",
            )
            .as_str(),
        ),
    );
    assert_eq!(
        fetch_sled(cptestctx, &opctx, existing_sled_id).await,
        before,
        "a refused registration must not write the path sled's row",
    );

    // Simulate a sled on a second rack that hasn't joined the fleet yet.
    let other_rack_id = RackUuid::new_v4();
    assert_ne!(other_rack_id, nexus.rack_id());
    let sled_id = SledUuid::new_v4();
    let sim = cptestctx.start_unregistered_sled(sled_id, other_rack_id).await;
    let body = sim.sled_agent_info();

    // This will return a 503 until the rack has been adopted by the fleet.
    let (status, error) = expect_error_response(
        internal_client.sled_agent_put(&sled_id, &body).await,
    );
    assert_eq!(
        (status, error.error_code.as_deref(), error.message.as_str()),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Some("ServiceNotAvailable"),
            "Service Unavailable",
        ),
        "registration of a sled on a rack with no `rack` row should be \
         refused as retryable",
    );
    assert_no_sled_row(cptestctx, &opctx, sled_id).await;

    nexus
        .datastore()
        .rack_insert(&opctx, &nexus_db_model::Rack::new(other_rack_id))
        .await
        .unwrap_or_else(|e| panic!("inserted rack {other_rack_id}: {e}"));

    // Retry this, now that the rack is adopted. This is the happy path.
    internal_client
        .sled_agent_put(&sled_id, &body)
        .await
        .expect("registration of a sled on an adopted rack was accepted");
    assert_eq!(
        fetch_sled(cptestctx, &opctx, sled_id).await.rack_id(),
        other_rack_id,
        "the row should hold the rack ID the sled agent reports, not this \
         Nexus's rack ID {}",
        nexus.rack_id(),
    );

    expunge_and_decommission(cptestctx, &opctx, sled_id).await;
}
