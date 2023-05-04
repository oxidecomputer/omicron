// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests basic instance support in the API

use super::metrics::query_for_latest_metric;

use camino::Utf8Path;
use chrono::Utc;
use http::method::Method;
use http::StatusCode;
use nexus_db_queries::context::OpContext;
use nexus_test_interface::NexusServer;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_ip_pool;
use nexus_test_utils::resource_helpers::create_local_user;
use nexus_test_utils::resource_helpers::create_silo;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::resource_helpers::populate_ip_pool;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::start_sled_agent;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::InstanceNetworkInterface;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Ipv4Net;
use omicron_common::api::external::Name;
use omicron_nexus::authz::SiloRole;
use omicron_nexus::db::fixed_data::silo::SILO_ID;
use omicron_nexus::db::lookup::LookupPath;
use omicron_nexus::external_api::shared::IpKind;
use omicron_nexus::external_api::shared::IpRange;
use omicron_nexus::external_api::shared::Ipv4Range;
use omicron_nexus::external_api::shared::SiloIdentityMode;
use omicron_nexus::external_api::views;
use omicron_nexus::TestInterfaces as _;
use omicron_nexus::{external_api::params, Nexus};
use omicron_sled_agent::sim::SledAgent;
use sled_agent_client::TestInterfaces as _;
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;

use dropshot::test_util::ClientTestContext;
use dropshot::{HttpErrorResponseBody, ResultsPage};

use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::{create_instance, create_project};
use nexus_test_utils_macros::nexus_test;
use omicron_sled_agent::sim;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

static PROJECT_NAME: &str = "springfield-squidport";

fn get_project_selector() -> String {
    format!("project={}", PROJECT_NAME)
}

fn get_instances_url() -> String {
    format!("/v1/instances?{}", get_project_selector())
}

fn get_instance_url(instance_name: &str) -> String {
    format!("/v1/instances/{}?{}", instance_name, get_project_selector())
}

fn get_disks_url() -> String {
    format!("/v1/disks?{}", get_project_selector())
}

fn default_vpc_subnets_url() -> String {
    format!("/v1/vpc-subnets?{}&vpc=default", get_project_selector())
}

async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
    populate_ip_pool(&client, "default", None).await;
    let project = create_project(client, PROJECT_NAME).await;
    project.identity.id
}

#[nexus_test]
async fn test_instances_access_before_create_returns_not_found(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let _ = create_project(&client, PROJECT_NAME).await;

    // List instances.  There aren't any yet.
    let instances = instances_list(&client, &get_instances_url()).await;
    assert_eq!(instances.len(), 0);

    // Make sure we get a 404 if we fetch one.
    let instance_url = get_instance_url("just-rainsticks");
    let error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &instance_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        "not found: instance with name \"just-rainsticks\""
    );

    // Ditto if we try to delete one.
    let error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::DELETE,
        &instance_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        "not found: instance with name \"just-rainsticks\""
    );
}

#[nexus_test]
async fn test_instance_access(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    populate_ip_pool(&client, "default", None).await;
    let project = create_project(client, PROJECT_NAME).await;

    // Create an instance.
    let instance_name = "test-instance";
    let instance = create_instance(client, PROJECT_NAME, instance_name).await;

    // Fetch instance by id
    let fetched_instance = instance_get(
        &client,
        format!("/v1/instances/{}", instance.identity.id).as_str(),
    )
    .await;
    assert_eq!(fetched_instance.identity.id, instance.identity.id);

    // Fetch instance by name and project_id
    let fetched_instance = instance_get(
        &client,
        format!(
            "/v1/instances/{}?project={}",
            instance.identity.name, project.identity.id
        )
        .as_str(),
    )
    .await;
    assert_eq!(fetched_instance.identity.id, instance.identity.id);

    // Fetch instance by name and project_name
    let fetched_instance = instance_get(
        &client,
        format!(
            "/v1/instances/{}?project={}",
            instance.identity.name, project.identity.name
        )
        .as_str(),
    )
    .await;
    assert_eq!(fetched_instance.identity.id, instance.identity.id);
}

#[nexus_test]
async fn test_instances_create_reboot_halt(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx();
    let nexus = &apictx.nexus;
    let instance_name = "just-rainsticks";

    create_org_and_project(&client).await;

    // Create an instance.
    let instance_url = get_instance_url(instance_name);
    let instance = create_instance(client, PROJECT_NAME, instance_name).await;
    assert_eq!(instance.identity.name, instance_name);
    assert_eq!(
        instance.identity.description,
        format!("instance \"{}\"", instance_name).as_str()
    );
    let InstanceCpuCount(nfoundcpus) = instance.ncpus;
    // These particulars are hardcoded in create_instance().
    assert_eq!(nfoundcpus, 4);
    assert_eq!(instance.memory.to_whole_gibibytes(), 1);
    assert_eq!(instance.hostname, "the_host");
    assert_eq!(instance.runtime.run_state, InstanceState::Starting);

    // Attempt to create a second instance with a conflicting name.
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &get_instances_url())
            .body(Some(&params::InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name: instance.identity.name.clone(),
                    description: format!(
                        "instance {:?}",
                        &instance.identity.name
                    ),
                },
                ncpus: instance.ncpus,
                memory: instance.memory,
                hostname: instance.hostname.clone(),
                user_data: vec![],
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::Default,
                external_ips: vec![],
                disks: vec![],
                start: true,
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!("already exists: instance \"{}\"", instance_name).as_str()
    );

    // List instances again and expect to find the one we just created.
    let instances = instances_list(&client, &get_instances_url()).await;
    assert_eq!(instances.len(), 1);
    instances_eq(&instances[0], &instance);

    // Fetch the instance and expect it to match.
    let instance = instance_get(&client, &instance_url).await;
    instances_eq(&instances[0], &instance);
    assert_eq!(instance.runtime.run_state, InstanceState::Starting);

    // Check that the instance got a network interface
    let nics_url = format!(
        "/v1/vpc-subnets/default/network-interfaces?project={}&vpc=default",
        PROJECT_NAME
    );
    let network_interfaces =
        objects_list_page_authz::<InstanceNetworkInterface>(client, &nics_url)
            .await
            .items;
    assert_eq!(network_interfaces.len(), 1);
    assert_eq!(network_interfaces[0].instance_id, instance.identity.id);
    assert_eq!(
        network_interfaces[0].identity.name,
        nexus_defaults::DEFAULT_PRIMARY_NIC_NAME
    );

    // Now, simulate completion of instance boot and check the state reported.
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    identity_eq(&instance.identity, &instance_next.identity);
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    // Request another boot.  This should succeed without changing the state,
    // not even the state timestamp.
    let instance = instance_next;
    let instance_next =
        instance_post(&client, instance_name, InstanceOp::Start).await;
    instances_eq(&instance, &instance_next);
    let instance_next = instance_get(&client, &instance_url).await;
    instances_eq(&instance, &instance_next);

    // Reboot the instance.
    let instance = instance_next;
    let instance_next =
        instance_post(&client, instance_name, InstanceOp::Reboot).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Rebooting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    // Request a halt and verify both the immediate state and the finished state.
    let instance = instance_next;
    let instance_next =
        instance_post(&client, instance_name, InstanceOp::Stop).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopping);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopped);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    // Request another halt.  This should succeed without changing the state,
    // not even the state timestamp.
    let instance = instance_next;
    let instance_next =
        instance_post(&client, instance_name, InstanceOp::Stop).await;
    instances_eq(&instance, &instance_next);
    let instance_next = instance_get(&client, &instance_url).await;
    instances_eq(&instance, &instance_next);
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopped);

    // Attempt to reboot the halted instance. This should fail.
    let _error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::POST,
        get_instance_url(format!("{}/reboot", instance_name).as_str()).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    // TODO communicating this error message through requires exporting error
    // types from dropshot, translating that into a component of the generated
    // client, and expressing that as a rich error type.
    // assert_eq!(error.message, "cannot reboot instance in state \"stopped\"");

    // State should still be stopped.
    let instance = instance_get(&client, &instance_url).await;
    assert_eq!(instance.runtime.run_state, InstanceState::Stopped);

    // Start the instance.  While it's starting, issue a reboot.  This should
    // succeed, having stopped in between.
    let instance = instance_next;
    let instance_next =
        instance_post(&client, instance_name, InstanceOp::Start).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Starting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    let instance_next =
        instance_post(&client, instance_name, InstanceOp::Reboot).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Rebooting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    // Stop the instance.  While it's stopping, issue a reboot.  This should
    // fail because you cannot stop an instance that's en route to a stopped
    // state.
    let instance = instance_next;
    let instance_next =
        instance_post(&client, instance_name, InstanceOp::Stop).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopping);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let _error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::POST,
        get_instance_url(format!("{}/reboot", instance_name).as_str()).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    // assert_eq!(error.message, "cannot reboot instance in state \"stopping\"");
    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopped);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    // TODO-coverage add a test to try to delete the project at this point.

    // Delete the instance.
    NexusRequest::object_delete(client, &instance_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // Check that the network interfaces for that instance are gone, peeking
    // at the subnet-scoped URL so we don't 404 at the instance-scoped route.
    let url_interfaces = format!(
        "/v1/vpc-subnets/default/network-interfaces?project={}&vpc=default",
        PROJECT_NAME,
    );
    let interfaces = objects_list_page_authz::<InstanceNetworkInterface>(
        client,
        &url_interfaces,
    )
    .await
    .items;
    assert!(
        interfaces.is_empty(),
        "Expected all network interfaces for the instance to be deleted"
    );

    // TODO-coverage re-add tests that check the server-side state after
    // deleting.  We need to figure out how these actually get cleaned up from
    // the API namespace when this happens.

    // Once more, try to reboot it.  This should not work on a destroyed
    // instance.
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::POST,
        get_instance_url(format!("{}/reboot", instance_name).as_str()).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Similarly, we should not be able to start or stop the instance.
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::POST,
        get_instance_url(format!("{}/start", instance_name).as_str()).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::POST,
        get_instance_url(format!("{}/stop", instance_name).as_str()).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn test_instance_metrics(cptestctx: &ControlPlaneTestContext) {
    // Normally, Nexus is not registered as a producer for tests.
    // Turn this bit on so we can also test some metrics from Nexus itself.
    cptestctx.server.register_as_producer().await;

    let client = &cptestctx.external_client;
    let oximeter = &cptestctx.oximeter;
    let apictx = &cptestctx.server.apictx();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();

    // Create an IP pool and project that we'll use for testing.
    populate_ip_pool(&client, "default", None).await;
    let project_id = create_project(&client, PROJECT_NAME).await.identity.id;

    // Query the view of these metrics stored within CRDB
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    assert_eq!(virtual_provisioning_collection.cpus_provisioned, 0);
    assert_eq!(virtual_provisioning_collection.ram_provisioned.to_bytes(), 0);

    // Query the view of these metrics stored within Clickhouse
    let metric_url = |metric_type: &str, id: Uuid| {
        format!(
            "/v1/system/metrics/{metric_type}?start_time={:?}&end_time={:?}&id={id}",
            Utc::now() - chrono::Duration::seconds(30),
            Utc::now() + chrono::Duration::seconds(30),
        )
    };
    oximeter.force_collect().await;
    for id in &[*SILO_ID, project_id] {
        assert_eq!(
            query_for_latest_metric(
                client,
                &metric_url("virtual_disk_space_provisioned", *id),
            )
            .await,
            0
        );
        assert_eq!(
            query_for_latest_metric(
                client,
                &metric_url("cpus_provisioned", *id),
            )
            .await,
            0
        );
        assert_eq!(
            query_for_latest_metric(
                client,
                &metric_url("ram_provisioned", *id),
            )
            .await,
            0
        );
    }

    // Create an instance.
    let instance_name = "just-rainsticks";
    create_instance(client, PROJECT_NAME, instance_name).await;
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    assert_eq!(virtual_provisioning_collection.cpus_provisioned, 4);
    assert_eq!(
        virtual_provisioning_collection.ram_provisioned.0,
        ByteCount::from_gibibytes_u32(1),
    );

    // Stop the instance
    let instance =
        instance_post(&client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance =
        instance_get(&client, &get_instance_url(&instance_name)).await;
    assert_eq!(instance.runtime.run_state, InstanceState::Stopped);
    // NOTE: I think it's arguably "more correct" to identify that the
    // number of CPUs being used by guests at this point is actually "0",
    // not "4", because the instance is stopped (same re: RAM usage).
    //
    // However, for implementation reasons, this is complicated (we have a
    // tendency to update the runtime without checking the prior state, which
    // makes edge-triggered behavior trickier to notice).
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    let expected_cpus = 4;
    let expected_ram =
        i64::try_from(ByteCount::from_gibibytes_u32(1).to_bytes()).unwrap();
    assert_eq!(virtual_provisioning_collection.cpus_provisioned, expected_cpus);
    assert_eq!(
        i64::from(virtual_provisioning_collection.ram_provisioned.0),
        expected_ram
    );
    oximeter.force_collect().await;
    for id in &[*SILO_ID, project_id] {
        assert_eq!(
            query_for_latest_metric(
                client,
                &metric_url("virtual_disk_space_provisioned", *id),
            )
            .await,
            0
        );
        assert_eq!(
            query_for_latest_metric(
                client,
                &metric_url("cpus_provisioned", *id),
            )
            .await,
            expected_cpus
        );
        assert_eq!(
            query_for_latest_metric(
                client,
                &metric_url("ram_provisioned", *id),
            )
            .await,
            expected_ram
        );
    }

    // Stop the instance
    NexusRequest::object_delete(client, &get_instance_url(&instance_name))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    assert_eq!(virtual_provisioning_collection.cpus_provisioned, 0);
    assert_eq!(virtual_provisioning_collection.ram_provisioned.to_bytes(), 0);
    oximeter.force_collect().await;
    for id in &[*SILO_ID, project_id] {
        assert_eq!(
            query_for_latest_metric(
                client,
                &metric_url("virtual_disk_space_provisioned", *id),
            )
            .await,
            0
        );
        assert_eq!(
            query_for_latest_metric(
                client,
                &metric_url("cpus_provisioned", *id),
            )
            .await,
            0
        );
        assert_eq!(
            query_for_latest_metric(
                client,
                &metric_url("ram_provisioned", *id),
            )
            .await,
            0
        );
    }
}

#[nexus_test]
async fn test_instances_create_stopped_start(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx();
    let nexus = &apictx.nexus;
    let instance_name = "just-rainsticks";

    create_org_and_project(&client).await;

    // Create an instance in a stopped state.
    let instance: Instance = object_create(
        client,
        &get_instances_url(),
        &params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: instance_name.parse().unwrap(),
                description: format!("instance {}", instance_name),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: String::from("the_host"),
            user_data: vec![],
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::Default,
            external_ips: vec![],
            disks: vec![],
            start: false,
        },
    )
    .await;
    assert_eq!(instance.runtime.run_state, InstanceState::Stopped);

    // Start the instance.
    let instance_url = get_instance_url(instance_name);
    let instance =
        instance_post(&client, instance_name, InstanceOp::Start).await;

    // Now, simulate completion of instance boot and check the state reported.
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    identity_eq(&instance.identity, &instance_next.identity);
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );
}

#[nexus_test]
async fn test_instances_delete_fails_when_running_succeeds_when_stopped(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx();
    let nexus = &apictx.nexus;
    let instance_name = "just-rainsticks";

    create_org_and_project(&client).await;

    // Create an instance.
    let instance_url = get_instance_url(instance_name);
    let instance = create_instance(client, PROJECT_NAME, instance_name).await;

    // Simulate the instance booting.
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    identity_eq(&instance.identity, &instance_next.identity);
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);

    // Attempt to delete a running instance. This should fail.
    let error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &instance_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        "instance cannot be deleted in state \"running\""
    );

    // Stop the instance
    let instance =
        instance_post(&client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance = instance_get(&client, &instance_url).await;
    assert_eq!(instance.runtime.run_state, InstanceState::Stopped);

    // Now deletion should succeed.
    NexusRequest::object_delete(&client, &instance_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
}

#[nexus_test]
async fn test_instances_invalid_creation_returns_bad_request(
    cptestctx: &ControlPlaneTestContext,
) {
    // The rest of these examples attempt to create invalid instances.  We don't
    // do exhaustive tests of the model here -- those are part of unit tests --
    // but we exercise a few different types of errors to make sure those get
    // passed through properly.

    let client = &cptestctx.external_client;
    let error = client
        .make_request_with_body(
            Method::POST,
            &get_instances_url(),
            "{".into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .unwrap_err();
    assert!(error
        .message
        .starts_with("unable to parse JSON body: EOF while parsing an object"));

    let request_body = r##"
        {
            "name": "an-instance",
            "description": "will never exist",
            "ncpus": -3,
            "memory": 256,
            "hostname": "localhost",
        }
    "##;
    let error = client
        .make_request_with_body(
            Method::POST,
            &get_instances_url(),
            request_body.into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .unwrap_err();
    assert!(error.message.starts_with(
        "unable to parse JSON body: ncpus: invalid value: integer `-3`"
    ));
}

#[nexus_test]
async fn test_instance_create_saga_removes_instance_database_record(
    cptestctx: &ControlPlaneTestContext,
) {
    // This test verifies that we remove the database record for an instance
    // when the instance-create saga fails and unwinds.
    //
    // The test works as follows:
    //
    // - Create one instance, with a known IP. This should succeed.
    // - Create another instance, with the same IP. This should fail.
    // - Create that same instance again, with a different IP. This should
    // succeed, even though most of the data (such as the name) would have
    // conflicted, should the second provision have succeeded.
    let client = &cptestctx.external_client;

    // Create test IP pool, organization and project
    create_org_and_project(&client).await;

    // The network interface parameters.
    let default_name = "default".parse::<Name>().unwrap();
    let requested_address = "172.30.0.10".parse::<std::net::IpAddr>().unwrap();
    let if0_params = params::InstanceNetworkInterfaceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("if0")).unwrap(),
            description: String::from("first custom interface"),
        },
        vpc_name: default_name.clone(),
        subnet_name: default_name.clone(),
        ip: Some(requested_address),
    };
    let interface_params =
        params::InstanceNetworkInterfaceAttachment::Create(vec![
            if0_params.clone()
        ]);

    // Create the parameters for the instance itself, and create it.
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("unwind-test-inst")).unwrap(),
            description: String::from("instance to test saga unwind"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("inst"),
        user_data: vec![],
        network_interfaces: interface_params.clone(),
        external_ips: vec![],
        disks: vec![],
        start: true,
    };
    let response = NexusRequest::objects_post(
        client,
        &get_instances_url(),
        &instance_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create first instance");
    let _ = response.parsed_body::<Instance>().unwrap();

    // Try to create a _new_ instance, with the same IP address. Note that the
    // other data does not conflict yet.
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("unwind-test-inst2")).unwrap(),
            description: String::from("instance to test saga unwind 2"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("inst2"),
        user_data: vec![],
        network_interfaces: interface_params,
        external_ips: vec![],
        disks: vec![],
        start: true,
    };
    let _ = NexusRequest::objects_post(
        client,
        &get_instances_url(),
        &instance_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect_err(
        "Should have failed to create second instance with \
                the same IP address as the first",
    );

    // Update the IP address to one that will succeed, but leave the other data
    // as-is. This would fail with a conflict on the instance name, if we don't
    // fully unwind the saga and delete the instance database record.
    let requested_address = "172.30.0.11".parse::<std::net::IpAddr>().unwrap();
    let if0_params = params::InstanceNetworkInterfaceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("if0")).unwrap(),
            description: String::from("first custom interface"),
        },
        vpc_name: default_name.clone(),
        subnet_name: default_name.clone(),
        ip: Some(requested_address),
    };
    let interface_params =
        params::InstanceNetworkInterfaceAttachment::Create(vec![
            if0_params.clone()
        ]);
    let instance_params = params::InstanceCreate {
        network_interfaces: interface_params,
        ..instance_params.clone()
    };
    let response = NexusRequest::objects_post(
        client,
        &get_instances_url(),
        &instance_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Creating a new instance should succeed");
    let instance = response.parsed_body::<Instance>().unwrap();
    assert_eq!(instance.identity.name, instance_params.identity.name);
}

// Basic test requesting an interface with a specific IP address.
#[nexus_test]
async fn test_instance_with_single_explicit_ip_address(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_org_and_project(&client).await;

    // Create the parameters for the interface.
    let default_name = "default".parse::<Name>().unwrap();
    let requested_address = "172.30.0.10".parse::<std::net::IpAddr>().unwrap();
    let if0_params = params::InstanceNetworkInterfaceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("if0")).unwrap(),
            description: String::from("first custom interface"),
        },
        vpc_name: default_name.clone(),
        subnet_name: default_name.clone(),
        ip: Some(requested_address),
    };
    let interface_params =
        params::InstanceNetworkInterfaceAttachment::Create(vec![
            if0_params.clone()
        ]);

    // Create the parameters for the instance itself, and create it.
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("nic-test-inst")).unwrap(),
            description: String::from("instance to test multiple nics"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nic-test"),
        user_data: vec![],
        network_interfaces: interface_params,
        external_ips: vec![],
        disks: vec![],
        start: true,
    };
    let response = NexusRequest::objects_post(
        client,
        &get_instances_url(),
        &instance_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create instance with two network interfaces");
    let instance = response.parsed_body::<Instance>().unwrap();

    // Get the interface, and verify it has the requested address
    let url_interface = format!(
        "/v1/network-interfaces/{}?{}&instance={}",
        if0_params.identity.name,
        get_project_selector(),
        instance_params.identity.name,
    );
    let interface = NexusRequest::object_get(client, &url_interface)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Failed to get network interface for new instance")
        .parsed_body::<InstanceNetworkInterface>()
        .expect("Failed to parse a network interface");
    assert_eq!(interface.instance_id, instance.identity.id);
    assert_eq!(interface.identity.name, if0_params.identity.name);
    assert_eq!(
        interface.ip, requested_address,
        "Interface was not assigned the requested IP address"
    );
}

// Test creating two new interfaces for an instance, at creation time.
#[nexus_test]
async fn test_instance_with_new_custom_network_interfaces(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_org_and_project(&client).await;
    // Create a VPC Subnet other than the default.
    //
    // We'll create one interface in the default VPC Subnet and one in this new
    // VPC Subnet.
    let default_name = Name::try_from(String::from("default")).unwrap();
    let non_default_subnet_name =
        Name::try_from(String::from("non-default-subnet")).unwrap();
    let vpc_subnet_params = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: non_default_subnet_name.clone(),
            description: String::from("A non-default subnet"),
        },
        ipv4_block: Ipv4Net("172.31.0.0/24".parse().unwrap()),
        ipv6_block: None,
    };
    let _response = NexusRequest::objects_post(
        client,
        &default_vpc_subnets_url(),
        &vpc_subnet_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create custom VPC Subnet");

    // TODO-coverage: We'd like to assert things about this VPC Subnet we just
    // created, but the `vpc_subnets_post` endpoint in Nexus currently returns
    // the "private" `omicron_nexus::db::model::VpcSubnet` type. That should be
    // converted to return the public `omicron_common::external` type, which is
    // work tracked in https://github.com/oxidecomputer/omicron/issues/388.

    // Create the parameters for the interfaces. These will be created during
    // the saga for instance creation.
    let if0_params = params::InstanceNetworkInterfaceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("if0")).unwrap(),
            description: String::from("first custom interface"),
        },
        vpc_name: default_name.clone(),
        subnet_name: default_name.clone(),
        ip: None,
    };
    let if1_params = params::InstanceNetworkInterfaceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("if1")).unwrap(),
            description: String::from("second custom interface"),
        },
        vpc_name: default_name.clone(),
        subnet_name: non_default_subnet_name.clone(),
        ip: None,
    };
    let interface_params =
        params::InstanceNetworkInterfaceAttachment::Create(vec![
            if0_params.clone(),
            if1_params.clone(),
        ]);

    // Create the parameters for the instance itself, and create it.
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("nic-test-inst")).unwrap(),
            description: String::from("instance to test multiple nics"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nic-test"),
        user_data: vec![],
        network_interfaces: interface_params,
        external_ips: vec![],
        disks: vec![],
        start: true,
    };
    let response = NexusRequest::objects_post(
        client,
        &get_instances_url(),
        &instance_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create instance with two network interfaces");
    let instance = response.parsed_body::<Instance>().unwrap();

    // Check that both interfaces actually appear correct.
    let nics_url = |subnet_name: &Name| {
        format!(
            "/v1/vpc-subnets/{}/network-interfaces?project={}&vpc=default",
            subnet_name, PROJECT_NAME
        )
    };

    // The first interface is in the default VPC Subnet
    let interfaces = NexusRequest::iter_collection_authn::<
        InstanceNetworkInterface,
    >(
        client, nics_url(&default_name).as_str(), "", Some(100)
    )
    .await
    .expect("Failed to get interfaces in default VPC Subnet");
    assert_eq!(
        interfaces.all_items.len(),
        1,
        "Should be a single interface in the default subnet"
    );
    let if0 = &interfaces.all_items[0];
    assert_eq!(if0.identity.name, if0_params.identity.name);
    assert_eq!(if0.identity.description, if0_params.identity.description);
    assert_eq!(if0.instance_id, instance.identity.id);
    assert_eq!(if0.ip, std::net::IpAddr::V4("172.30.0.5".parse().unwrap()));

    let interfaces1 =
        NexusRequest::iter_collection_authn::<InstanceNetworkInterface>(
            client,
            nics_url(&non_default_subnet_name).as_str(),
            "",
            Some(100),
        )
        .await
        .expect("Failed to get interfaces in non-default VPC Subnet");
    assert_eq!(
        interfaces1.all_items.len(),
        1,
        "Should be a single interface in the non-default subnet"
    );
    let if1 = &interfaces1.all_items[0];

    // TODO-coverage: Add this test once the `VpcSubnet` type can be
    // deserialized.
    // assert_eq!(if1.subnet_id, non_default_vpc_subnet.id);

    assert_eq!(if1.identity.name, if1_params.identity.name);
    assert_eq!(if1.identity.description, if1_params.identity.description);
    assert_eq!(if1.ip, std::net::IpAddr::V4("172.31.0.5".parse().unwrap()));
    assert_eq!(if1.instance_id, instance.identity.id);
    assert_eq!(if0.vpc_id, if1.vpc_id);
    assert_ne!(
        if0.subnet_id, if1.subnet_id,
        "Two interfaces should be created in different subnets"
    );
}

#[nexus_test]
async fn test_instance_create_delete_network_interface(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx().nexus;
    let instance_name = "nic-attach-test-inst";

    create_org_and_project(&client).await;

    // Create the VPC Subnet for the secondary interface
    let secondary_subnet = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("secondary")).unwrap(),
            description: String::from("A secondary VPC subnet"),
        },
        ipv4_block: Ipv4Net("172.31.0.0/24".parse().unwrap()),
        ipv6_block: None,
    };
    let _response = NexusRequest::objects_post(
        client,
        &default_vpc_subnets_url(),
        &secondary_subnet,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create secondary VPC Subnet");

    // Create an instance with no network interfaces
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("instance to test attaching new nic"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nic-test"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::None,
        external_ips: vec![],
        disks: vec![],
        start: true,
    };
    let response = NexusRequest::objects_post(
        client,
        &get_instances_url(),
        &instance_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create instance with two network interfaces");
    let instance = response.parsed_body::<Instance>().unwrap();

    // Verify there are no interfaces
    let url_interfaces = format!(
        "/v1/network-interfaces?project={}&instance={}",
        PROJECT_NAME, instance.identity.name,
    );
    let interfaces = NexusRequest::iter_collection_authn::<
        InstanceNetworkInterface,
    >(client, url_interfaces.as_str(), "", Some(100))
    .await
    .expect("Failed to get interfaces for instance");
    assert!(
        interfaces.all_items.is_empty(),
        "Expected no network interfaces for instance"
    );

    // Parameters for the interfaces to create/attach
    let if_params = vec![
        params::InstanceNetworkInterfaceCreate {
            identity: IdentityMetadataCreateParams {
                name: "if0".parse().unwrap(),
                description: String::from("a new nic"),
            },
            vpc_name: "default".parse().unwrap(),
            subnet_name: "default".parse().unwrap(),
            ip: Some("172.30.0.10".parse().unwrap()),
        },
        params::InstanceNetworkInterfaceCreate {
            identity: IdentityMetadataCreateParams {
                name: "if1".parse().unwrap(),
                description: String::from("a new nic"),
            },
            vpc_name: "default".parse().unwrap(),
            subnet_name: secondary_subnet.identity.name.clone(),
            ip: Some("172.31.0.11".parse().unwrap()),
        },
    ];

    // We should not be able to create an interface while the instance is running.
    //
    // NOTE: Need to use RequestBuilder manually because `expect_failure` does not allow setting
    // the body.
    let builder = RequestBuilder::new(
        client,
        http::Method::POST,
        url_interfaces.as_str(),
    )
    .body(Some(&if_params[0]))
    .expect_status(Some(http::StatusCode::BAD_REQUEST));
    let err = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Should not be able to create network interface on running instance")
        .parsed_body::<HttpErrorResponseBody>()
        .expect("Failed to parse error response body");
    assert_eq!(
        err.message,
        "Instance must be stopped to attach a new network interface",
        "Expected an InvalidRequest response when creating an interface on a running instance"
    );

    // Stop the instance
    let instance = instance_post(client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance.identity.id).await;

    // Verify we can now make the requests again
    let mut interfaces = Vec::with_capacity(2);
    for (i, params) in if_params.iter().enumerate() {
        let response = NexusRequest::objects_post(
            client,
            url_interfaces.as_str(),
            &params,
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Failed to create network interface on stopped instance");
        let iface = response.parsed_body::<InstanceNetworkInterface>().unwrap();
        assert_eq!(iface.identity.name, params.identity.name);
        assert_eq!(iface.ip, params.ip.unwrap());
        assert_eq!(
            iface.primary,
            i == 0,
            "Only the first interface should be primary"
        );
        interfaces.push(iface);
    }

    // Restart the instance, verify the interfaces are still correct.
    let instance =
        instance_post(client, instance_name, InstanceOp::Start).await;
    instance_simulate(nexus, &instance.identity.id).await;

    // Get all interfaces in one request.
    let other_interfaces = objects_list_page_authz::<InstanceNetworkInterface>(
        client,
        &url_interfaces,
    )
    .await
    .items;
    for (iface0, iface1) in interfaces.iter().zip(other_interfaces) {
        assert_eq!(iface0.identity.id, iface1.identity.id);
        assert_eq!(iface0.vpc_id, iface1.vpc_id);
        assert_eq!(iface0.subnet_id, iface1.subnet_id);
        assert_eq!(iface0.ip, iface1.ip);
        assert_eq!(iface0.primary, iface1.primary);
    }

    // Verify we cannot delete either interface while the instance is running
    for iface in interfaces.iter() {
        let err = NexusRequest::expect_failure(
            client,
            http::StatusCode::BAD_REQUEST,
            http::Method::DELETE,
            &format!("/v1/network-interfaces/{}", iface.identity.id),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect(
            "Should not be able to delete network interface on running instance",
        )
        .parsed_body::<HttpErrorResponseBody>()
        .expect("Failed to parse error response body");
        assert_eq!(
            err.message,
            "Instance must be stopped or failed to detach a network interface",
            "Expected an InvalidRequest response when detaching an interface from a running instance"
        );
    }

    // Stop the instance and verify we can delete the interface
    let instance = instance_post(client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance.identity.id).await;

    // We should not be able to delete the primary interface, while the
    // secondary still exists
    let url_interface =
        format!("/v1/network-interfaces/{}", interfaces[0].identity.id);
    let err = NexusRequest::expect_failure(
        client,
        http::StatusCode::BAD_REQUEST,
        http::Method::DELETE,
        url_interface.as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect(
        "Should not be able to delete the primary network interface \
        while secondary interfaces still exist",
    )
    .parsed_body::<HttpErrorResponseBody>()
    .expect("Failed to parse error response body");
    assert_eq!(
        err.message,
        "The primary interface \
        may not be deleted while secondary interfaces \
        are still attached",
        "Expected an InvalidRequest response when detaching \
        the primary interface from an instance with at least one \
        secondary interface",
    );

    // Verify that we can delete the secondary.
    let url_interface =
        format!("/v1/network-interfaces/{}", interfaces[1].identity.id);
    NexusRequest::object_delete(client, url_interface.as_str())
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Failed to delete secondary interface from stopped instance");

    // Now verify that we can delete the primary
    let url_interface =
        format!("/v1/network-interfaces/{}", interfaces[0].identity.id);
    NexusRequest::object_delete(client, url_interface.as_str())
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect(
            "Failed to delete sole primary interface from stopped instance",
        );
}

#[nexus_test]
async fn test_instance_update_network_interfaces(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx().nexus;
    let instance_name = "nic-update-test-inst";

    create_org_and_project(&client).await;

    // Create the VPC Subnet for the secondary interface
    let secondary_subnet = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("secondary")).unwrap(),
            description: String::from("A secondary VPC subnet"),
        },
        ipv4_block: Ipv4Net("172.31.0.0/24".parse().unwrap()),
        ipv6_block: None,
    };
    let _response = NexusRequest::objects_post(
        client,
        &default_vpc_subnets_url(),
        &secondary_subnet,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create secondary VPC Subnet");

    // Create an instance with no network interfaces
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("instance to test updatin nics"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nic-test"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::None,
        external_ips: vec![],
        disks: vec![],
        start: true,
    };
    let response = NexusRequest::objects_post(
        client,
        &get_instances_url(),
        &instance_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create instance with two network interfaces");
    let instance = response.parsed_body::<Instance>().unwrap();
    let url_interfaces = format!(
        "/v1/network-interfaces?project={}&instance={}",
        PROJECT_NAME, instance.identity.name,
    );

    // Parameters for each interface to try to modify.
    let if_params = vec![
        params::InstanceNetworkInterfaceCreate {
            identity: IdentityMetadataCreateParams {
                name: "if0".parse().unwrap(),
                description: String::from("a new nic"),
            },
            vpc_name: "default".parse().unwrap(),
            subnet_name: "default".parse().unwrap(),
            ip: Some("172.30.0.10".parse().unwrap()),
        },
        params::InstanceNetworkInterfaceCreate {
            identity: IdentityMetadataCreateParams {
                name: "if1".parse().unwrap(),
                description: String::from("a new nic"),
            },
            vpc_name: "default".parse().unwrap(),
            subnet_name: secondary_subnet.identity.name.clone(),
            ip: Some("172.31.0.11".parse().unwrap()),
        },
    ];

    // Stop the instance
    let instance = instance_post(client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance.identity.id).await;

    // Create the first interface on the instance.
    let primary_iface = NexusRequest::objects_post(
        client,
        url_interfaces.as_str(),
        &if_params[0],
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create network interface on stopped instance")
    .parsed_body::<InstanceNetworkInterface>()
    .unwrap();
    assert_eq!(primary_iface.identity.name, if_params[0].identity.name);
    assert_eq!(primary_iface.ip, if_params[0].ip.unwrap());
    assert!(primary_iface.primary, "The first interface should be primary");

    // Restart the instance, to ensure we can only modify things when it's
    // stopped.
    let instance =
        instance_post(client, instance_name, InstanceOp::Start).await;
    instance_simulate(nexus, &instance.identity.id).await;

    // We'll change the interface's name and description
    let new_name = Name::try_from(String::from("new-if0")).unwrap();
    let new_description = String::from("new description");
    let updates = params::InstanceNetworkInterfaceUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some(new_name.clone()),
            description: Some(new_description.clone()),
        },
        primary: false,
    };

    // Verify we fail to update the NIC when the instance is running
    //
    // NOTE: Need to use RequestBuilder manually because `expect_failure` does
    // not allow setting the body.
    let url_interface =
        format!("/v1/network-interfaces/{}", primary_iface.identity.id);
    let builder =
        RequestBuilder::new(client, http::Method::PUT, url_interface.as_str())
            .body(Some(&updates))
            .expect_status(Some(http::StatusCode::BAD_REQUEST));
    let err = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Should not be able to update network interface on running instance")
        .parsed_body::<HttpErrorResponseBody>()
        .expect("Failed to parse error response body");
    assert_eq!(
        err.message,
        "Instance must be stopped to update its network interfaces",
        "Expected an InvalidRequest response when modifying an interface on a running instance"
    );

    // Stop the instance again, and now verify that the update works.
    let instance = instance_post(client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance.identity.id).await;
    let updated_primary_iface = NexusRequest::object_put(
        client,
        &format!("/v1/network-interfaces/{}", primary_iface.identity.id),
        Some(&updates),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to update an interface")
    .parsed_body::<InstanceNetworkInterface>()
    .unwrap();

    // Verify the modifications have taken effect, updating the name,
    // description, and modification time.
    assert_eq!(updated_primary_iface.identity.name, new_name);
    assert_eq!(updated_primary_iface.identity.description, new_description);
    assert!(updated_primary_iface.primary);

    // Helper to check that most attributes are unchanged when updating an
    // interface, and that the modification time for the new is later than the
    // old.
    let verify_unchanged_attributes =
        |original_iface: &InstanceNetworkInterface,
         new_iface: &InstanceNetworkInterface| {
            assert_eq!(
                original_iface.identity.time_created,
                new_iface.identity.time_created
            );
            assert!(
                original_iface.identity.time_modified
                    < new_iface.identity.time_modified
            );
            assert_eq!(original_iface.ip, new_iface.ip);
            assert_eq!(original_iface.mac, new_iface.mac);
            assert_eq!(original_iface.subnet_id, new_iface.subnet_id);
            assert_eq!(original_iface.vpc_id, new_iface.vpc_id);
            assert_eq!(original_iface.instance_id, new_iface.instance_id);
        };
    verify_unchanged_attributes(&primary_iface, &updated_primary_iface);

    // Try with the same request again, but this time only changing
    // `primary`. This should have no effect.
    let updates = params::InstanceNetworkInterfaceUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        primary: true,
    };
    let updated_primary_iface1 = NexusRequest::object_put(
        client,
        &format!(
            "/v1/network-interfaces/{}",
            updated_primary_iface.identity.id
        ),
        Some(&updates),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to update an interface")
    .parsed_body::<InstanceNetworkInterface>()
    .unwrap();

    // Everything should still be the same, except the modification time.
    assert_eq!(
        updated_primary_iface.identity.name,
        updated_primary_iface1.identity.name
    );
    assert_eq!(
        updated_primary_iface.identity.description,
        updated_primary_iface1.identity.description
    );
    assert_eq!(updated_primary_iface.primary, updated_primary_iface1.primary);
    verify_unchanged_attributes(
        &updated_primary_iface,
        &updated_primary_iface1,
    );

    // Add a secondary interface to the instance. We'll use this to check
    // behavior related to making a new primary interface for the instance.
    // Create the first interface on the instance.
    let secondary_iface = NexusRequest::objects_post(
        client,
        url_interfaces.as_str(),
        &if_params[1],
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create network interface on stopped instance")
    .parsed_body::<InstanceNetworkInterface>()
    .unwrap();
    assert_eq!(secondary_iface.identity.name, if_params[1].identity.name);
    assert_eq!(secondary_iface.ip, if_params[1].ip.unwrap());
    assert!(
        !secondary_iface.primary,
        "Only the first interface should be primary"
    );

    // Restart the instance, and verify that we can't update either interface.
    let instance =
        instance_post(client, instance_name, InstanceOp::Start).await;
    instance_simulate(nexus, &instance.identity.id).await;

    for if_id in
        [&updated_primary_iface.identity.id, &secondary_iface.identity.id]
    {
        let url_interface = format!("/v1/network-interfaces/{}", if_id);
        let builder = RequestBuilder::new(
            client,
            http::Method::PUT,
            url_interface.as_str(),
        )
        .body(Some(&updates))
        .expect_status(Some(http::StatusCode::BAD_REQUEST));
        let err = NexusRequest::new(builder)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("Should not be able to update network interface on running instance")
            .parsed_body::<HttpErrorResponseBody>()
            .expect("Failed to parse error response body");
        assert_eq!(
            err.message,
            "Instance must be stopped to update its network interfaces",
            "Expected an InvalidRequest response when modifying an interface on a running instance"
        );
    }

    // Stop the instance again.
    let instance = instance_post(client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance.identity.id).await;

    // Verify that we can set the secondary as the new primary, and that nothing
    // else changes about the NICs.
    let updates = params::InstanceNetworkInterfaceUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        primary: true,
    };
    let new_primary_iface = NexusRequest::object_put(
        client,
        &format!("/v1/network-interfaces/{}", secondary_iface.identity.id),
        Some(&updates),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to update an interface")
    .parsed_body::<InstanceNetworkInterface>()
    .unwrap();

    // It should now be the primary and have an updated modification time
    assert!(new_primary_iface.primary, "Failed to set the new primary");

    // Nothing else about the new primary should have changed
    assert_eq!(new_primary_iface.identity.name, secondary_iface.identity.name);
    assert_eq!(
        new_primary_iface.identity.description,
        secondary_iface.identity.description
    );
    verify_unchanged_attributes(&secondary_iface, &new_primary_iface);

    // Get the newly-made secondary interface to test
    let new_secondary_iface = NexusRequest::object_get(
        client,
        &format!(
            "/v1/network-interfaces/{}",
            updated_primary_iface.identity.id
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to get the old primary / new secondary interface")
    .parsed_body::<InstanceNetworkInterface>()
    .unwrap();

    // The now-secondary interface should be, well, secondary
    assert!(
        !new_secondary_iface.primary,
        "The old primary interface should now be a seconary"
    );

    // Nothing else about the old primary should have changed
    assert_eq!(
        new_secondary_iface.identity.name,
        updated_primary_iface.identity.name
    );
    assert_eq!(
        new_secondary_iface.identity.description,
        updated_primary_iface.identity.description
    );
    verify_unchanged_attributes(&updated_primary_iface, &new_secondary_iface);

    // Let's delete the original primary, and verify that we've still got the
    // secondary.
    let url_interface =
        format!("/v1/network-interfaces/{}", new_secondary_iface.identity.id);
    NexusRequest::object_delete(&client, &url_interface)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Failed to delete original secondary interface");
    let all_interfaces = objects_list_page_authz::<InstanceNetworkInterface>(
        client,
        &url_interfaces,
    )
    .await
    .items;
    assert_eq!(
        all_interfaces.len(),
        1,
        "Expected just one interface after deleting the original primary"
    );
    assert_eq!(all_interfaces[0].identity.id, new_primary_iface.identity.id);
    assert!(all_interfaces[0].primary);

    // Add a _new_ interface, and verify that it still isn't the primary
    let iface = NexusRequest::objects_post(
        client,
        url_interfaces.as_str(),
        &if_params[0],
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create network interface on stopped instance")
    .parsed_body::<InstanceNetworkInterface>()
    .unwrap();
    assert!(!iface.primary);
    assert_eq!(iface.identity.name, if_params[0].identity.name);
}

/// This test specifically creates two NICs, the second of which will fail the
/// saga on purpose, since its IP address is the same. This is to verify that
/// the initial NIC is also deleted.
#[nexus_test]
async fn test_instance_with_multiple_nics_unwinds_completely(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let _ = create_project(&client, PROJECT_NAME).await;

    // Create two interfaces, in the same VPC Subnet. This will trigger an
    // error on creation of the second NIC, and we'll make sure that both are
    // deleted.
    let default_name = "default".parse::<Name>().unwrap();
    let if0_params = params::InstanceNetworkInterfaceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("if0")).unwrap(),
            description: String::from("first custom interface"),
        },
        vpc_name: default_name.clone(),
        subnet_name: default_name.clone(),
        ip: Some("172.30.0.6".parse().unwrap()),
    };
    let if1_params = params::InstanceNetworkInterfaceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("if1")).unwrap(),
            description: String::from("second custom interface"),
        },
        vpc_name: default_name.clone(),
        subnet_name: default_name.clone(),
        ip: Some("172.30.0.7".parse().unwrap()),
    };
    let interface_params =
        params::InstanceNetworkInterfaceAttachment::Create(vec![
            if0_params.clone(),
            if1_params.clone(),
        ]);

    // Create the parameters for the instance itself, and create it.
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("nic-fail-test-inst")).unwrap(),
            description: String::from("instance to test multiple bad nics"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nic-test"),
        user_data: vec![],
        network_interfaces: interface_params,
        external_ips: vec![],
        disks: vec![],
        start: true,
    };
    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::BAD_REQUEST));
    let response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance saga to fail");
    assert_eq!(response.status, http::StatusCode::BAD_REQUEST);

    // Verify that there are no NICs at all in the subnet.
    let url_nics = format!(
        "/v1/vpc-subnets/default/network-interfaces?project={}&vpc=default",
        PROJECT_NAME,
    );
    let interfaces = NexusRequest::iter_collection_authn::<
        InstanceNetworkInterface,
    >(client, &url_nics, "", None)
    .await
    .expect("failed to list network interfaces")
    .all_items;
    assert!(
        interfaces.is_empty(),
        "There should be no network interfaces in the subnet"
    );
}

/// Create a disk and attach during instance creation
#[nexus_test]
async fn test_attach_one_disk_to_instance(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let instance_name = "nifs";

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_org_and_project(&client).await;

    // Create the "probablydata" disk
    create_disk(&client, PROJECT_NAME, "probablydata").await;

    // Verify disk is there and currently detached
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 1);
    assert_eq!(disks[0].state, DiskState::Detached);

    // Create the instance
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nfs"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach {
                name: Name::try_from(String::from("probablydata")).unwrap(),
            },
        )],
        start: true,
    };

    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));
    let response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation to work!");

    let instance = response.parsed_body::<Instance>().unwrap();

    // Verify disk is attached to the instance
    let url_instance_disks =
        format!("/v1/instances/{}/disks", instance.identity.id,);
    let disks: Vec<Disk> = NexusRequest::iter_collection_authn(
        client,
        &url_instance_disks,
        "",
        None,
    )
    .await
    .expect("failed to list disks")
    .all_items;
    assert_eq!(disks.len(), 1);
    assert_eq!(disks[0].state, DiskState::Attached(instance.identity.id));
}

#[nexus_test]
async fn test_instance_create_attach_disks(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_org_and_project(&client).await;
    let attachable_disk =
        create_disk(&client, PROJECT_NAME, "attachable-disk").await;

    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("nfs")).unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(3),
        hostname: String::from("nfs"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![
            params::InstanceDiskAttachment::Create(params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: Name::try_from(String::from("created-disk")).unwrap(),
                    description: String::from(
                        "A disk that was created by instance create",
                    ),
                },
                size: ByteCount::from_gibibytes_u32(4),
                disk_source: params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
            }),
            params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach {
                    name: attachable_disk.identity.name,
                },
            ),
        ],
        start: true,
    };

    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));

    let response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation!");

    let instance = response.parsed_body::<Instance>().unwrap();

    // Assert disks are created and attached
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 2);

    for disk in disks {
        assert_eq!(disk.state, DiskState::Attached(instance.identity.id));
    }
}

/// Tests to ensure that when an error occurs in the instance create saga after
/// some disks are succesfully created and attached, those disks are detached
/// and deleted.
#[nexus_test]
async fn test_instance_create_attach_disks_undo(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_org_and_project(&client).await;
    let regular_disk = create_disk(&client, PROJECT_NAME, "a-reg-disk").await;
    let faulted_disk = create_disk(&client, PROJECT_NAME, "faulted-disk").await;

    // set `faulted_disk` to the faulted state
    let apictx = &cptestctx.server.apictx();
    let nexus = &apictx.nexus;
    assert!(nexus
        .set_disk_as_faulted(&faulted_disk.identity.id)
        .await
        .unwrap());

    // Assert regular and faulted disks were created
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 2);

    assert_eq!(disks[0].identity.id, regular_disk.identity.id);
    assert_eq!(disks[0].state, DiskState::Detached);

    assert_eq!(disks[1].identity.id, faulted_disk.identity.id);
    assert_eq!(disks[1].state, DiskState::Faulted);

    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("nfs")).unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nfs"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![
            params::InstanceDiskAttachment::Create(params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: Name::try_from(String::from("probablydata")).unwrap(),
                    description: String::from("probably data"),
                },
                size: ByteCount::from_gibibytes_u32(4),
                disk_source: params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
            }),
            params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach { name: regular_disk.identity.name },
            ),
            params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach { name: faulted_disk.identity.name },
            ),
        ],
        start: true,
    };

    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::BAD_REQUEST));

    let _response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation to fail!");

    // Assert disks are in the same state as before the instance creation began
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 2);

    assert_eq!(disks[0].identity.id, regular_disk.identity.id);
    assert_eq!(disks[0].state, DiskState::Detached);

    assert_eq!(disks[1].identity.id, faulted_disk.identity.id);
    assert_eq!(disks[1].state, DiskState::Faulted);
}

// Test that 8 disks is supported
#[nexus_test]
async fn test_attach_eight_disks_to_instance(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_org_and_project(&client).await;

    // Make 8 disks
    for i in 0..8 {
        create_disk(&client, PROJECT_NAME, &format!("probablydata{}", i,))
            .await;
    }

    // Assert we created 8 disks
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 8);

    // Try to boot an instance that has 8 disks attached
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("nfs")).unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nfs"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: (0..8)
            .map(|i| {
                params::InstanceDiskAttachment::Attach(
                    params::InstanceDiskAttach {
                        name: Name::try_from(format!("probablydata{}", i))
                            .unwrap(),
                    },
                )
            })
            .collect(),
        start: true,
    };

    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));

    let response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation!");

    let instance = response.parsed_body::<Instance>().unwrap();

    // Assert disks are attached
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 8);

    for disk in disks {
        assert_eq!(disk.state, DiskState::Attached(instance.identity.id));
    }
}

// Test that disk attach limit is enforced
#[nexus_test]
async fn test_cannot_attach_nine_disks_to_instance(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let project_name = "bit-barrel";

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_project(client, project_name).await;

    // Make 9 disks
    for i in 0..9 {
        create_disk(&client, project_name, &format!("probablydata{}", i,))
            .await;
    }

    let disks_url = format!("/v1/disks?project={}", project_name,);

    // Assert we created 9 disks
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &disks_url, "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 9);

    // Try to boot an instance that has 9 disks attached
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("nfs")).unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nfs"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: (0..9)
            .map(|i| {
                params::InstanceDiskAttachment::Attach(
                    params::InstanceDiskAttach {
                        name: Name::try_from(format!("probablydata{}", i))
                            .unwrap(),
                    },
                )
            })
            .collect(),
        start: true,
    };

    let url_instances = format!("/v1/instances?project={}", project_name);

    let builder =
        RequestBuilder::new(client, http::Method::POST, &url_instances)
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::BAD_REQUEST));

    let _response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation to fail with bad request!");

    // Check that disks are still detached
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &disks_url, "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 9);

    for disk in disks {
        assert_eq!(disk.state, DiskState::Detached);
    }
}

// Test that faulted disks cannot be attached
#[nexus_test]
async fn test_cannot_attach_faulted_disks(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_org_and_project(&client).await;

    // Make 8 disks
    for i in 0..8 {
        create_disk(&client, PROJECT_NAME, &format!("probablydata{}", i,))
            .await;
    }

    // Assert we created 8 disks
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 8);

    // Set the 7th to FAULTED
    let apictx = &cptestctx.server.apictx();
    let nexus = &apictx.nexus;
    assert!(nexus.set_disk_as_faulted(&disks[6].identity.id).await.unwrap());

    // Assert FAULTED
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 8);

    for (i, disk) in disks.iter().enumerate() {
        if i == 6 {
            assert_eq!(disk.state, DiskState::Faulted);
        } else {
            assert_eq!(disk.state, DiskState::Detached);
        }
    }

    // Try to boot the instance
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("nfs")).unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nfs"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: (0..8)
            .map(|i| {
                params::InstanceDiskAttachment::Attach(
                    params::InstanceDiskAttach {
                        name: Name::try_from(format!("probablydata{}", i))
                            .unwrap(),
                    },
                )
            })
            .collect(),
        start: true,
    };

    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::BAD_REQUEST));

    let _response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation to fail!");

    // Assert disks are detached (except for the 7th)
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 8);

    for (i, disk) in disks.iter().enumerate() {
        if i == 6 {
            assert_eq!(disk.state, DiskState::Faulted);
        } else {
            assert_eq!(disk.state, DiskState::Detached);
        }
    }
}

// Test that disks are detached when instance is destroyed
#[nexus_test]
async fn test_disks_detached_when_instance_destroyed(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let instance_name = "nfs";

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_org_and_project(&client).await;

    // Make 8 disks
    for i in 0..8 {
        create_disk(&client, PROJECT_NAME, &format!("probablydata{}", i,))
            .await;
    }

    // Assert we created 8 disks
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;

    assert_eq!(disks.len(), 8);
    for disk in &disks {
        assert_eq!(disk.state, DiskState::Detached);
    }

    // Boot the instance
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nfs"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: (0..8)
            .map(|i| {
                params::InstanceDiskAttachment::Attach(
                    params::InstanceDiskAttach {
                        name: Name::try_from(format!("probablydata{}", i))
                            .unwrap(),
                    },
                )
            })
            .collect(),
        start: true,
    };

    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));

    let _response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("expected instance creation!");

    // Assert disks are attached
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;

    assert_eq!(disks.len(), 8);
    for disk in &disks {
        assert!(matches!(disk.state, DiskState::Attached(_)));
    }

    // Stop and delete instance
    let instance_url = format!("/v1/instances/nfs?project={}", PROJECT_NAME);
    let instance =
        instance_post(&client, instance_name, InstanceOp::Stop).await;

    let apictx = &cptestctx.server.apictx();
    let nexus = &apictx.nexus;

    // Store the sled agent for this instance for later disk simulation
    let sa = nexus.instance_sled_by_id(&instance.identity.id).await.unwrap();

    instance_simulate(nexus, &instance.identity.id).await;
    let instance = instance_get(&client, &instance_url).await;
    assert_eq!(instance.runtime.run_state, InstanceState::Stopped);

    NexusRequest::object_delete(&client, &instance_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // Assert disks are detached
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;

    assert_eq!(disks.len(), 8);
    for disk in &disks {
        assert_eq!(disk.state, DiskState::Detached);

        // Simulate each one of the disks to move from "Detaching" to "Detached"
        sa.disk_finish_transition(disk.identity.id).await;
    }

    // Ensure that the disks can be attached to another instance
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "nfsv2".parse().unwrap(),
            description: String::from("probably serving data too!"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("nfsv2"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: (0..8)
            .map(|i| {
                params::InstanceDiskAttachment::Attach(
                    params::InstanceDiskAttach {
                        name: Name::try_from(format!("probablydata{}", i))
                            .unwrap(),
                    },
                )
            })
            .collect(),
        start: true,
    };

    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));

    let _response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("expected instance creation!");

    // Assert disks are attached to this new instance
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;

    assert_eq!(disks.len(), 8);
    for disk in &disks {
        assert!(matches!(disk.state, DiskState::Attached(_)));
    }
}

// Tests that an instance is rejected if the memory is less than
// MIN_MEMORY_SIZE_BYTES
#[nexus_test]
async fn test_instances_memory_rejected_less_than_min_memory_size(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_org_and_project(client).await;

    // Attempt to create the instance, observe a server error.
    let instance_name = "just-rainsticks";
    let instance = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: format!("instance {:?}", &instance_name),
        },
        ncpus: InstanceCpuCount(1),
        memory: ByteCount::from(params::MIN_MEMORY_SIZE_BYTES / 2),
        hostname: String::from("inst"),
        user_data:
            b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                .to_vec(),
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        start: true,
    };

    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &get_instances_url())
            .body(Some(&instance))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();

    assert_eq!(
        error.message,
        format!(
            "unsupported value for \"size\": memory must be at least {}",
            ByteCount::from(params::MIN_MEMORY_SIZE_BYTES)
        ),
    );
}

// Test that an instance is rejected if memory is not divisible by
// MIN_MEMORY_SIZE
#[nexus_test]
async fn test_instances_memory_not_divisible_by_min_memory_size(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_org_and_project(client).await;

    // Attempt to create the instance, observe a server error.
    let instance_name = "just-rainsticks";
    let instance = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: format!("instance {:?}", &instance_name),
        },
        ncpus: InstanceCpuCount(1),
        memory: ByteCount::from(1024 * 1024 * 1024 + 300),
        hostname: String::from("inst"),
        user_data:
            b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                .to_vec(),
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        start: true,
    };

    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &get_instances_url())
            .body(Some(&instance))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();

    assert_eq!(
        error.message,
        format!(
            "unsupported value for \"size\": memory must be divisible by {}",
            ByteCount::from(params::MIN_MEMORY_SIZE_BYTES)
        ),
    );
}

async fn expect_instance_creation_fail_unavailable(
    client: &ClientTestContext,
    url_instances: &str,
    instance_params: &params::InstanceCreate,
) {
    let builder =
        RequestBuilder::new(client, http::Method::POST, &url_instances)
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::SERVICE_UNAVAILABLE));
    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation to fail with SERVICE_UNAVAILABLE!");
}

async fn expect_instance_creation_ok(
    client: &ClientTestContext,
    url_instances: &str,
    instance_params: &params::InstanceCreate,
) {
    let builder =
        RequestBuilder::new(client, http::Method::POST, &url_instances)
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));
    let _ = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation to work!");
}

async fn expect_instance_deletion_ok(
    client: &ClientTestContext,
    url_instances: &str,
) {
    NexusRequest::object_delete(client, &url_instances)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
}

#[nexus_test]
async fn test_cannot_provision_instance_beyond_cpu_capacity(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project(client, PROJECT_NAME).await;
    populate_ip_pool(&client, "default", None).await;

    let too_many_cpus = InstanceCpuCount::try_from(i64::from(
        nexus_test_utils::TEST_HARDWARE_THREADS + 1,
    ))
    .unwrap();
    let enough_cpus = InstanceCpuCount::try_from(i64::from(
        nexus_test_utils::TEST_HARDWARE_THREADS,
    ))
    .unwrap();

    // Try to boot an instance that uses more CPUs than we have
    // on our test sled setup.
    let name1 = Name::try_from(String::from("test")).unwrap();
    let mut instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: name1.clone(),
            description: String::from("probably serving data"),
        },
        ncpus: too_many_cpus,
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("test"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        start: false,
    };
    let url_instances = get_instances_url();

    expect_instance_creation_fail_unavailable(
        client,
        &url_instances,
        &instance_params,
    )
    .await;

    // If we ask for fewer CPUs, the request should work
    instance_params.ncpus = enough_cpus;
    expect_instance_creation_ok(client, &url_instances, &instance_params).await;

    // Requesting another instance won't have enough space
    let name2 = Name::try_from(String::from("test2")).unwrap();
    instance_params.identity.name = name2;
    expect_instance_creation_fail_unavailable(
        client,
        &url_instances,
        &instance_params,
    )
    .await;

    // But if we delete the first instace, we'll have space
    let url_instance = get_instance_url(&name1.to_string());
    expect_instance_deletion_ok(client, &url_instance).await;
    expect_instance_creation_ok(client, &url_instances, &instance_params).await;
}

#[nexus_test]
async fn test_cannot_provision_instance_beyond_ram_capacity(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project(client, PROJECT_NAME).await;
    populate_ip_pool(&client, "default", None).await;

    let too_much_ram = ByteCount::try_from(
        nexus_test_utils::TEST_PHYSICAL_RAM
            + u64::from(params::MIN_MEMORY_SIZE_BYTES),
    )
    .unwrap();
    let enough_ram =
        ByteCount::try_from(nexus_test_utils::TEST_PHYSICAL_RAM).unwrap();

    // Try to boot an instance that uses more RAM than we have
    // on our test sled setup.
    let name1 = Name::try_from(String::from("test")).unwrap();
    let mut instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: name1.clone(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: too_much_ram,
        hostname: String::from("test"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        start: false,
    };
    let url_instances = get_instances_url();
    expect_instance_creation_fail_unavailable(
        client,
        &url_instances,
        &instance_params,
    )
    .await;

    // If we ask for less RAM, the request should work
    instance_params.memory = enough_ram;
    expect_instance_creation_ok(client, &url_instances, &instance_params).await;

    // Requesting another instance won't have enough space
    let name2 = Name::try_from(String::from("test2")).unwrap();
    instance_params.identity.name = name2;
    expect_instance_creation_fail_unavailable(
        client,
        &url_instances,
        &instance_params,
    )
    .await;

    // But if we delete the first instace, we'll have space
    let url_instance = get_instance_url(&name1.to_string());
    expect_instance_deletion_ok(client, &url_instance).await;
    expect_instance_creation_ok(client, &url_instances, &instance_params).await;
}

#[nexus_test]
async fn test_instance_serial(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx();
    let nexus = &apictx.nexus;
    let instance_name = "kris-picks";

    cptestctx
        .sled_agent
        .sled_agent
        .start_local_mock_propolis_server(&cptestctx.logctx.log)
        .await
        .unwrap();

    create_org_and_project(&client).await;
    let instance_url = get_instance_url(instance_name);

    // Make sure we get a 404 if we try to access the serial console before creation.
    let instance_serial_url =
        get_instance_url(format!("{}/serial-console", instance_name).as_str());
    let error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &format!("{}&from_start=0", instance_serial_url),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!("not found: instance with name \"{}\"", instance_name).as_str()
    );

    // Create an instance.
    let instance = create_instance(client, PROJECT_NAME, instance_name).await;

    // Now, simulate completion of instance boot and check the state reported.
    // NOTE: prior to this instance_simulate call, nexus's stored propolis addr
    // is one it allocated in a 'real' sled-agent IP range as part of its usual
    // instance-creation saga. after we poke the new run state for the instance
    // here, sled-agent-sim will send an entire updated InstanceRuntimeState
    // back to nexus, including the localhost address on which the mock
    // propolis-server is running, which overwrites this -- after which nexus's
    // serial-console related API calls will start working.
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    identity_eq(&instance.identity, &instance_next.identity);
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    // Query serial output history endpoint
    // This is the first line of output generated by the mock propolis-server.
    let expected = "This is simulated serial console output for ".as_bytes();
    let mut actual = Vec::new();
    while actual.len() < expected.len() {
        let serial_data: params::InstanceSerialConsoleData =
            NexusRequest::object_get(
                client,
                &format!("{}&from_start={}", instance_serial_url, actual.len()),
            )
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to make request")
            .parsed_body()
            .unwrap();
        actual.extend_from_slice(&serial_data.data);
        assert_eq!(actual.len(), serial_data.last_byte_offset as usize);
    }
    assert_eq!(&actual[..expected.len()], expected);

    // Request a halt and verify both the immediate state and the finished state.
    let instance = instance_next;
    let instance_next =
        instance_post(&client, instance_name, InstanceOp::Stop).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopping);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopped);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    // Delete the instance.
    NexusRequest::object_delete(client, &instance_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
}

#[nexus_test]
async fn test_instance_ephemeral_ip_from_correct_pool(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create test organization and projects.
    let _ = create_project(&client, PROJECT_NAME).await;

    // Create two IP pools.
    //
    // The first is given to the "default" pool, the provided to a distinct
    // explicit pool.
    let first_range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 0, 0, 5),
        )
        .unwrap(),
    );
    let second_range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 1, 0, 1),
            std::net::Ipv4Addr::new(10, 1, 0, 5),
        )
        .unwrap(),
    );
    populate_ip_pool(&client, "default", Some(first_range)).await;
    create_ip_pool(&client, "other-pool", Some(second_range)).await;

    // Create an instance explicitly using the "other-pool".
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("ip-pool-test")).unwrap(),
            description: String::from("instance to test IP Pool restriction"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("inst"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![params::ExternalIpCreate::Ephemeral {
            pool_name: Some(
                Name::try_from(String::from("other-pool")).unwrap(),
            ),
        }],
        disks: vec![],
        start: true,
    };
    let response = NexusRequest::objects_post(
        client,
        &get_instances_url(),
        &instance_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to create first instance");
    let _ = response.parsed_body::<Instance>().unwrap();

    // Fetch the external IPs for the instance.
    let ips_url = format!(
        "/v1/instances/{}/external-ips?project={}",
        instance_params.identity.name, PROJECT_NAME
    );
    let ips = NexusRequest::object_get(client, &ips_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Failed to fetch external IPs")
        .parsed_body::<ResultsPage<views::ExternalIp>>()
        .expect("Failed to parse external IPs");
    assert_eq!(ips.items.len(), 1);
    assert_eq!(ips.items[0].kind, IpKind::Ephemeral);
    assert!(
        ips.items[0].ip >= second_range.first_address()
            && ips.items[0].ip <= second_range.last_address(),
        "Expected the Ephemeral IP to come from the second address \
        range, since the first is reserved for the default pool, not \
        the requested pool."
    );
}

#[nexus_test]
async fn test_instance_create_in_silo(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a silo with a Collaborator User
    let silo =
        create_silo(&client, "authz", true, SiloIdentityMode::LocalOnly).await;
    let user_id = create_local_user(
        client,
        &silo,
        &"unpriv".parse().unwrap(),
        params::UserPassword::InvalidPassword,
    )
    .await
    .id;
    grant_iam(
        client,
        "/v1/system/silos/authz",
        SiloRole::Collaborator,
        user_id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Populate IP Pool
    populate_ip_pool(&client, "default", None).await;

    // Create test projects
    NexusRequest::objects_post(
        client,
        "/v1/projects",
        &params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: PROJECT_NAME.parse().unwrap(),
                description: String::new(),
            },
        },
    )
    .authn_as(AuthnMode::SiloUser(user_id))
    .execute()
    .await
    .expect("failed to create Project")
    .parsed_body::<views::Project>()
    .expect("failed to parse new Project");

    // Create an instance using the authorization granted to the collaborator
    // Silo User.
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("ip-pool-test")).unwrap(),
            description: String::from("instance to test IP Pool authz"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: String::from("inst"),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![params::ExternalIpCreate::Ephemeral {
            pool_name: Some(Name::try_from(String::from("default")).unwrap()),
        }],
        disks: vec![],
        start: true,
    };
    let url_instances = format!("/v1/instances?project={}", PROJECT_NAME);
    NexusRequest::objects_post(client, &url_instances, &instance_params)
        .authn_as(AuthnMode::SiloUser(user_id))
        .execute()
        .await
        .expect("Failed to create instance")
        .parsed_body::<Instance>()
        .expect("Failed to parse instance");
}

/// Test that appropriate OPTE V2P mappings are created and deleted.
#[nexus_test]
async fn test_instance_v2p_mappings(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    populate_ip_pool(&client, "default", None).await;
    create_project(client, PROJECT_NAME).await;

    // Add a few more sleds
    let nsleds = 3;
    let mut additional_sleds = Vec::with_capacity(nsleds);
    for _ in 0..nsleds {
        let sa_id = Uuid::new_v4();
        let log =
            cptestctx.logctx.log.new(o!( "sled_id" => sa_id.to_string() ));
        let addr = cptestctx.server.get_http_server_internal_address().await;
        let update_directory = Utf8Path::new("/should/not/be/used");
        additional_sleds.push(
            start_sled_agent(
                log,
                addr,
                sa_id,
                &update_directory,
                sim::SimMode::Explicit,
            )
            .await
            .unwrap(),
        );
    }

    // Create an instance.
    let instance_name = "test-instance";
    let instance = create_instance(client, PROJECT_NAME, instance_name).await;

    let nics_url =
        format!("/v1/network-interfaces?instance={}", instance.identity.id,);

    let nics =
        objects_list_page_authz::<InstanceNetworkInterface>(client, &nics_url)
            .await
            .items;

    // The default config is one NIC
    assert_eq!(nics.len(), 1);

    // Validate that every sled (except the instance's sled) now has a V2P
    // mapping for this instance
    let apictx = &cptestctx.server.apictx();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., authz_instance, db_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance.identity.id)
        .fetch()
        .await
        .unwrap();

    let guest_nics = datastore
        .derive_guest_network_interface_info(&opctx, &authz_instance)
        .await
        .unwrap();
    assert_eq!(guest_nics.len(), 1);

    let mut sled_agents: Vec<&Arc<SledAgent>> =
        additional_sleds.iter().map(|x| &x.sled_agent).collect();
    sled_agents.push(&cptestctx.sled_agent.sled_agent);

    for sled_agent in &sled_agents {
        let v2p_mappings = sled_agent.v2p_mappings.lock().await;
        if sled_agent.id != db_instance.runtime().sled_id {
            assert!(!v2p_mappings.is_empty());
            assert_eq!(
                v2p_mappings.get(&nics[0].identity.id).unwrap().len(),
                1
            );

            // Confirm that OPTE was passed the correct information
            let mapping = &v2p_mappings.get(&nics[0].identity.id).unwrap()[0];
            assert_eq!(mapping.virtual_ip, nics[0].ip);
            assert_eq!(mapping.virtual_mac, nics[0].mac);
            assert_eq!(mapping.physical_host_ip, sled_agent.ip);
            assert_eq!(mapping.vni, guest_nics[0].vni.clone().into());
        } else {
            assert!(v2p_mappings.is_empty());
        }
    }

    // Delete the instance
    instance_simulate(nexus, &instance.identity.id).await;
    instance_post(&client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance.identity.id).await;

    let instance_url = get_instance_url(instance_name);
    NexusRequest::object_delete(client, &instance_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // Validate that every sled no longer has the V2P mapping for this instance
    for sled_agent in &sled_agents {
        let v2p_mappings = sled_agent.v2p_mappings.lock().await;
        if sled_agent.id != db_instance.runtime().sled_id {
            assert!(!v2p_mappings.is_empty());
            assert!(v2p_mappings.get(&nics[0].identity.id).unwrap().is_empty());
        } else {
            assert!(v2p_mappings.is_empty());
        }
    }
}

async fn instance_get(
    client: &ClientTestContext,
    instance_url: &str,
) -> Instance {
    NexusRequest::object_get(client, instance_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

async fn instances_list(
    client: &ClientTestContext,
    instances_url: &str,
) -> Vec<Instance> {
    NexusRequest::iter_collection_authn(client, instances_url, "", None)
        .await
        .expect("failed to list Instances")
        .all_items
}

/// Convenience function for starting, stopping, or rebooting an instance.
pub enum InstanceOp {
    Start,
    Stop,
    Reboot,
}

pub async fn instance_post(
    client: &ClientTestContext,
    instance_name: &str,
    which: InstanceOp,
) -> Instance {
    let url = get_instance_url(
        format!(
            "{}/{}",
            instance_name,
            match which {
                InstanceOp::Start => "start",
                InstanceOp::Stop => "stop",
                InstanceOp::Reboot => "reboot",
            }
        )
        .as_str(),
    );
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

fn instances_eq(instance1: &Instance, instance2: &Instance) {
    identity_eq(&instance1.identity, &instance2.identity);
    assert_eq!(instance1.project_id, instance2.project_id);

    let InstanceCpuCount(nfoundcpus1) = instance1.ncpus;
    let InstanceCpuCount(nfoundcpus2) = instance2.ncpus;
    assert_eq!(nfoundcpus1, nfoundcpus2);

    assert_eq!(instance1.memory.to_bytes(), instance2.memory.to_bytes());
    assert_eq!(instance1.hostname, instance2.hostname);
    assert_eq!(instance1.runtime.run_state, instance2.runtime.run_state);
    assert_eq!(
        instance1.runtime.time_run_state_updated,
        instance2.runtime.time_run_state_updated
    );
}

/// Simulate completion of an ongoing instance state transition.  To do this, we
/// have to look up the instance, then get the sled agent associated with that
/// instance, and then tell it to finish simulating whatever async transition is
/// going on.
pub async fn instance_simulate(nexus: &Arc<Nexus>, id: &Uuid) {
    let sa = nexus.instance_sled_by_id(id).await.unwrap();
    sa.instance_finish_transition(*id).await;
}
