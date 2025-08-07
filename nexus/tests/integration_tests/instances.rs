// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests basic instance support in the API

use super::external_ips::floating_ip_get;
use super::external_ips::get_floating_ip_by_id_url;
use super::metrics::{assert_silo_metrics, assert_system_metrics};

use http::StatusCode;
use http::method::Method;
use itertools::Itertools;
use nexus_auth::authz::Action;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_interface::NexusServer;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::resource_helpers::assert_ip_pool_utilization;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_floating_ip;
use nexus_test_utils::resource_helpers::create_ip_pool;
use nexus_test_utils::resource_helpers::create_local_user;
use nexus_test_utils::resource_helpers::create_silo;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::link_ip_pool;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::object_create_error;
use nexus_test_utils::resource_helpers::object_delete;
use nexus_test_utils::resource_helpers::object_delete_error;
use nexus_test_utils::resource_helpers::object_get;
use nexus_test_utils::resource_helpers::object_put;
use nexus_test_utils::resource_helpers::object_put_error;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::resource_helpers::test_params;
use nexus_test_utils::start_sled_agent_with_config;
use nexus_test_utils::wait_for_producer;
use nexus_types::external_api::params::SshKeyCreate;
use nexus_types::external_api::shared::IpKind;
use nexus_types::external_api::shared::IpRange;
use nexus_types::external_api::shared::Ipv4Range;
use nexus_types::external_api::shared::SiloIdentityMode;
use nexus_types::external_api::views::Sled;
use nexus_types::external_api::views::SshKey;
use nexus_types::external_api::{params, views};
use nexus_types::identity::Resource;
use nexus_types::internal_api::params::InstanceMigrateRequest;
use nexus_types::silo::DEFAULT_SILO_ID;
use omicron_common::api::external::AffinityPolicy;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use omicron_common::api::external::FailureDomain;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceAutoRestartPolicy;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::InstanceCpuPlatform;
use omicron_common::api::external::InstanceNetworkInterface;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::Vni;
use omicron_common::api::internal::shared::ResolvedVpcRoute;
use omicron_common::api::internal::shared::RouterId;
use omicron_common::api::internal::shared::RouterKind;
use omicron_nexus::Nexus;
use omicron_nexus::TestInterfaces as _;
use omicron_nexus::app::MAX_MEMORY_BYTES_PER_INSTANCE;
use omicron_nexus::app::MAX_VCPU_PER_INSTANCE;
use omicron_nexus::app::MIN_MEMORY_BYTES_PER_INSTANCE;
use omicron_sled_agent::sim::SledAgent;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};
use sled_agent_client::TestInterfaces as _;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use dropshot::test_util::ClientTestContext;
use dropshot::{HttpErrorResponseBody, ResultsPage};

use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::{
    create_instance, create_instance_with, create_instance_with_error,
    create_project,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::shared::SiloRole;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::poll::CondCheckError;

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

fn get_instance_start_url(instance_name: &str) -> String {
    format!("/v1/instances/{}/start?{}", instance_name, get_project_selector())
}

fn get_instance_stop_url(instance_name: &str) -> String {
    format!("/v1/instances/{}/stop?{}", instance_name, get_project_selector())
}

fn get_disks_url() -> String {
    format!("/v1/disks?{}", get_project_selector())
}

fn anti_affinity_groups_url() -> String {
    format!("/v1/anti-affinity-groups?{}", get_project_selector())
}

fn default_vpc_subnets_url() -> String {
    format!("/v1/vpc-subnets?{}&vpc=default", get_project_selector())
}

pub async fn create_project_and_pool(
    client: &ClientTestContext,
) -> views::Project {
    create_default_ip_pool(client).await;
    create_project(client, PROJECT_NAME).await
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

// Regression tests for https://github.com/oxidecomputer/omicron/issues/4923.
#[nexus_test]
async fn test_cannot_create_instance_with_bad_hostname(
    cptestctx: &ControlPlaneTestContext,
) {
    test_create_instance_with_bad_hostname_impl(cptestctx, "bad_hostname")
        .await;
}

#[nexus_test]
async fn test_cannot_create_instance_with_empty_hostname(
    cptestctx: &ControlPlaneTestContext,
) {
    test_create_instance_with_bad_hostname_impl(cptestctx, "").await;
}

async fn test_create_instance_with_bad_hostname_impl(
    cptestctx: &ControlPlaneTestContext,
    hostname: &str,
) {
    let client = &cptestctx.external_client;
    let _project = create_project_and_pool(client).await;

    // Create an instance, with what should be an invalid hostname.
    //
    // We'll do this by creating a _valid_ set of parameters, convert it to
    // JSON, and then muck with the hostname.
    let instance_name = "happy-accident";
    let params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: format!("instance {:?}", instance_name),
        },
        ncpus: InstanceCpuCount(4),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "the-host".parse().unwrap(),
        user_data:
            b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                .to_vec(),
        network_interfaces: Default::default(),
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false,
        ssh_public_keys: None,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };
    let mut body: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&params).unwrap()).unwrap();
    body["hostname"] = hostname.into();
    let err = create_instance_with_error(
        client,
        PROJECT_NAME,
        &body,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(err.message.contains("Hostnames must comply with RFC 1035"));
}

#[nexus_test]
async fn test_instance_access(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let project = create_project_and_pool(client).await;

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
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let instance_name = "just-rainsticks";

    create_project_and_pool(&client).await;

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
    assert_eq!(instance.hostname.as_str(), "the-host");
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
                hostname: instance.hostname.parse().unwrap(),
                user_data: vec![],
                ssh_public_keys: None,
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::Default,
                external_ips: vec![],
                disks: vec![],
                boot_disk: None,
                cpu_platform: None,
                start: true,
                auto_restart_policy: Default::default(),
                anti_affinity_groups: Vec::new(),
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
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    instance_simulate(nexus, &instance_id).await;
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
    instance_simulate(nexus, &instance_id).await;
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
    instance_simulate(nexus, &instance_id).await;
    let instance_next =
        instance_wait_for_state(client, instance_id, InstanceState::Stopped)
            .await;
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
    instance_simulate(nexus, &instance_id).await;
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
    instance_simulate(nexus, &instance_id).await;
    let instance_next =
        instance_wait_for_state(client, instance_id, InstanceState::Stopped)
            .await;
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
        get_instance_start_url(instance_name).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::POST,
        get_instance_stop_url(instance_name).as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test(extra_sled_agents = 3)]
async fn test_instance_start_creates_networking_state(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let instance_name = "series-of-tubes";

    // This test requires some additional sleds that can receive V2P mappings.
    let additional_sleds: Vec<_> = cptestctx.extra_sled_agents().collect();

    create_project_and_pool(&client).await;
    let instance_url = get_instance_url(instance_name);
    let instance = create_instance(client, PROJECT_NAME, instance_name).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Drive the instance to Running, then stop it.
    instance_simulate(nexus, &instance_id).await;
    instance_post(&client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    // Forcibly clear the instance's V2P mappings to simulate what happens when
    // the control plane comes up when an instance is stopped.
    let mut sled_agents: Vec<&Arc<SledAgent>> =
        additional_sleds.iter().map(|x| &x.sled_agent).collect();

    sled_agents.push(&cptestctx.first_sled_agent());
    for agent in &sled_agents {
        agent.v2p_mappings.lock().unwrap().clear();
    }

    // Start the instance and make sure that it gets to Running.
    instance_post(&client, instance_name, InstanceOp::Start).await;
    instance_simulate(nexus, &instance_id).await;
    let instance = instance_get(&client, &instance_url).await;
    assert_eq!(instance.runtime.run_state, InstanceState::Running);

    // Now ensure the V2P mappings have been reestablished everywhere.
    let nics_url =
        format!("/v1/network-interfaces?instance={}", instance.identity.id,);

    let nics =
        objects_list_page_authz::<InstanceNetworkInterface>(client, &nics_url)
            .await
            .items;

    assert_eq!(nics.len(), 1);
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., authz_instance) = LookupPath::new(&opctx, datastore)
        .instance_id(instance.identity.id)
        .lookup_for(nexus_db_queries::authz::Action::Read)
        .await
        .unwrap();

    let guest_nics = datastore
        .derive_guest_network_interface_info(&opctx, &authz_instance)
        .await
        .unwrap();

    assert_eq!(guest_nics.len(), 1);
    for agent in &sled_agents {
        assert_sled_v2p_mappings(agent, &nics[0], guest_nics[0].vni).await;
    }

    // Ensure that the target sled agent for our instance has received
    // up-to-date VPC routes.
    let with_vmm = datastore
        .instance_fetch_with_vmm(&opctx, &authz_instance)
        .await
        .unwrap();

    let mut checked = false;
    for agent in &sled_agents {
        if Some(agent.id) == with_vmm.sled_id() {
            assert_sled_vpc_routes(
                agent,
                &opctx,
                datastore,
                nics[0].subnet_id,
                guest_nics[0].vni,
            )
            .await;
            checked = true;
        }
    }
    assert!(checked);
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_instance_migrate(cptestctx: &ControlPlaneTestContext) {
    use nexus_db_model::Migration;
    use omicron_common::api::internal::nexus::MigrationState;
    async fn migration_fetch(
        cptestctx: &ControlPlaneTestContext,
        migration_id: Uuid,
    ) -> Migration {
        use async_bb8_diesel::AsyncRunQueryDsl;
        use diesel::prelude::*;
        use nexus_db_schema::schema::migration::dsl;

        let datastore =
            cptestctx.server.server_context().nexus.datastore().clone();
        let db_state = dsl::migration
            // N.B. that for the purposes of this test, we explicitly should
            // *not* filter out migrations that are marked as deleted, as the
            // migration record is marked as deleted once the migration completes.
            .filter(dsl::id.eq(migration_id))
            .select(Migration::as_select())
            .get_results_async::<Migration>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .unwrap();

        info!(&cptestctx.logctx.log, "refetched migration info from db";
                "migration" => ?db_state);

        db_state.into_iter().next().unwrap()
    }

    let client = &cptestctx.external_client;
    let internal_client = &cptestctx.internal_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let instance_name = "bird-ecology";

    // Get the second sled to migrate to/from.
    let default_sled_id = cptestctx.first_sled_id();
    let other_sled_id = cptestctx.second_sled_id();

    create_project_and_pool(&client).await;
    let instance_url = get_instance_url(instance_name);

    // Explicitly create an instance with no disks. Simulated sled agent assumes
    // that disks are co-located with their instances.
    let instance = nexus_test_utils::resource_helpers::create_instance_with(
        client,
        PROJECT_NAME,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::Default,
        Vec::<params::InstanceDiskAttachment>::new(),
        Vec::<params::ExternalIpCreate>::new(),
        true,
        Default::default(),
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Poke the instance into an active state.
    instance_simulate(nexus, &instance_id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);

    let sled_info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("running instance should have a sled");

    let original_sled = sled_info.sled_id;
    let dst_sled_id = if original_sled == default_sled_id {
        other_sled_id
    } else {
        default_sled_id
    };

    let migrate_url =
        format!("/instances/{}/migrate", &instance_id.to_string());
    let instance = NexusRequest::new(
        RequestBuilder::new(internal_client, Method::POST, &migrate_url)
            .body(Some(&InstanceMigrateRequest {
                dst_sled_id: dst_sled_id.into_untyped_uuid(),
            }))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<Instance>()
    .unwrap();

    let new_sled_info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("running instance should have a sled");

    let current_sled = new_sled_info.sled_id;
    assert_eq!(current_sled, original_sled);

    // Ensure that both sled agents report that the migration is in progress.
    let migration_id = {
        let datastore = apictx.nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            datastore.clone(),
        );
        let (.., authz_instance) = LookupPath::new(&opctx, datastore)
            .instance_id(instance.identity.id)
            .lookup_for(nexus_db_queries::authz::Action::Read)
            .await
            .unwrap();
        datastore
            .instance_refetch(&opctx, &authz_instance)
            .await
            .unwrap()
            .runtime_state
            .migration_id
            .expect("since we've started a migration, the instance record must have a migration id!")
    };
    let migration = dbg!(migration_fetch(cptestctx, migration_id).await);
    assert_eq!(migration.target_state, MigrationState::Pending.into());
    assert_eq!(migration.source_state, MigrationState::Pending.into());

    let info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("instance should be on a sled");
    let src_propolis_id = info.propolis_id;
    let dst_propolis_id =
        info.dst_propolis_id.expect("instance should have a migration target");

    // Simulate the migration. We will use `instance_single_step_on_sled` to
    // single-step both sled-agents through the migration state machine and
    // ensure that the migration state looks nice at each step.
    instance_simulate_migration_source(
        cptestctx,
        nexus,
        original_sled,
        src_propolis_id,
        migration_id,
    )
    .await;

    // Move source to "migrating".
    vmm_single_step_on_sled(cptestctx, nexus, original_sled, src_propolis_id)
        .await;
    vmm_single_step_on_sled(cptestctx, nexus, original_sled, src_propolis_id)
        .await;

    let migration = dbg!(migration_fetch(cptestctx, migration_id).await);
    assert_eq!(migration.source_state, MigrationState::InProgress.into());
    assert_eq!(migration.target_state, MigrationState::Pending.into());
    let instance = instance_get(&client, &instance_url).await;
    assert_eq!(instance.runtime.run_state, InstanceState::Migrating);

    // Move target to "migrating".
    vmm_single_step_on_sled(cptestctx, nexus, dst_sled_id, dst_propolis_id)
        .await;
    vmm_single_step_on_sled(cptestctx, nexus, dst_sled_id, dst_propolis_id)
        .await;

    let migration = dbg!(migration_fetch(cptestctx, migration_id).await);
    assert_eq!(migration.source_state, MigrationState::InProgress.into());
    assert_eq!(migration.target_state, MigrationState::InProgress.into());
    let instance = instance_get(&client, &instance_url).await;
    assert_eq!(instance.runtime.run_state, InstanceState::Migrating);

    // Move the source to "completed"
    vmm_simulate_on_sled(cptestctx, nexus, original_sled, src_propolis_id)
        .await;

    let migration = dbg!(migration_fetch(cptestctx, migration_id).await);
    assert_eq!(migration.source_state, MigrationState::Completed.into());
    assert_eq!(migration.target_state, MigrationState::InProgress.into());
    let instance = dbg!(instance_get(&client, &instance_url).await);
    assert_eq!(instance.runtime.run_state, InstanceState::Migrating);

    // Move the target to "completed".
    vmm_simulate_on_sled(cptestctx, nexus, dst_sled_id, dst_propolis_id).await;

    instance_wait_for_state(&client, instance_id, InstanceState::Running).await;

    let current_sled = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("migrated instance should still have a sled")
        .sled_id;

    assert_eq!(current_sled, dst_sled_id);

    let migration = dbg!(migration_fetch(cptestctx, migration_id).await);
    assert_eq!(migration.target_state, MigrationState::Completed.into());
    assert_eq!(migration.source_state, MigrationState::Completed.into());
}

#[nexus_test(extra_sled_agents = 3)]
async fn test_instance_migrate_v2p_and_routes(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let internal_client = &cptestctx.internal_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let instance_name = "desert-locust";

    // Get the extra test sleds.
    let other_sleds: Vec<_> = cptestctx.extra_sled_agents().collect();

    // Set up the project and test instance.
    create_project_and_pool(client).await;

    let instance = nexus_test_utils::resource_helpers::create_instance_with(
        client,
        PROJECT_NAME,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::Default,
        // Omit disks: simulated sled agent assumes that disks are always co-
        // located with their instances.
        Vec::<params::InstanceDiskAttachment>::new(),
        Vec::<params::ExternalIpCreate>::new(),
        true,
        Default::default(),
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // The default configuration gives one NIC.
    let nics_url = format!("/v1/network-interfaces?instance={}", instance_id);
    let nics =
        objects_list_page_authz::<InstanceNetworkInterface>(client, &nics_url)
            .await
            .items;
    assert_eq!(nics.len(), 1);

    // Poke the instance into an active state.
    instance_simulate(nexus, &instance_id).await;
    let instance_url = get_instance_url(instance_name);
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);

    // Ensure that all of the V2P information is correct.
    let (.., authz_instance) = LookupPath::new(&opctx, datastore)
        .instance_id(instance_id.into_untyped_uuid())
        .lookup_for(nexus_db_queries::authz::Action::Read)
        .await
        .unwrap();
    let guest_nics = datastore
        .derive_guest_network_interface_info(&opctx, &authz_instance)
        .await
        .unwrap();

    let original_sled_id = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("running instance should have a sled")
        .sled_id;

    let mut sled_agents = vec![cptestctx.first_sled_agent().clone()];
    sled_agents.extend(other_sleds.iter().map(|tup| tup.sled_agent.clone()));
    for sled_agent in &sled_agents {
        assert_sled_v2p_mappings(sled_agent, &nics[0], guest_nics[0].vni).await;
    }

    let testctx_sled_id = cptestctx.first_sled_agent().id;
    let dst_sled_id = if original_sled_id == testctx_sled_id {
        other_sleds[0].sled_agent.id
    } else {
        testctx_sled_id
    };

    // Kick off migration and simulate its completion on the target.
    let migrate_url =
        format!("/instances/{}/migrate", &instance_id.to_string());
    let _ = NexusRequest::new(
        RequestBuilder::new(internal_client, Method::POST, &migrate_url)
            .body(Some(&InstanceMigrateRequest {
                dst_sled_id: dst_sled_id.into_untyped_uuid(),
            }))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<Instance>()
    .unwrap();

    let migration_id = {
        let datastore = apictx.nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            datastore.clone(),
        );
        let (.., authz_instance) = LookupPath::new(&opctx, datastore)
            .instance_id(instance.identity.id)
            .lookup_for(nexus_db_queries::authz::Action::Read)
            .await
            .unwrap();
        datastore
            .instance_refetch(&opctx, &authz_instance)
            .await
            .unwrap()
            .runtime_state
            .migration_id
            .expect("since we've started a migration, the instance record must have a migration id!")
    };

    let info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("instance should be on a sled");
    let src_propolis_id = info.propolis_id;
    let dst_propolis_id =
        info.dst_propolis_id.expect("instance should have a migration target");

    // Tell both sled-agents to pretend to do the migration.
    instance_simulate_migration_source(
        cptestctx,
        nexus,
        original_sled_id,
        src_propolis_id,
        migration_id,
    )
    .await;
    vmm_simulate_on_sled(cptestctx, nexus, original_sled_id, src_propolis_id)
        .await;
    vmm_simulate_on_sled(cptestctx, nexus, dst_sled_id, dst_propolis_id).await;
    instance_wait_for_state(&client, instance_id, InstanceState::Running).await;

    let current_sled = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("migrated instance should have a sled")
        .sled_id;
    assert_eq!(current_sled, dst_sled_id);

    for sled_agent in &sled_agents {
        // Completing migration published a sled ID update that should have
        // programmed new V2P mappings to all sleds other than the destination
        // sled.
        //
        // TODO(#3107): As above, the destination sled's mappings are not
        // updated here because Nexus presumes that the instance's new sled
        // agent will have updated any mappings there. Remove this bifurcation
        // when Nexus programs all mappings explicitly.
        if sled_agent.id != dst_sled_id {
            assert_sled_v2p_mappings(sled_agent, &nics[0], guest_nics[0].vni)
                .await;
        } else {
            assert_sled_vpc_routes(
                sled_agent,
                &opctx,
                datastore,
                nics[0].subnet_id,
                guest_nics[0].vni,
            )
            .await;
        }
    }
}

// Verifies that if a request to reboot or stop an instance fails because of a
// 404 error from sled agent, then the instance moves to the Failed state, and
// can be restarted once it has transitioned to that state.
#[nexus_test]
async fn test_instance_failed_after_sled_agent_forgets_vmm_can_be_restarted(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let instance_name = "losing-is-fun";
    let instance_id = make_forgotten_instance(
        &cptestctx,
        instance_name,
        InstanceAutoRestartPolicy::Never,
    )
    .await;

    // Attempting to reboot the forgotten instance will result in a 404
    // NO_SUCH_INSTANCE from the sled-agent, which Nexus turns into a 503.
    expect_instance_reboot_fail(
        client,
        instance_name,
        http::StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;

    // Wait for the instance to transition to Failed.
    instance_wait_for_state(client, instance_id, InstanceState::Failed).await;

    // Now, the instance should be restartable.
    expect_instance_start_ok(client, instance_name).await;
}

// Verifies that if a request to reboot or stop an instance fails because of a
// 404 error from sled agent, then the instance moves to the Failed state, and
// can be deleted once it has transitioned to that state..
#[nexus_test]
async fn test_instance_failed_after_sled_agent_forgets_vmm_can_be_deleted(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let instance_name = "losing-is-fun";
    let instance_id = make_forgotten_instance(
        &cptestctx,
        instance_name,
        InstanceAutoRestartPolicy::Never,
    )
    .await;

    // Attempting to reboot the forgotten instance will result in a 404
    // NO_SUCH_INSTANCE from the sled-agent, which Nexus turns into a 503.
    expect_instance_reboot_fail(
        client,
        instance_name,
        http::StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;

    // Wait for the instance to transition to Failed.
    instance_wait_for_state(client, instance_id, InstanceState::Failed).await;

    // Now, the instance should be deleteable.
    expect_instance_delete_ok(client, instance_name).await;
}

// Verifies that if a request to reboot or stop an instance fails because of a
// 404 error from sled agent, then the instance moves to the Failed state, and
// can then be Stopped once it has transitioned to Failed.
#[nexus_test]
async fn test_instance_failed_after_sled_agent_forgets_vmm_can_be_stopped(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let instance_name = "losing-is-fun";
    let instance_id = make_forgotten_instance(
        &cptestctx,
        instance_name,
        InstanceAutoRestartPolicy::Never,
    )
    .await;

    // Attempting to reboot the forgotten instance will result in a 404
    // NO_SUCH_INSTANCE from the sled-agent, which Nexus turns into a 503.
    expect_instance_reboot_fail(
        client,
        instance_name,
        http::StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;

    // Wait for the instance to transition to Failed.
    instance_wait_for_state(client, instance_id, InstanceState::Failed).await;

    // Now, it should be possible to stop the instance.
    let instance_next =
        instance_post(&client, instance_name, InstanceOp::Stop).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopped);

    // Now, the Stopped nstance should be deleteable..
    expect_instance_delete_ok(client, instance_name).await;
}

// Verifies that the instance-watcher background task transitions an instance
// to Failed when the sled-agent returns a 404, and that the instance can be
// deleted after it transitions to Failed.
#[nexus_test]
async fn test_instance_failed_by_instance_watcher_can_be_deleted(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let instance_name = "losing-is-fun";
    let instance_id = make_forgotten_instance(
        &cptestctx,
        instance_name,
        InstanceAutoRestartPolicy::Never,
    )
    .await;

    nexus_test_utils::background::activate_background_task(
        &cptestctx.internal_client,
        "instance_watcher",
    )
    .await;

    // Wait for the instance to transition to Failed.
    instance_wait_for_state(client, instance_id, InstanceState::Failed).await;

    // Now, the instance should be deleteable.
    expect_instance_delete_ok(&client, instance_name).await;
}

// Verifies that the instance-watcher background task transitions an instance
// to Failed when the sled-agent returns a 404, and that the instance can be
// deleted after it transitions to Failed.
#[nexus_test]
async fn test_instance_failed_by_instance_watcher_can_be_restarted(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let instance_name = "losing-is-fun";
    let instance_id = make_forgotten_instance(
        &cptestctx,
        instance_name,
        InstanceAutoRestartPolicy::Never,
    )
    .await;

    nexus_test_utils::background::activate_background_task(
        &cptestctx.internal_client,
        "instance_watcher",
    )
    .await;

    // Wait for the instance to transition to Failed.
    instance_wait_for_state(client, instance_id, InstanceState::Failed).await;

    // Now, the instance should be deleteable.
    expect_instance_delete_ok(&client, instance_name).await;
}

// Verified that instances currently on Expunged sleds are marked as failed.
#[nexus_test(extra_sled_agents = 1)]
async fn test_instance_failed_when_on_expunged_sled(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    create_project_and_pool(&client).await;

    // Make sure all instances allocate to the first sled - make the second sled
    // agent non-provisionable.
    let (authz_sled, ..) = LookupPath::new(&opctx, datastore)
        .sled_id(cptestctx.second_sled_id().into_untyped_uuid())
        .lookup_for(Action::Modify)
        .await
        .expect("lookup authz_sled");

    datastore
        .sled_set_provision_policy(
            &opctx,
            &authz_sled,
            views::SledProvisionPolicy::NonProvisionable,
        )
        .await
        .expect("set sled provision policy");

    // Create and start the test instances.
    let instance1_name = "romeo";
    let instance2_name = "juliet";
    let instance3_name = "mercutio";

    let mk_instance =
        |name: &'static str, auto_restart: InstanceAutoRestartPolicy| async move {
            let instance_url = get_instance_url(name);
            let instance = create_instance_with(
                client,
                PROJECT_NAME,
                name,
                &params::InstanceNetworkInterfaceAttachment::Default,
                // Disks=
                Vec::<params::InstanceDiskAttachment>::new(),
                // External IPs=
                Vec::<params::ExternalIpCreate>::new(),
                true,
                Some(auto_restart),
            )
            .await;
            let instance_id =
                InstanceUuid::from_untyped_uuid(instance.identity.id);
            instance_simulate(nexus, &instance_id).await;
            let instance_next = instance_get(&client, &instance_url).await;
            assert_eq!(instance_next.runtime.run_state, InstanceState::Running);

            slog::info!(
                &cptestctx.logctx.log,
                "test instance is running";
                "instance_name" => %name,
                "instance_id" => %instance_id,
            );

            instance_id
        };

    // We are going to manually attempt to delete/restart these instances when
    // they go to `Failed`, so don't allow them to reincarnate.
    let instance1_id =
        mk_instance(instance1_name, InstanceAutoRestartPolicy::Never).await;
    let instance2_id =
        mk_instance(instance2_name, InstanceAutoRestartPolicy::Never).await;
    let instance3_id =
        mk_instance(instance3_name, InstanceAutoRestartPolicy::BestEffort)
            .await;

    // Set the second sled to provisionable now that the instances have all been
    // created

    datastore
        .sled_set_provision_policy(
            &opctx,
            &authz_sled,
            views::SledProvisionPolicy::Provisionable,
        )
        .await
        .expect("set sled provision policy");

    // Expunge the sled
    let default_sled_id = cptestctx.first_sled_id();
    slog::info!(
        &cptestctx.logctx.log,
        "expunging sled";
        "sled_id" => %default_sled_id,
    );
    let int_client = &cptestctx.internal_client;
    int_client
        .make_request(
            Method::POST,
            "/sleds/expunge",
            Some(params::SledSelector {
                sled: default_sled_id.into_untyped_uuid(),
            }),
            StatusCode::OK,
        )
        .await
        .unwrap();

    // Wait for both instances to transition to Failed.
    instance_wait_for_state(client, instance1_id, InstanceState::Failed).await;
    instance_wait_for_state(client, instance2_id, InstanceState::Failed).await;

    // Now, the instances should be deleteable...
    expect_instance_delete_ok(&client, instance1_name).await;
    // ...or restartable.
    expect_instance_start_ok(client, instance2_name).await;

    // The restarted instance should now transition back to `Running`, on its
    // new sled.
    instance_wait_for_simulated_transition(
        &cptestctx,
        &instance2_id,
        InstanceState::Running,
    )
    .await;

    // The auto-restartable instance should be...restarted automatically.
    instance_wait_for_simulated_transition(
        &cptestctx,
        &instance3_id,
        InstanceState::Running,
    )
    .await;
}

// Verifies that the instance-watcher background task transitions an instance
// to Failed when the sled-agent returns a 404, and that the instance can be
// deleted after it transitions to Failed.
#[nexus_test]
async fn test_instance_failed_by_instance_watcher_automatically_reincarnates(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let instance_id = dbg!(
        make_forgotten_instance(
            &cptestctx,
            "resurgam",
            InstanceAutoRestartPolicy::BestEffort,
        )
        .await
    );

    dbg!(
        nexus_test_utils::background::activate_background_task(
            &cptestctx.internal_client,
            "instance_watcher",
        )
        .await
    );

    // Now, it should be automatically restarted!

    // N.B.: it's tempting to want to wait for the instance to transition
    // the way to `Failed`, or at least to `Stopping`, before it transitions to
    // `Starting` again, but
    // unfortunately, because the instance-update saga triggered by the
    // instance-watcher task immediately triggers instance reincarnation, and
    // this instance will be the only instance eligible to reincarnate, it's
    // _very_ easy for the test to miss the very brief period of time it's in
    // the `Failed` state, especially if we only poll once per second in
    // `instance_wait_for_state`.
    dbg!(
        instance_wait_for_state(client, instance_id, InstanceState::Starting)
            .await
    );
    // Wait for the VMM to be registered with the sim sled-agent before poking
    // it.
    dbg!(instance_wait_for_vmm_registration(cptestctx, &instance_id).await);
    // Now, we can actually poke the instance.
    dbg!(
        instance_wait_for_simulated_transition(
            &cptestctx,
            &instance_id,
            InstanceState::Running
        )
        .await
    );
}

// Verifies that if an instance transitions to `Failed` due to a failed request
// to stop it, it will not automatically restart, even if it is configured to
// restart it automatically on failure.
#[nexus_test]
async fn test_instance_failed_by_stop_request_does_not_reincarnate(
    cptestctx: &ControlPlaneTestContext,
) {
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let client = &cptestctx.external_client;
    let instance_name = "resurgam";
    let instance_url = get_instance_url(instance_name);
    let instance_id = dbg!(
        make_forgotten_instance(
            &cptestctx,
            instance_name,
            InstanceAutoRestartPolicy::BestEffort,
        )
        .await
    );

    // Attempting to stop the forgotten instance will result in a 404
    // NO_SUCH_INSTANCE from the sled-agent, which Nexus turns into a 503.
    expect_instance_stop_fail(
        client,
        instance_name,
        http::StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;

    // Wait for the instance to transition to Failed.
    dbg!(
        instance_wait_for_state(client, instance_id, InstanceState::Failed)
            .await
    );

    // Activate the reincarnation task.
    dbg!(
        nexus_test_utils::background::activate_background_task(
            &cptestctx.internal_client,
            "instance_reincarnation",
        )
        .await
    );

    // Unfortunately there isn't really a great way to assert "no start saga has
    // been started", so we'll do the somewhat jankier thing of waiting a bit
    // and making sure that the instance doesn't transition to Starting.
    for secs in 0..30 {
        let state = instance_get(client, &instance_url).await;
        assert_eq!(
            state.runtime.run_state,
            InstanceState::Failed,
            "instance transitioned out of Failed (to {}) after {secs} \
             seconds! state: {:#?}",
            state.runtime.run_state,
            state
        );
        assert_eq!(state.runtime.time_last_auto_restarted, None);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Okay, now let's try restarting the instance, failing it, and then
    // asserting it *does* reincarnate.
    dbg!(expect_instance_start_ok(client, instance_name).await);
    instance_simulate(nexus, &instance_id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);

    // Forcibly unregister the instance from the sled-agent without telling
    // Nexus. It will now behave as though it has forgotten the instance and
    // return a 404 error with the "NO_SUCH_INSTANCE" error code
    let vmm_id = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("running instance must be on a sled")
        .propolis_id;
    let sled_agent = cptestctx.first_sled_agent();
    sled_agent
        .instance_unregister(vmm_id)
        .await
        .expect("instance_unregister must succeed");

    // Attempting to reboot the instance will fail.
    expect_instance_reboot_fail(
        client,
        instance_name,
        http::StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;

    // Because the instance can be restarted, the discovery of its failure above
    // will result in the reincarnation background task being activated. Don't
    // bother checking for Failed here because it's possible that task could
    // have restarted the instance before we poll it (issue #8119 has two cases
    // of just this!). Instead, we'll just wait for the final Starting state
    // that reincarnation should leave the instance in.
    dbg!(
        instance_wait_for_state(client, instance_id, InstanceState::Starting)
            .await
    );
}

#[nexus_test]
async fn test_instances_are_not_marked_failed_on_other_sled_agent_errors(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let instance_name = "my-cool-instance";

    // Create and start the test instance.
    create_project_and_pool(&client).await;
    let instance_url = get_instance_url(instance_name);
    let instance = create_instance(client, PROJECT_NAME, instance_name).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    instance_simulate(nexus, &instance_id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);

    let sled_agent = cptestctx.first_sled_agent();
    sled_agent
        .set_instance_ensure_state_error(Some(
            omicron_common::api::external::Error::internal_error(
                "somebody set up us the bomb",
            ),
        ))
        .await;

    // Attempting to reboot the instance will fail, but it should *not* go to
    // Failed.
    expect_instance_reboot_fail(
        client,
        instance_name,
        http::StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;

    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);

    sled_agent.set_instance_ensure_state_error(None).await;

    expect_instance_reboot_ok(client, instance_name).await;
}

#[nexus_test]
async fn test_instances_are_not_marked_failed_on_other_sled_agent_errors_by_instance_watcher(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let instance_name = "my-cool-instance";

    // Create and start the test instance.
    create_project_and_pool(&client).await;
    let instance_url = get_instance_url(instance_name);
    let instance = create_instance(client, PROJECT_NAME, instance_name).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    instance_simulate(nexus, &instance_id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);

    let sled_agent = cptestctx.first_sled_agent();
    sled_agent
        .set_instance_ensure_state_error(Some(
            omicron_common::api::external::Error::internal_error(
                "you have no chance to survive, make your time",
            ),
        ))
        .await;

    nexus_test_utils::background::activate_background_task(
        &cptestctx.internal_client,
        "instance_watcher",
    )
    .await;

    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);
}

/// Prepare an instance and sled-agent for one of the "instance fails after
/// sled-agent forgets VMM" tests.
async fn make_forgotten_instance(
    cptestctx: &ControlPlaneTestContext,
    instance_name: &str,
    auto_restart: InstanceAutoRestartPolicy,
) -> InstanceUuid {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;

    // Create and start the test instance.
    create_project_and_pool(&client).await;
    let instance_url = get_instance_url(instance_name);
    let instance = create_instance_with(
        client,
        PROJECT_NAME,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::Default,
        // Disks=
        Vec::<params::InstanceDiskAttachment>::new(),
        // External IPs=
        Vec::<params::ExternalIpCreate>::new(),
        true,
        Some(auto_restart),
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    instance_simulate(nexus, &instance_id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);

    // Forcibly unregister the instance from the sled-agent without telling
    // Nexus. It will now behave as though it has forgotten the instance and
    // return a 404 error with the "NO_SUCH_INSTANCE" error code
    let vmm_id = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("running instance must be on a sled")
        .propolis_id;
    let sled_agent = cptestctx.first_sled_agent();
    sled_agent
        .instance_unregister(vmm_id)
        .await
        .expect("instance_unregister must succeed");

    instance_id
}

async fn expect_instance_delete_ok(
    client: &ClientTestContext,
    instance_name: &str,
) {
    NexusRequest::object_delete(&client, &get_instance_url(instance_name))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("instance should be deleted");
}

async fn expect_instance_reboot_fail(
    client: &ClientTestContext,
    instance_name: &str,
    status: http::StatusCode,
) {
    let url = get_instance_url(format!("{instance_name}/reboot").as_str());
    let builder = RequestBuilder::new(client, Method::POST, &url)
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(status));
    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("expected instance reboot to fail");
}

async fn expect_instance_start_fail(
    client: &ClientTestContext,
    instance_name: &str,
    status: http::StatusCode,
) {
    let url = get_instance_url(format!("{instance_name}/start").as_str());
    let builder = RequestBuilder::new(client, Method::POST, &url)
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(status));
    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("expected instance start to fail");
}

async fn expect_instance_stop_fail(
    client: &ClientTestContext,
    instance_name: &str,
    status: http::StatusCode,
) {
    let url = get_instance_url(format!("{instance_name}/stop").as_str());
    let builder = RequestBuilder::new(client, Method::POST, &url)
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(status));
    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("expected instance stop to fail");
}

async fn expect_instance_reboot_ok(
    client: &ClientTestContext,
    instance_name: &str,
) {
    let url = get_instance_url(format!("{instance_name}/reboot").as_str());
    let builder = RequestBuilder::new(client, Method::POST, &url)
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(http::StatusCode::ACCEPTED));
    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("expected instance reboot to succeed");
}

/// Assert values for fleet, silo, and project using both system and silo
/// metrics endpoints
async fn assert_metrics(
    cptestctx: &ControlPlaneTestContext,
    project_id: Uuid,
    disk: i64,
    cpus: i64,
    ram: i64,
) {
    for id in [None, Some(project_id)] {
        assert_silo_metrics(cptestctx, id, disk, cpus, ram).await;
    }
    for id in [None, Some(DEFAULT_SILO_ID)] {
        assert_system_metrics(cptestctx, id, disk, cpus, ram).await;
    }
}

#[nexus_test]
async fn test_instance_metrics(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();

    // Create an IP pool and project that we'll use for testing.
    let project = create_project_and_pool(&client).await;
    let project_id = project.identity.id;

    // Wait until Nexus is registered as a metric producer with Oximeter.
    wait_for_producer(&cptestctx.oximeter, nexus.id()).await;

    // Query the view of these metrics stored within CRDB
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    assert_eq!(virtual_provisioning_collection.cpus_provisioned, 0);
    assert_eq!(virtual_provisioning_collection.ram_provisioned.to_bytes(), 0);

    assert_metrics(cptestctx, project_id, 0, 0, 0).await;

    // Create and start an instance.
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

    // Stop the instance. This should cause the relevant resources to be
    // deprovisioned.
    let instance =
        instance_post(&client, instance_name, InstanceOp::Stop).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    let expected_cpus = 0;
    let expected_ram =
        i64::try_from(ByteCount::from_gibibytes_u32(0).to_bytes()).unwrap();
    assert_eq!(virtual_provisioning_collection.cpus_provisioned, expected_cpus);
    assert_eq!(
        i64::from(virtual_provisioning_collection.ram_provisioned.0),
        expected_ram
    );
    assert_metrics(cptestctx, project_id, 0, expected_cpus, expected_ram).await;

    // Delete the instance.
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
    assert_metrics(cptestctx, project_id, 0, 0, 0).await;
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_instance_metrics_with_migration(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let internal_client = &cptestctx.internal_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let instance_name = "bird-ecology";

    // Wait until Nexus registers as a producer with Oximeter.
    wait_for_producer(
        &cptestctx.oximeter,
        cptestctx.server.server_context().nexus.id(),
    )
    .await;

    // Use the second sled to migrate to/from.
    let default_sled_id = cptestctx.first_sled_id();
    let other_sled_id = cptestctx.second_sled_id();

    let project = create_project_and_pool(&client).await;
    let project_id = project.identity.id;
    let instance_url = get_instance_url(instance_name);

    // Explicitly create an instance with no disks. Simulated sled agent assumes
    // that disks are co-located with their instances.
    let instance = nexus_test_utils::resource_helpers::create_instance_with(
        client,
        PROJECT_NAME,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::Default,
        Vec::<params::InstanceDiskAttachment>::new(),
        Vec::<params::ExternalIpCreate>::new(),
        true,
        Default::default(),
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Poke the instance into an active state.
    instance_simulate(nexus, &instance_id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);

    // The instance should be provisioned while it's in the running state.
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let check_provisioning_state = |cpus: i64, mem_gib: u32| async move {
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            datastore.clone(),
        );
        let virtual_provisioning_collection = datastore
            .virtual_provisioning_collection_get(&opctx, project_id)
            .await
            .unwrap();
        assert_eq!(
            virtual_provisioning_collection.cpus_provisioned,
            cpus.clone()
        );
        assert_eq!(
            virtual_provisioning_collection.ram_provisioned.0,
            ByteCount::from_gibibytes_u32(mem_gib)
        );
    };

    check_provisioning_state(4, 1).await;

    // Request migration to the other sled. This reserves resources on the
    // target sled, but shouldn't change the virtual provisioning counters.
    let original_sled = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("running instance should have a sled")
        .sled_id;

    let dst_sled_id = if original_sled == default_sled_id {
        other_sled_id
    } else {
        default_sled_id
    };

    // instance is already running on destination sled
    let migrate_url =
        format!("/instances/{}/migrate", &instance_id.to_string());
    let _ = NexusRequest::new(
        RequestBuilder::new(internal_client, Method::POST, &migrate_url)
            .body(Some(&InstanceMigrateRequest {
                dst_sled_id: dst_sled_id.into_untyped_uuid(),
            }))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<Instance>()
    .unwrap();

    let migration_id = {
        let datastore = apictx.nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            datastore.clone(),
        );
        let (.., authz_instance) = LookupPath::new(&opctx, datastore)
            .instance_id(instance.identity.id)
            .lookup_for(nexus_db_queries::authz::Action::Read)
            .await
            .unwrap();
        datastore
            .instance_refetch(&opctx, &authz_instance)
            .await
            .unwrap()
            .runtime_state
            .migration_id
            .expect("since we've started a migration, the instance record must have a migration id!")
    };

    let info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("instance should be on a sled");
    let src_propolis_id = info.propolis_id;
    let dst_propolis_id =
        info.dst_propolis_id.expect("instance should have a migration target");

    // Wait for the instance to be in the `Migrating` state. Otherwise, the
    // subsequent `instance_wait_for_state(..., Running)` may see the `Running`
    // state from the *old* VMM, rather than waiting for the migration to
    // complete.
    instance_simulate_migration_source(
        cptestctx,
        nexus,
        original_sled,
        src_propolis_id,
        migration_id,
    )
    .await;
    vmm_single_step_on_sled(cptestctx, nexus, original_sled, src_propolis_id)
        .await;
    vmm_single_step_on_sled(cptestctx, nexus, dst_sled_id, dst_propolis_id)
        .await;
    instance_wait_for_state(&client, instance_id, InstanceState::Migrating)
        .await;

    check_provisioning_state(4, 1).await;

    // Complete migration on the target. Simulated migrations always succeed.
    // After this the instance should be running and should continue to appear
    // to be provisioned.
    vmm_simulate_on_sled(cptestctx, nexus, original_sled, src_propolis_id)
        .await;
    vmm_simulate_on_sled(cptestctx, nexus, dst_sled_id, dst_propolis_id).await;
    instance_wait_for_state(&client, instance_id, InstanceState::Running).await;

    check_provisioning_state(4, 1).await;

    // Now stop the instance. This should retire the instance's active Propolis
    // and cause the virtual provisioning charges to be released. Note that
    // the source sled still has an active resource charge for the source
    // instance (whose demise wasn't simulated here), but this is intentionally
    // not reflected in the virtual provisioning counters (which reflect the
    // logical states of instances ignoring migration).
    instance_post(&client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(&client, instance_id, InstanceState::Stopped).await;

    check_provisioning_state(0, 0).await;
}

#[nexus_test]
async fn test_instances_create_stopped_start(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let instance_name = "just-rainsticks";

    create_project_and_pool(&client).await;

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
            hostname: "the-host".parse().unwrap(),
            user_data: vec![],
            ssh_public_keys: None,
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::Default,
            external_ips: vec![],
            disks: vec![],
            boot_disk: None,
            cpu_platform: None,
            start: false,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
        },
    )
    .await;
    assert_eq!(instance.runtime.run_state, InstanceState::Stopped);

    // Start the instance.
    let instance_url = get_instance_url(instance_name);
    let instance =
        instance_post(&client, instance_name, InstanceOp::Start).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Now, simulate completion of instance boot and check the state reported.
    instance_simulate(nexus, &instance_id).await;
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
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let instance_name = "just-rainsticks";

    create_project_and_pool(&client).await;

    // Create an instance.
    let instance_url = get_instance_url(instance_name);
    let instance = create_instance(client, PROJECT_NAME, instance_name).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Simulate the instance booting.
    instance_simulate(nexus, &instance_id).await;
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
        "cannot delete instance: instance is running or has not yet fully stopped"
    );

    // Stop the instance
    instance_post(&client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(&client, instance_id, InstanceState::Stopped).await;

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
    assert!(
        error.message.starts_with(
            "unable to parse JSON body: EOF while parsing an object"
        )
    );

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
async fn test_instance_using_image_from_other_project_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project_and_pool(&client).await;

    // Create an image in springfield-squidport.
    let images_url = format!("/v1/images?project={}", PROJECT_NAME);
    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        os: "alpine".to_string(),
        version: "edge".to_string(),
        source: params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    };
    let image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    // Try and fail to create an instance in another project.
    let project = create_project(client, "moes-tavern").await;
    let instances_url =
        format!("/v1/instances?project={}", project.identity.name);
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &instances_url)
            .body(Some(&params::InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name: "stolen".parse().unwrap(),
                    description: "i stole an image".into(),
                },
                ncpus: InstanceCpuCount(4),
                memory: ByteCount::from_gibibytes_u32(1),
                hostname: "stolen".parse().unwrap(),
                user_data: vec![],
                ssh_public_keys: None,
                network_interfaces:
                    params::InstanceNetworkInterfaceAttachment::Default,
                external_ips: vec![],
                disks: vec![params::InstanceDiskAttachment::Create(
                    params::DiskCreate {
                        identity: IdentityMetadataCreateParams {
                            name: "stolen".parse().unwrap(),
                            description: "i stole an image".into(),
                        },
                        disk_source: params::DiskSource::Image {
                            image_id: image.identity.id,
                        },
                        size: ByteCount::from_gibibytes_u32(4),
                    },
                )],
                boot_disk: None,
                cpu_platform: None,
                start: true,
                auto_restart_policy: Default::default(),
                anti_affinity_groups: Vec::new(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "image does not belong to this project");
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
    create_project_and_pool(&client).await;

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
            if0_params.clone(),
        ]);

    // Create the parameters for the instance itself, and create it.
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("unwind-test-inst")).unwrap(),
            description: String::from("instance to test saga unwind"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "inst".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: interface_params.clone(),
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
        hostname: "inst2".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: interface_params,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
            if0_params.clone(),
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

    create_project_and_pool(&client).await;

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
            if0_params.clone(),
        ]);

    // Create the parameters for the instance itself, and create it.
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("nic-test-inst")).unwrap(),
            description: String::from("instance to test multiple nics"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "nic-test".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: interface_params,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,

        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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

    create_project_and_pool(&client).await;
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
        ipv4_block: "172.31.0.0/24".parse().unwrap(),
        ipv6_block: None,
        custom_router: None,
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
        hostname: "nic-test".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: interface_params,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
    let nexus = &cptestctx.server.server_context().nexus;
    let instance_name = "nic-attach-test-inst";

    create_project_and_pool(&client).await;

    // Create the VPC Subnet for the secondary interface
    let secondary_subnet = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("secondary")).unwrap(),
            description: String::from("A secondary VPC subnet"),
        },
        ipv4_block: "172.31.0.0/24".parse().unwrap(),
        ipv6_block: None,
        custom_router: None,
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
        hostname: "nic-test".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::None,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

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
    instance_post(client, instance_name, InstanceOp::Start).await;
    instance_simulate(nexus, &instance_id).await;

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
    instance_post(client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

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
    let nexus = &cptestctx.server.server_context().nexus;
    let instance_name = "nic-update-test-inst";

    create_project_and_pool(&client).await;

    // Create the VPC Subnet for the secondary interface
    let secondary_subnet = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("secondary")).unwrap(),
            description: String::from("A secondary VPC subnet"),
        },
        ipv4_block: "172.31.0.0/24".parse().unwrap(),
        ipv6_block: None,
        custom_router: None,
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
        hostname: "nic-test".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::None,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

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
    instance_post(client, instance_name, InstanceOp::Start).await;
    instance_simulate(nexus, &instance_id).await;

    // We'll change the interface's name and description
    let new_name = Name::try_from(String::from("new-if0")).unwrap();
    let new_description = String::from("new description");
    let updates = params::InstanceNetworkInterfaceUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some(new_name.clone()),
            description: Some(new_description.clone()),
        },
        primary: false,
        transit_ips: vec![],
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
    instance_post(client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

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
        transit_ips: vec![],
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
    instance_post(client, instance_name, InstanceOp::Start).await;
    instance_simulate(nexus, &instance_id).await;

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
    instance_post(client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    // Verify that we can set the secondary as the new primary, and that nothing
    // else changes about the NICs.
    let updates = params::InstanceNetworkInterfaceUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        primary: true,
        transit_ips: vec![],
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

#[nexus_test]
async fn test_instance_update_network_interface_transit_ips(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let instance_name = "transit-ips-test";
    let nic_name = "net0";

    create_project_and_pool(&client).await;

    // Create a stopped instance with a single network interface.
    let instance = create_instance_with(
        &client,
        PROJECT_NAME,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::Default,
        vec![],
        vec![],
        false,
        Default::default(),
    )
    .await;

    let url_interface = format!(
        "/v1/network-interfaces/{nic_name}?instance={}",
        instance.identity.id
    );

    let base_update = params::InstanceNetworkInterfaceUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: None,
        },
        primary: false,
        transit_ips: vec![
            "10.0.0.0/9".parse().unwrap(),
            "10.128.0.0/9".parse().unwrap(),
            "1.1.1.1/32".parse().unwrap(),
        ],
    };

    // Verify that a selection of transit IPs (mixture of private and global
    // unicast, no overlaps) is accepted.
    let updated_nic: InstanceNetworkInterface =
        object_put(client, &url_interface, &base_update).await;

    assert_eq!(base_update.transit_ips, updated_nic.transit_ips);

    // Non-canonical form (e.g., host identifier is nonzero) subnets should
    // be rejected.
    let with_extra_bits = params::InstanceNetworkInterfaceUpdate {
        transit_ips: vec![
            "10.0.0.0/9".parse().unwrap(),
            "10.128.0.0/9".parse().unwrap(),
            //  Invalid vvv
            "172.30.255.255/24".parse().unwrap(),
        ],
        ..base_update.clone()
    };
    let err = object_put_error(
        client,
        &url_interface,
        &with_extra_bits,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "transit IP block 172.30.255.255/24 has a non-zero host identifier",
    );

    // Multicast IP blocks should be rejected.
    let with_mc1 = params::InstanceNetworkInterfaceUpdate {
        transit_ips: vec![
            "10.0.0.0/9".parse().unwrap(),
            "10.128.0.0/9".parse().unwrap(),
            "224.0.0.0/4".parse().unwrap(),
        ],
        ..base_update.clone()
    };
    let err = object_put_error(
        client,
        &url_interface,
        &with_mc1,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "transit IP block 224.0.0.0/4 is a multicast network",
    );

    let with_mc2 = params::InstanceNetworkInterfaceUpdate {
        transit_ips: vec![
            "10.0.0.0/9".parse().unwrap(),
            "10.128.0.0/9".parse().unwrap(),
            "230.20.20.128/32".parse().unwrap(),
        ],
        ..base_update.clone()
    };
    let err = object_put_error(
        client,
        &url_interface,
        &with_mc2,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "transit IP 230.20.20.128/32 is a multicast address",
    );

    // Loopback ranges.
    let with_lo1 = params::InstanceNetworkInterfaceUpdate {
        transit_ips: vec![
            "10.0.0.0/9".parse().unwrap(),
            "10.128.0.0/9".parse().unwrap(),
            "127.42.77.0/24".parse().unwrap(),
        ],
        ..base_update.clone()
    };
    let err = object_put_error(
        client,
        &url_interface,
        &with_lo1,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "transit IP block 127.42.77.0/24 is a loopback network",
    );

    let with_lo2 = params::InstanceNetworkInterfaceUpdate {
        transit_ips: vec![
            "10.0.0.0/9".parse().unwrap(),
            "10.128.0.0/9".parse().unwrap(),
            "127.0.0.1/32".parse().unwrap(),
        ],
        ..base_update.clone()
    };
    let err = object_put_error(
        client,
        &url_interface,
        &with_lo2,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err.message, "transit IP 127.0.0.1/32 is a loopback address");

    // Overlapping IP ranges should be rejected, as should identical ranges.
    let with_dup1 = params::InstanceNetworkInterfaceUpdate {
        transit_ips: vec![
            "10.0.0.0/9".parse().unwrap(),
            "10.128.0.0/9".parse().unwrap(),
            "10.0.0.0/9".parse().unwrap(),
        ],
        ..base_update.clone()
    };
    let err = object_put_error(
        client,
        &url_interface,
        &with_dup1,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "transit IP block 10.0.0.0/9 overlaps with 10.0.0.0/9",
    );

    let with_dup2 = params::InstanceNetworkInterfaceUpdate {
        transit_ips: vec![
            "10.0.0.0/9".parse().unwrap(),
            "10.128.0.0/9".parse().unwrap(),
            "10.128.32.0/24".parse().unwrap(),
        ],
        ..base_update.clone()
    };
    let err = object_put_error(
        client,
        &url_interface,
        &with_dup2,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "transit IP block 10.128.32.0/24 overlaps with 10.128.0.0/9",
    );

    // Verify that we also catch more specific CIDRs appearing sooner in the list.
    let with_dup3 = params::InstanceNetworkInterfaceUpdate {
        transit_ips: vec![
            "10.20.20.0/30".parse().unwrap(),
            "10.0.0.0/8".parse().unwrap(),
        ],
        ..base_update.clone()
    };
    let err = object_put_error(
        client,
        &url_interface,
        &with_dup3,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "transit IP block 10.0.0.0/8 overlaps with 10.20.20.0/30",
    );

    // ...and in the end, no changes have applied.
    let final_nic: InstanceNetworkInterface =
        object_get(client, &url_interface).await;
    assert_eq!(updated_nic.transit_ips, final_nic.transit_ips);

    // As a final sanity test, we can still effectively remove spoof checking
    // using the unspecified network address.
    let allow_all = params::InstanceNetworkInterfaceUpdate {
        transit_ips: vec!["0.0.0.0/0".parse().unwrap()],
        ..base_update.clone()
    };

    let updated_nic: InstanceNetworkInterface =
        object_put(client, &url_interface, &allow_all).await;

    assert_eq!(allow_all.transit_ips, updated_nic.transit_ips);
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
        hostname: "nic-test".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: interface_params,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
    create_project_and_pool(&client).await;

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

    let disk_name = Name::try_from(String::from("probablydata")).unwrap();

    // Create the instance
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "nfs".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: Some(params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach { name: disk_name.clone() },
        )),
        cpu_platform: None,
        disks: Vec::new(),
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
    create_project_and_pool(&client).await;
    let attachable_disk =
        create_disk(&client, PROJECT_NAME, "attachable-disk").await;

    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from("nfs")).unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(3),
        hostname: "nfs".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: Some(params::InstanceDiskAttachment::Create(
            params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: Name::try_from(String::from("created-disk")).unwrap(),
                    description: String::from(
                        "A boot disk that was created by instance create",
                    ),
                },
                size: ByteCount::from_gibibytes_u32(4),
                disk_source: params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
            },
        )),
        disks: vec![
            params::InstanceDiskAttachment::Create(params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: Name::try_from(String::from("created-disk2"))
                        .unwrap(),
                    description: String::from(
                        "A data disk that was created by instance create",
                    ),
                },
                size: ByteCount::from_gibibytes_u32(4),
                disk_source: params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
            }),
            params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach {
                    name: attachable_disk.identity.name.clone(),
                },
            ),
        ],
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
    assert_eq!(disks.len(), 3);

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
    create_project_and_pool(&client).await;
    let regular_disk = create_disk(&client, PROJECT_NAME, "a-reg-disk").await;
    let faulted_disk = create_disk(&client, PROJECT_NAME, "faulted-disk").await;

    // set `faulted_disk` to the faulted state
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    assert!(
        nexus.set_disk_as_faulted(&faulted_disk.identity.id).await.unwrap()
    );

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
        hostname: "nfs".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
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
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
    create_project_and_pool(&client).await;

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
        hostname: "nfs".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: Some(params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach {
                name: Name::try_from("probablydata0".to_string()).unwrap(),
            },
        )),
        disks: (1..8)
            .map(|i| {
                params::InstanceDiskAttachment::Attach(
                    params::InstanceDiskAttach {
                        name: Name::try_from(format!("probablydata{}", i))
                            .unwrap(),
                    },
                )
            })
            .collect(),
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
        hostname: "nfs".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: Some(params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach {
                name: Name::try_from("probablydata0".to_string()).unwrap(),
            },
        )),
        disks: (1..9)
            .map(|i| {
                params::InstanceDiskAttachment::Attach(
                    params::InstanceDiskAttach {
                        name: Name::try_from(format!("probablydata{}", i))
                            .unwrap(),
                    },
                )
            })
            .collect(),
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
    create_project_and_pool(&client).await;

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
    let apictx = &cptestctx.server.server_context();
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
        hostname: "nfs".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: Some(params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach {
                name: Name::try_from("probablydata0".to_string()).unwrap(),
            },
        )),
        disks: (1..8)
            .map(|i| {
                params::InstanceDiskAttachment::Attach(
                    params::InstanceDiskAttach {
                        name: Name::try_from(format!("probablydata{}", i))
                            .unwrap(),
                    },
                )
            })
            .collect(),
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
    create_project_and_pool(&client).await;

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
        hostname: "nfs".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: Some(params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach {
                name: Name::try_from("probablydata0".to_string()).unwrap(),
            },
        )),
        disks: (1..8)
            .map(|i| {
                params::InstanceDiskAttachment::Attach(
                    params::InstanceDiskAttach {
                        name: Name::try_from(format!("probablydata{}", i))
                            .unwrap(),
                    },
                )
            })
            .collect(),
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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

    // Stash the instance's current sled agent for later disk simulation. This
    // needs to be done before the instance is stopped and dissociated from its
    // sled.
    let instance_url = format!("/v1/instances/nfs?project={}", PROJECT_NAME);
    let instance = instance_get(&client, &instance_url).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let sa = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("instance should be on a sled while it's running")
        .sled_client;

    // Stop and delete instance
    instance_post(&client, instance_name, InstanceOp::Stop).await;

    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(&client, instance_id, InstanceState::Stopped).await;

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
        hostname: "nfsv2".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: Some(params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach {
                name: Name::try_from("probablydata0".to_string()).unwrap(),
            },
        )),
        disks: (1..8)
            .map(|i| {
                params::InstanceDiskAttachment::Attach(
                    params::InstanceDiskAttach {
                        name: Name::try_from(format!("probablydata{}", i))
                            .unwrap(),
                    },
                )
            })
            .collect(),
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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

// Surprising but true: mentioning a disk multiple times for attachment is just
// fine. This means that having a disk in the boot_disk field and disks list
// will succeed as well.
//
// Test here to ensure we're not caught by surprise if this behavior is changed,
// rather than to assert that this is a specific desired behavior.
#[nexus_test]
async fn test_duplicate_disk_attach_requests_ok(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(&client).await;

    create_disk(&client, PROJECT_NAME, "probablydata").await;
    create_disk(&client, PROJECT_NAME, "alsodata").await;

    // Verify disk is there and currently detached
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 2);
    assert_eq!(disks[0].state, DiskState::Detached);
    assert_eq!(disks[1].state, DiskState::Detached);

    // Create the instance with a duplicate disks entry
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "nfs".parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "nfs".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![
            params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach {
                    name: Name::try_from(String::from("probablydata")).unwrap(),
                },
            ),
            params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach {
                    name: Name::try_from(String::from("probablydata")).unwrap(),
                },
            ),
        ],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));
    let response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("can attempt to create instance");

    let instance = response
        .parsed_body::<Instance>()
        .expect("Failed to parse error response body");
    assert_eq!(instance.boot_disk_id, None);

    // Create the instance with a disk mentioned both as a data disk and a boot
    // disk
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "nfs2".parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "nfs2".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: Some(params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach {
                name: Name::try_from(String::from("alsodata")).unwrap(),
            },
        )),
        disks: vec![params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach {
                name: Name::try_from(String::from("alsodata")).unwrap(),
            },
        )],
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));
    let response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("can attempt to create instance");

    let instance = response
        .parsed_body::<Instance>()
        .expect("Failed to parse error response body");
    let expected_disk =
        disks.iter().find(|d| d.identity.name.as_str() == "alsodata").unwrap();
    assert_eq!(instance.boot_disk_id, Some(expected_disk.identity.id));
}

// Create an instance with a boot disk, try and fail to detach it, change the
// boot disk to something else, and succeed to detach the formerly-boot
// device.
#[nexus_test]
async fn test_cannot_detach_boot_disk(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let instance_name = "nifs";

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(&client).await;

    // Create the "probablydata" disk
    create_disk(&client, PROJECT_NAME, "probablydata0").await;

    // Create the instance
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "nfs".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: Some(params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach {
                name: Name::try_from(String::from("probablydata0")).unwrap(),
            },
        )),
        cpu_platform: None,
        disks: Vec::new(),
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
        format!("/v1/instances/{}/disks", instance.identity.id);
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
    assert_eq!(instance.boot_disk_id, Some(disks[0].identity.id));

    // Attempt to detach the instance's boot disk. This should fail.
    let url_instance_detach_disk =
        format!("/v1/instances/{}/disks/detach", instance.identity.id);

    let builder = RequestBuilder::new(
        client,
        http::Method::POST,
        &url_instance_detach_disk,
    )
    .body(Some(&params::DiskPath { disk: disks[0].identity.id.into() }))
    .expect_status(Some(http::StatusCode::CONFLICT));
    let response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("can attempt to detach boot disk");

    let err = response
        .parsed_body::<HttpErrorResponseBody>()
        .expect("Failed to parse error response body");
    assert_eq!(err.message, "boot disk cannot be detached");

    // Change the instance's boot disk.
    let instance = expect_instance_reconfigure_ok(
        &client,
        &instance.identity.id,
        params::InstanceUpdate {
            boot_disk: None,
            auto_restart_policy: None,
            cpu_platform: None,
            ncpus: InstanceCpuCount::try_from(2).unwrap(),
            memory: ByteCount::from_gibibytes_u32(4),
        },
    )
    .await;
    assert_eq!(instance.boot_disk_id, None);

    // Now try to detach `disks[0]` again. This should succeed.
    let builder = RequestBuilder::new(
        client,
        http::Method::POST,
        &url_instance_detach_disk,
    )
    .body(Some(&params::DiskPath { disk: disks[0].identity.id.into() }))
    .expect_status(Some(http::StatusCode::ACCEPTED));
    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("can attempt to detach boot disk");
}

#[nexus_test]
async fn test_updating_running_instance_boot_disk_is_conflict(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let instance_name = "immediately-running";

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(&client).await;

    create_disk(&client, PROJECT_NAME, "probablydata").await;
    create_disk(&client, PROJECT_NAME, "alsodata").await;

    // Verify disk is there and currently detached
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 2);
    assert_eq!(disks[0].state, DiskState::Detached);
    assert_eq!(disks[1].state, DiskState::Detached);

    let probablydata = Name::try_from(String::from("probablydata")).unwrap();
    let alsodata = Name::try_from(String::from("alsodata")).unwrap();

    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from(instance_name)).unwrap(),
            description: String::from("instance to run and fail to update"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "inst".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![
            params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach { name: probablydata.clone() },
            ),
            params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach { name: alsodata.clone() },
            ),
        ],
        boot_disk: Some(params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach { name: probablydata.clone() },
        )),
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // The instance is technically updatable in the brief window that it is
    // `Creating`. Wait for it to leave `Creating` to make sure we're in a
    // non-updatable state before trying to update.
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    let error = expect_instance_reconfigure_err(
        &client,
        &instance_id.into_untyped_uuid(),
        params::InstanceUpdate {
            boot_disk: Some(alsodata.clone().into()),
            auto_restart_policy: None,
            cpu_platform: None,
            ncpus: InstanceCpuCount::try_from(2).unwrap(),
            memory: ByteCount::from_gibibytes_u32(4),
        },
        http::StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(error.message, "instance must be stopped to set boot disk");

    // However, we can freely change the auto-restart policy of a running
    // instance.
    expect_instance_reconfigure_ok(
        &client,
        &instance_id.into_untyped_uuid(),
        params::InstanceUpdate {
            // Leave the boot disk the same as the one with which the instance
            // was created.
            boot_disk: Some(probablydata.clone().into()),
            auto_restart_policy: Some(InstanceAutoRestartPolicy::BestEffort),
            cpu_platform: None,
            ncpus: InstanceCpuCount::try_from(2).unwrap(),
            memory: ByteCount::from_gibibytes_u32(4),
        },
    )
    .await;
}

#[nexus_test]
async fn test_updating_missing_instance_is_not_found(
    cptestctx: &ControlPlaneTestContext,
) {
    const UUID_THAT_DOESNT_EXIST: Uuid =
        Uuid::from_u128(0x12341234_4321_8765_1234_432143214321);

    let client = &cptestctx.external_client;

    let error = expect_instance_reconfigure_err(
        &client,
        &UUID_THAT_DOESNT_EXIST,
        params::InstanceUpdate {
            boot_disk: None,
            auto_restart_policy: None,
            cpu_platform: None,
            ncpus: InstanceCpuCount::try_from(0).unwrap(),
            memory: ByteCount::from_gibibytes_u32(0),
        },
        http::StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(
        error.message,
        format!("not found: instance with id \"{}\"", UUID_THAT_DOESNT_EXIST)
    );
}

async fn expect_instance_reconfigure_ok(
    external_client: &ClientTestContext,
    instance_id: &Uuid,
    update: params::InstanceUpdate,
) -> Instance {
    let url_instance_update = format!("/v1/instances/{instance_id}");

    let builder = RequestBuilder::new(
        external_client,
        http::Method::PUT,
        &url_instance_update,
    )
    .body(Some(&update))
    .expect_status(Some(http::StatusCode::OK));

    let response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("can attempt to reconfigure the instance");

    response
        .parsed_body::<Instance>()
        .expect("response should be parsed as an instance")
}

async fn expect_instance_reconfigure_err(
    external_client: &ClientTestContext,
    instance_id: &Uuid,
    update: params::InstanceUpdate,
    status: http::StatusCode,
) -> HttpErrorResponseBody {
    let url_instance_update = format!("/v1/instances/{instance_id}");

    let builder = RequestBuilder::new(
        external_client,
        http::Method::PUT,
        &url_instance_update,
    )
    .body(Some(&update))
    .expect_status(Some(status));

    let response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("can attempt to reconfigure the instance");

    response
        .parsed_body::<HttpErrorResponseBody>()
        .expect("error response should parse successfully")
}

// Test reconfiguring an instance's size in CPUs and memory.
#[nexus_test]
async fn test_size_can_be_changed(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let instance_name = "downloading-more-ram";

    create_project_and_pool(&client).await;

    let initial_ncpus = InstanceCpuCount::try_from(2).unwrap();
    let initial_memory = ByteCount::from_gibibytes_u32(4);

    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("stuff"),
        },
        ncpus: initial_ncpus,
        memory: initial_memory,
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: None,
        cpu_platform: None,
        disks: Vec::new(),
        start: true,
        // Start out with None
        auto_restart_policy: None,
        anti_affinity_groups: Vec::new(),
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
    let boot_disk_nameorid: Option<NameOrId> =
        instance.boot_disk_id.map(|x| x.into());
    let auto_restart_policy = instance.auto_restart_status.policy;

    let new_ncpus = InstanceCpuCount::try_from(4).unwrap();
    let new_memory = ByteCount::from_gibibytes_u32(8);

    // Resizing the instance immediately will error; the instance is running.
    let err = expect_instance_reconfigure_err(
        client,
        &instance.identity.id,
        params::InstanceUpdate {
            auto_restart_policy,
            boot_disk: boot_disk_nameorid.clone(),
            cpu_platform: None,
            ncpus: new_ncpus,
            memory: new_memory,
        },
        StatusCode::CONFLICT,
    )
    .await;

    assert_eq!(err.message, "instance must be stopped to change CPU or memory");

    instance_post(&client, instance_name, InstanceOp::Stop).await;
    let nexus = &cptestctx.server.server_context().nexus;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    // Now that the instance is stopped, we can resize it..
    let instance = expect_instance_reconfigure_ok(
        client,
        &instance.identity.id,
        params::InstanceUpdate {
            auto_restart_policy,
            boot_disk: boot_disk_nameorid.clone(),
            cpu_platform: None,
            ncpus: new_ncpus,
            memory: new_memory,
        },
    )
    .await;
    assert_eq!(instance.ncpus.0, new_ncpus.0);
    assert_eq!(instance.memory, new_memory);

    // No harm in reverting to the original size one field at a time though:
    let instance = expect_instance_reconfigure_ok(
        client,
        &instance.identity.id,
        params::InstanceUpdate {
            auto_restart_policy,
            boot_disk: boot_disk_nameorid.clone(),
            cpu_platform: None,
            ncpus: initial_ncpus,
            memory: new_memory,
        },
    )
    .await;
    assert_eq!(instance.ncpus.0, initial_ncpus.0);
    assert_eq!(instance.memory, new_memory);

    let instance = expect_instance_reconfigure_ok(
        client,
        &instance.identity.id,
        params::InstanceUpdate {
            auto_restart_policy,
            boot_disk: boot_disk_nameorid.clone(),
            cpu_platform: None,
            ncpus: initial_ncpus,
            memory: initial_memory,
        },
    )
    .await;
    assert_eq!(instance.ncpus.0, initial_ncpus.0);
    assert_eq!(instance.memory, initial_memory);

    // Now try a few invalid sizes. These all should fail for slightly different
    // reasons.

    // Too many CPUs.
    let err = expect_instance_reconfigure_err(
        client,
        &instance.identity.id,
        params::InstanceUpdate {
            auto_restart_policy,
            boot_disk: boot_disk_nameorid.clone(),
            cpu_platform: None,
            ncpus: InstanceCpuCount(MAX_VCPU_PER_INSTANCE + 1),
            memory: instance.memory,
        },
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        format!(
            "cannot have more than {} vCPUs per instance",
            MAX_VCPU_PER_INSTANCE
        )
    );

    // Too little memory.
    let err = expect_instance_reconfigure_err(
        client,
        &instance.identity.id,
        params::InstanceUpdate {
            auto_restart_policy,
            boot_disk: boot_disk_nameorid.clone(),
            cpu_platform: None,
            ncpus: instance.ncpus,
            memory: ByteCount::from_mebibytes_u32(0),
        },
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(err.message.contains("memory must be at least"));

    // Enough memory, but not an amount we want to accept.
    let err = expect_instance_reconfigure_err(
        client,
        &instance.identity.id,
        params::InstanceUpdate {
            auto_restart_policy,
            boot_disk: boot_disk_nameorid.clone(),
            cpu_platform: None,
            ncpus: instance.ncpus,
            memory: ByteCount::try_from(MAX_MEMORY_BYTES_PER_INSTANCE - 1)
                .unwrap(),
        },
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(err.message.contains("memory must be divisible by"));

    // Too much memory, but an otherwise-acceptable amount.
    let max_mib = MAX_MEMORY_BYTES_PER_INSTANCE / (1024 * 1024);
    let err = expect_instance_reconfigure_err(
        client,
        &instance.identity.id,
        params::InstanceUpdate {
            auto_restart_policy,
            boot_disk: boot_disk_nameorid.clone(),
            cpu_platform: None,
            ncpus: instance.ncpus,
            memory: ByteCount::from_mebibytes_u32(
                (max_mib + 1024).try_into().unwrap(),
            ),
        },
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(err.message.contains("must be less than or equal"));

    // Now delete the instance; we should not be able to resize (or really
    // interact at all!) with a deleted instance..
    expect_instance_delete_ok(client, instance_name).await;

    // Attempt to send a previously-valid update again. The only reason this
    // will not work is that we just deleted the instance we'd be updating.
    expect_instance_reconfigure_err(
        client,
        &instance.identity.id,
        params::InstanceUpdate {
            auto_restart_policy,
            boot_disk: boot_disk_nameorid.clone(),
            cpu_platform: None,
            ncpus: new_ncpus,
            memory: new_memory,
        },
        StatusCode::NOT_FOUND,
    )
    .await;
}

// Test reconfiguring an instance's auto-restart policy.
#[nexus_test]
async fn test_auto_restart_policy_can_be_changed(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let instance_name = "reincarnation-station";

    create_project_and_pool(&client).await;

    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("stuff"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: None,
        cpu_platform: None,
        disks: Vec::new(),
        start: true,
        // Start out with None
        auto_restart_policy: None,
        anti_affinity_groups: Vec::new(),
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

    // Starts out as None.
    assert_eq!(instance.auto_restart_status.policy, None);

    let assert_reconfigured = |auto_restart_policy| async move {
        let instance = expect_instance_reconfigure_ok(
            client,
            &instance.identity.id,
            dbg!(params::InstanceUpdate {
                auto_restart_policy,
                boot_disk: None,
                cpu_platform: None,
                ncpus: InstanceCpuCount::try_from(2).unwrap(),
                memory: ByteCount::from_gibibytes_u32(4),
            }),
        )
        .await;
        assert_eq!(
            dbg!(instance).auto_restart_status.policy,
            auto_restart_policy,
        );
    };

    // Reconfigure to Never.
    assert_reconfigured(Some(InstanceAutoRestartPolicy::Never)).await;

    // Reconfigure to BestEffort
    assert_reconfigured(Some(InstanceAutoRestartPolicy::BestEffort)).await;

    // Reconfigure back to None.
    assert_reconfigured(None).await;
}

// Test reconfiguring an instance's CPU platform.
#[nexus_test]
async fn test_cpu_platform_can_be_changed(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let instance_name = "milan-is-enough-for-anyone";

    create_project_and_pool(&client).await;

    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("stuff"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: None,
        // Start out with None
        cpu_platform: None,
        disks: Vec::new(),
        start: false,
        auto_restart_policy: None,
        anti_affinity_groups: Vec::new(),
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

    // Starts out as None.
    assert_eq!(instance.cpu_platform, None);

    let assert_reconfigured = |cpu_platform| async move {
        let instance = expect_instance_reconfigure_ok(
            client,
            &instance.identity.id,
            dbg!(params::InstanceUpdate {
                auto_restart_policy: None,
                boot_disk: None,
                cpu_platform,
                ncpus: InstanceCpuCount::try_from(2).unwrap(),
                memory: ByteCount::from_gibibytes_u32(4),
            }),
        )
        .await;
        assert_eq!(dbg!(instance).cpu_platform, cpu_platform,);
    };

    // Reconfigure to Milan.
    assert_reconfigured(Some(InstanceCpuPlatform::AmdMilan)).await;

    // Reconfigure to Turin (even though we have no Turin in the test env!)
    assert_reconfigured(Some(InstanceCpuPlatform::AmdTurin)).await;

    // Reconfigure back to None.
    assert_reconfigured(None).await;
}

// Create an instance with boot disk set to one of its attached disks, then set
// it to the other disk.
#[nexus_test]
async fn test_boot_disk_can_be_changed(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let instance_name = "nifs";

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(&client).await;

    // Create the "probablydata" disk
    create_disk(&client, PROJECT_NAME, "probablydata0").await;
    create_disk(&client, PROJECT_NAME, "probablydata1").await;

    // Verify disks are there and currently detached
    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;
    assert_eq!(disks.len(), 2);
    assert_eq!(disks[0].state, DiskState::Detached);
    assert_eq!(disks[1].state, DiskState::Detached);

    // Create the instance
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "nfs".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        boot_disk: Some(params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach {
                name: Name::try_from(String::from("probablydata0")).unwrap(),
            },
        )),
        disks: vec![params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach {
                name: Name::try_from(String::from("probablydata1")).unwrap(),
            },
        )],
        cpu_platform: None,
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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

    assert_eq!(instance.boot_disk_id, Some(disks[0].identity.id));

    // Change the instance's boot disk.ity.id);

    let instance = expect_instance_reconfigure_ok(
        &client,
        &instance.identity.id,
        params::InstanceUpdate {
            boot_disk: Some(disks[1].identity.id.into()),
            auto_restart_policy: None,
            cpu_platform: None,
            ncpus: InstanceCpuCount::try_from(2).unwrap(),
            memory: ByteCount::from_gibibytes_u32(4),
        },
    )
    .await;
    assert_eq!(instance.boot_disk_id, Some(disks[1].identity.id));
}

// Create an instance without a boot disk, fail to set the boot disk to a
// detached disk, then attach the disk and make it a boot disk.
#[nexus_test]
async fn test_boot_disk_must_be_attached(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let instance_name = "nifs";

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(&client).await;

    // Create the "probablydata" disk
    create_disk(&client, PROJECT_NAME, "probablydata0").await;

    let disks: Vec<Disk> =
        NexusRequest::iter_collection_authn(client, &get_disks_url(), "", None)
            .await
            .expect("failed to list disks")
            .all_items;

    // Create the instance
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "nfs".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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

    // Update the instance's boot disk to the unattached disk. This should fail.
    let error = expect_instance_reconfigure_err(
        &client,
        &instance.identity.id,
        params::InstanceUpdate {
            boot_disk: Some(disks[0].identity.id.into()),
            auto_restart_policy: None,
            cpu_platform: None,
            ncpus: InstanceCpuCount::try_from(2).unwrap(),
            memory: ByteCount::from_gibibytes_u32(4),
        },
        http::StatusCode::CONFLICT,
    )
    .await;

    assert_eq!(error.message, format!("boot disk must be attached"));

    // Now attach the disk.
    let url_instance_detach_disk =
        format!("/v1/instances/{}/disks/attach", instance.identity.id);

    let builder = RequestBuilder::new(
        client,
        http::Method::POST,
        &url_instance_detach_disk,
    )
    .body(Some(&params::DiskPath { disk: disks[0].identity.id.into() }))
    .expect_status(Some(http::StatusCode::ACCEPTED));
    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("can attempt to detach boot disk");

    // And now it can be made the boot disk.
    let instance = expect_instance_reconfigure_ok(
        &client,
        &instance.identity.id,
        params::InstanceUpdate {
            boot_disk: Some(disks[0].identity.id.into()),
            auto_restart_policy: None,
            cpu_platform: None,
            ncpus: InstanceCpuCount::try_from(2).unwrap(),
            memory: ByteCount::from_gibibytes_u32(4),
        },
    )
    .await;
    assert_eq!(instance.boot_disk_id, Some(disks[0].identity.id));
}

// Tests that an instance is rejected if the memory is less than
// MIN_MEMORY_BYTES_PER_INSTANCE
#[nexus_test]
async fn test_instances_memory_rejected_less_than_min_memory_size(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    // Attempt to create the instance, observe a server error.
    let instance_name = "just-rainsticks";
    let instance = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: format!("instance {:?}", &instance_name),
        },
        ncpus: InstanceCpuCount(1),
        memory: ByteCount::from(MIN_MEMORY_BYTES_PER_INSTANCE / 2),
        hostname: "inst".parse().unwrap(),
        user_data:
            b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                .to_vec(),
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
            ByteCount::from(MIN_MEMORY_BYTES_PER_INSTANCE)
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
    create_project_and_pool(client).await;

    // Attempt to create the instance, observe a server error.
    let instance_name = "just-rainsticks";
    let instance = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: format!("instance {:?}", &instance_name),
        },
        ncpus: InstanceCpuCount(1),
        memory: ByteCount::from(1024 * 1024 * 1024 + 300),
        hostname: "inst".parse().unwrap(),
        user_data:
            b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                .to_vec(),
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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
            ByteCount::from(MIN_MEMORY_BYTES_PER_INSTANCE)
        ),
    );
}

// Test that an instance is rejected if memory is above cap
#[nexus_test]
async fn test_instances_memory_greater_than_max_size(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    // Attempt to create the instance, observe a server error.
    let instance_name = "just-rainsticks";
    let instance = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: format!("instance {:?}", &instance_name),
        },
        ncpus: InstanceCpuCount(1),
        memory: ByteCount::try_from(MAX_MEMORY_BYTES_PER_INSTANCE + (1 << 30))
            .unwrap(),
        hostname: "inst".parse().unwrap(),
        user_data:
            b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                .to_vec(),
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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

    assert!(error.message.contains("memory must be less than"));
}

async fn create_anti_affinity_groups(
    client: &ClientTestContext,
    groups: &[&str],
) {
    for name in groups {
        let _: views::AntiAffinityGroup = object_create(
            client,
            &anti_affinity_groups_url(),
            &params::AntiAffinityGroupCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: String::from("This is a description"),
                },
                policy: AffinityPolicy::Allow,
                failure_domain: FailureDomain::Sled,
            },
        )
        .await;
    }
}

async fn ensure_anti_affinity_groups_match(
    client: &ClientTestContext,
    instance_name: &str,
    expected_groups: &[&str],
) {
    let mut expected_groups = expected_groups.to_vec();
    expected_groups.sort();

    let groups = objects_list_page_authz::<views::AntiAffinityGroup>(
        client,
        &format!(
            "/v1/instances/{instance_name}/anti-affinity-groups?{}&sort_by=name_ascending",
            get_project_selector()
        )
    )
    .await
    .items;

    let group_names =
        groups.iter().map(|g| g.identity.name.as_str()).collect::<Vec<_>>();
    assert_eq!(group_names, expected_groups);
}

#[nexus_test]
async fn test_instance_create_with_anti_affinity_groups(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let instance_name = "with-anti-affinity";

    cptestctx
        .first_sled_agent()
        .start_local_mock_propolis_server(&cptestctx.logctx.log)
        .await
        .unwrap();

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(&client).await;

    // Create anti-affinity groups
    let anti_affinity_groups = ["anti-affinity1", "anti-affinity2"];

    create_anti_affinity_groups(&client, &anti_affinity_groups).await;

    let anti_affinity_groups_param: Vec<_> = anti_affinity_groups
        .iter()
        .map(|name| NameOrId::Name(name.parse().unwrap()))
        .collect();

    // Create an instance belonging to all the groups
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        ssh_public_keys: None,
        start: false,
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: anti_affinity_groups_param,
    };

    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));
    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation!");

    // Check that the anti-affinity groups match
    ensure_anti_affinity_groups_match(
        client,
        instance_name,
        anti_affinity_groups.as_slice(),
    )
    .await;
}

#[nexus_test]
async fn test_instance_create_with_duplicate_anti_affinity_groups(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let instance_name = "with-anti-affinity";

    cptestctx
        .first_sled_agent()
        .start_local_mock_propolis_server(&cptestctx.logctx.log)
        .await
        .unwrap();

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(&client).await;

    // Create anti-affinity groups
    let anti_affinity_groups = ["anti-affinity1", "anti-affinity2"];

    create_anti_affinity_groups(&client, &anti_affinity_groups).await;

    let mut anti_affinity_groups_param: Vec<_> = anti_affinity_groups
        .iter()
        .map(|name| NameOrId::Name(name.parse().unwrap()))
        .collect();

    // Duplicate the names - this asks for each group twice.
    anti_affinity_groups_param.append(&mut anti_affinity_groups_param.clone());

    // Create an instance belonging to all the groups
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        ssh_public_keys: None,
        start: false,
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: anti_affinity_groups_param,
    };

    let builder =
        RequestBuilder::new(client, http::Method::POST, &get_instances_url())
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));
    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation!");

    // Check that the anti-affinity groups match.
    //
    // We'll only see membership in anti-affinity groups once.
    ensure_anti_affinity_groups_match(
        client,
        instance_name,
        anti_affinity_groups.as_slice(),
    )
    .await;
}

#[nexus_test]
async fn test_instance_create_with_anti_affinity_groups_that_do_not_exist(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let instance_name = "with-anti-affinity";

    cptestctx
        .first_sled_agent()
        .start_local_mock_propolis_server(&cptestctx.logctx.log)
        .await
        .unwrap();

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(&client).await;

    // Create anti-affinity groups
    let anti_affinity_groups = ["anti-affinity1", "anti-affinity2"];

    // Don't actually create these groups!
    //
    // Convert them into a format we can pass to "Instance Create".

    let anti_affinity_groups_param: Vec<_> = anti_affinity_groups
        .iter()
        .map(|name| NameOrId::Name(name.parse().unwrap()))
        .collect();

    // Create an instance belonging to all the groups
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        ssh_public_keys: None,
        start: false,
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: anti_affinity_groups_param,
    };

    let error = object_create_error(
        client,
        &get_instances_url(),
        &instance_params,
        StatusCode::NOT_FOUND,
    )
    .await;

    assert_eq!(
        error.message,
        "not found: anti-affinity-group with name \"anti-affinity1\""
    );
}

#[nexus_test]
async fn test_instance_create_with_ssh_keys(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let instance_name = "ssh-keys";

    cptestctx
        .first_sled_agent()
        .start_local_mock_propolis_server(&cptestctx.logctx.log)
        .await
        .unwrap();

    // Test pre-reqs
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(&client).await;

    // Add some SSH keys
    let key_configs = vec![
        ("key1", "an SSH public key", "ssh-test AAAAAAAA"),
        ("key2", "another SSH public key", "ssh-test BBBBBBBB"),
        ("key3", "yet another public key", "ssh-test CCCCCCCC"),
    ];
    let mut user_keys: Vec<SshKey> = Vec::new();
    for (name, description, public_key) in &key_configs {
        let new_key: SshKey = NexusRequest::objects_post(
            client,
            "/v1/me/ssh-keys",
            &SshKeyCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: description.to_string(),
                },
                public_key: public_key.to_string(),
            },
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make POST request")
        .parsed_body()
        .unwrap();
        assert_eq!(new_key.identity.name.as_str(), *name);
        assert_eq!(new_key.identity.description, *description);
        assert_eq!(new_key.public_key, *public_key);
        user_keys.push(new_key);
    }

    // Create an instance
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        // By default should transfer all profile keys
        ssh_public_keys: None,
        start: false,
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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

    let keys = objects_list_page_authz::<SshKey>(
        client,
        format!("/v1/instances/{}/ssh-public-keys", instance.identity.id)
            .as_str(),
    )
    .await
    .items;

    assert_eq!(keys[0], user_keys[0]);
    assert_eq!(keys[1], user_keys[1]);
    assert_eq!(keys[2], user_keys[2]);

    // Test creating an instance with only allow listed keys

    let instance_name = "ssh-keys-2";
    // Create an instance
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        // Should only transfer the first key
        ssh_public_keys: Some(vec![user_keys[0].identity.name.clone().into()]),
        start: false,
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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

    let keys = objects_list_page_authz::<SshKey>(
        client,
        format!("/v1/instances/{}/ssh-public-keys", instance.identity.id)
            .as_str(),
    )
    .await
    .items;

    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], user_keys[0]);

    // Test creating an instance with no keys

    let instance_name = "ssh-keys-3";
    // Create an instance
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        // Should transfer no keys
        ssh_public_keys: Some(vec![]),
        start: false,
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
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

    let keys = objects_list_page_authz::<SshKey>(
        client,
        format!("/v1/instances/{}/ssh-public-keys", instance.identity.id)
            .as_str(),
    )
    .await
    .items;

    assert_eq!(keys.len(), 0);
}

async fn expect_instance_start_fail_507(
    client: &ClientTestContext,
    instance_name: &str,
) {
    let builder = RequestBuilder::new(
        client,
        http::Method::POST,
        &get_instance_start_url(instance_name),
    )
    .expect_status(Some(http::StatusCode::INSUFFICIENT_STORAGE));

    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect(
            "Expected instance start to fail with 507 Insufficient Storage",
        );
}

async fn expect_instance_start_ok(
    client: &ClientTestContext,
    instance_name: &str,
) {
    let builder = RequestBuilder::new(
        client,
        http::Method::POST,
        &get_instance_start_url(instance_name),
    )
    .expect_status(Some(http::StatusCode::ACCEPTED));

    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance start to succeed with 202 Accepted");
}

async fn expect_instance_stop_ok(
    client: &ClientTestContext,
    instance_name: &str,
) {
    let builder = RequestBuilder::new(
        client,
        http::Method::POST,
        &get_instance_stop_url(instance_name),
    )
    .expect_status(Some(http::StatusCode::ACCEPTED));

    NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance stop to succeed with 202 Accepted");
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

#[nexus_test]
async fn test_cannot_provision_instance_beyond_cpu_capacity(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    // The third item in each tuple specifies whether instance start should
    // succeed or fail if all these configs are visited in order and started in
    // sequence. Note that for this reason the order of these elements matters.
    let configs = vec![
        ("too-many-cpus", nexus_test_utils::TEST_HARDWARE_THREADS + 1, Err(())),
        ("just-right-cpus", nexus_test_utils::TEST_HARDWARE_THREADS, Ok(())),
        (
            "insufficient-space",
            nexus_test_utils::TEST_HARDWARE_THREADS,
            Err(()),
        ),
    ];

    // Creating all the instances should succeed, even though there will never
    // be enough space to run the too-large instance.
    let mut instances = Vec::new();
    for config in &configs {
        let name = Name::try_from(config.0.to_string()).unwrap();
        let ncpus = InstanceCpuCount::try_from(i64::from(config.1)).unwrap();
        let params = params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name,
                description: String::from("probably serving data"),
            },
            ncpus,
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: config.0.parse().unwrap(),
            user_data: vec![],
            ssh_public_keys: None,
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::Default,
            external_ips: vec![],
            disks: vec![],
            boot_disk: None,
            cpu_platform: None,
            start: false,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
        };

        let url_instances = get_instances_url();
        expect_instance_creation_ok(client, &url_instances, &params).await;

        let instance = instance_get(&client, &get_instance_url(config.0)).await;
        instances.push(instance);
    }

    // Only the first properly-sized instance should be able to start.
    for config in &configs {
        match config.2 {
            Ok(_) => expect_instance_start_ok(client, config.0).await,
            Err(_) => expect_instance_start_fail_507(client, config.0).await,
        }
    }

    // Make the started instance transition to Running, shut it down, and verify
    // that the other reasonably-sized instance can now start.
    let nexus = &cptestctx.server.server_context().nexus;
    let instance_id = InstanceUuid::from_untyped_uuid(instances[1].identity.id);
    instance_simulate(nexus, &instance_id).await;
    instances[1] = instance_post(client, configs[1].0, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    expect_instance_start_ok(client, configs[2].0).await;
}

#[nexus_test]
async fn test_cannot_provision_instance_beyond_cpu_limit(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let too_many_cpus =
        InstanceCpuCount::try_from(i64::from(MAX_VCPU_PER_INSTANCE + 1))
            .unwrap();

    // Try to boot an instance that uses more CPUs than the limit
    let name1 = Name::try_from(String::from("test")).unwrap();
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: name1.clone(),
            description: String::from("probably serving data"),
        },
        ncpus: too_many_cpus,
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "test".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };
    let url_instances = get_instances_url();

    let builder =
        RequestBuilder::new(client, http::Method::POST, &url_instances)
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::BAD_REQUEST));

    let _response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation to fail with bad request!");
}

#[nexus_test]
async fn test_cannot_provision_instance_beyond_ram_capacity(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let configs = vec![
        (
            "too-much-memory",
            nexus_test_utils::TEST_RESERVOIR_RAM
                + u64::from(MIN_MEMORY_BYTES_PER_INSTANCE),
            Err(()),
        ),
        ("just-right-memory", nexus_test_utils::TEST_RESERVOIR_RAM, Ok(())),
        ("insufficient-space", nexus_test_utils::TEST_RESERVOIR_RAM, Err(())),
    ];

    // Creating all the instances should succeed, even though there will never
    // be enough space to run the too-large instance.
    let mut instances = Vec::new();
    for config in &configs {
        let name = Name::try_from(config.0.to_string()).unwrap();
        let params = params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name,
                description: String::from("probably serving data"),
            },
            ncpus: InstanceCpuCount::try_from(i64::from(1)).unwrap(),
            memory: ByteCount::try_from(config.1).unwrap(),
            hostname: config.0.parse().unwrap(),
            user_data: vec![],
            ssh_public_keys: None,
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::Default,
            external_ips: vec![],
            disks: vec![],
            boot_disk: None,
            cpu_platform: None,
            start: false,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
        };

        let url_instances = get_instances_url();
        expect_instance_creation_ok(client, &url_instances, &params).await;

        let instance = instance_get(&client, &get_instance_url(config.0)).await;
        instances.push(instance);
    }

    // Only the first properly-sized instance should be able to start.
    for config in &configs {
        match config.2 {
            Ok(_) => expect_instance_start_ok(client, config.0).await,
            Err(_) => expect_instance_start_fail_507(client, config.0).await,
        }
    }

    // Make the started instance transition to Running, shut it down, and verify
    // that the other reasonably-sized instance can now start.
    let nexus = &cptestctx.server.server_context().nexus;
    let instance_id = InstanceUuid::from_untyped_uuid(instances[1].identity.id);
    instance_simulate(nexus, &instance_id).await;
    instances[1] = instance_post(client, configs[1].0, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    expect_instance_start_ok(client, configs[2].0).await;
}

#[nexus_test]
async fn test_can_start_instance_with_cpu_platform(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let name1 = Name::try_from(String::from("test")).unwrap();
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: name1.clone(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "test".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        // Note that we're actually setting cpu_platform this time!
        cpu_platform: Some(InstanceCpuPlatform::AmdMilan),
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };
    let url_instances = get_instances_url();

    let builder =
        RequestBuilder::new(client, http::Method::POST, &url_instances)
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));

    let response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation to succeed.");

    let instance = response.parsed_body::<Instance>().unwrap();
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    // Now that the instance is created, lets try to start it.

    let nexus = &cptestctx.server.server_context().nexus;

    expect_instance_start_ok(client, instance.identity.name.as_str()).await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;

    // Great, now let's update the instance to require Turin and start it again.
    // This will fail because there is no Turin in our simulated environment
    // (yet!)
    expect_instance_stop_ok(client, instance.identity.name.as_str()).await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    let instance = expect_instance_reconfigure_ok(
        &client,
        &instance.identity.id,
        params::InstanceUpdate {
            boot_disk: None,
            auto_restart_policy: None,
            cpu_platform: Some(InstanceCpuPlatform::AmdTurin),
            ncpus: InstanceCpuCount::try_from(1).unwrap(),
            memory: ByteCount::from_gibibytes_u32(4),
        },
    )
    .await;

    expect_instance_start_fail_507(client, instance.identity.name.as_str())
        .await;

    // We'd like to see the instance actually start, so add a Turin sled and try again.

    // There should be one sled from `#[nexus_test]`, check that first.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(
        objects_list_page_authz::<Sled>(&client, &sleds_url).await.items.len(),
        1
    );

    let nexus_address =
        cptestctx.server.get_http_server_internal_address().await;

    let fake_turin_sled_id = SledUuid::new_v4();
    let turin_sled_agent_log = cptestctx
        .logctx
        .log
        .new(o!( "sled_id" => fake_turin_sled_id.to_string() ));

    let config = omicron_sled_agent::sim::Config::for_testing(
        fake_turin_sled_id,
        omicron_sled_agent::sim::SimMode::Explicit,
        Some(nexus_address),
        Some(&camino::Utf8Path::new("/an/unused/update/directory")),
        omicron_sled_agent::sim::ZpoolConfig::None,
        nexus_sled_agent_shared::inventory::SledCpuFamily::AmdTurin,
    );

    // We have to hold on to the new simulated sled-agent otherwise it will be immediately dropped
    // and shut down.
    let _agent = start_sled_agent_with_config(
        turin_sled_agent_log,
        &config,
        3,
        &cptestctx.first_sled_agent().simulated_upstairs,
    )
    .await
    .expect("can start test sled-agent");

    // Wait for Nexus to report that the new sled is present..
    poll::wait_for_condition(
        || async {
            let items = objects_list_page_authz::<Sled>(&client, &sleds_url)
                .await
                .items;

            if items.len() == 2 {
                Ok(())
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &Duration::from_secs(5),
        &Duration::from_secs(60),
    )
    .await
    .unwrap();

    // Finally, start the Turin-requiring instance for real!
    expect_instance_start_ok(client, instance.identity.name.as_str()).await;

    // The VMM should specifically be on our new fake Turin sled.
    let instance_sled = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("running instance should have a sled")
        .sled_id;

    assert_eq!(instance_sled, fake_turin_sled_id);
}

#[nexus_test]
async fn test_cannot_start_instance_with_unsatisfiable_cpu_platform(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let name1 = Name::try_from(String::from("test")).unwrap();
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: name1.clone(),
            description: String::from("probably serving data"),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "test".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        // Require Turin to start the instance, but there are no Turin sleds in
        // our fake environment. Creating this instance should succeed, but
        // starting it won't.
        cpu_platform: Some(InstanceCpuPlatform::AmdTurin),
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };
    let url_instances = get_instances_url();

    let builder =
        RequestBuilder::new(client, http::Method::POST, &url_instances)
            .body(Some(&instance_params))
            .expect_status(Some(http::StatusCode::CREATED));

    let _response = NexusRequest::new(builder)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Expected instance creation to succeed.");

    // Starting the instance, which should fail because we can't pick a sled
    // that satisfies the instance's requirements.

    expect_instance_start_fail(
        client,
        name1.as_str(),
        http::StatusCode::INSUFFICIENT_STORAGE,
    )
    .await;
}

#[nexus_test]
async fn test_instance_serial(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let instance_name = "kris-picks";

    let propolis_addr = cptestctx
        .first_sled_agent()
        .start_local_mock_propolis_server(&cptestctx.logctx.log)
        .await
        .unwrap();

    create_project_and_pool(&client).await;
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

    // Create an instance and poke it to ensure it's running.
    let instance = create_instance(client, PROJECT_NAME, instance_name).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    let instance_next = poll::wait_for_condition(
        || async {
            instance_simulate(nexus, &instance_id).await;
            let instance_next = instance_get(&client, &instance_url).await;
            if instance_next.runtime.run_state == InstanceState::Running {
                Ok(instance_next)
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &Duration::from_secs(5),
        &Duration::from_secs(60),
    )
    .await
    .unwrap();
    identity_eq(&instance.identity, &instance_next.identity);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    // Starting a simulated instance with a mock Propolis server starts the
    // mock, but it serves on localhost instead of the address that was chosen
    // by the instance start process. Forcibly update the VMM record to point to
    // the correct IP.
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let (.., db_instance) = LookupPath::new(&opctx, datastore)
        .instance_id(instance.identity.id)
        .fetch()
        .await
        .unwrap();
    let propolis_id = PropolisUuid::from_untyped_uuid(
        db_instance
            .runtime()
            .propolis_id
            .expect("running instance should have vmm"),
    );
    let updated_vmm = datastore
        .vmm_overwrite_addr_for_test(&opctx, &propolis_id, propolis_addr)
        .await
        .unwrap();
    assert_eq!(updated_vmm.propolis_ip.ip(), propolis_addr.ip());
    assert_eq!(updated_vmm.propolis_port.0, propolis_addr.port());

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
    instance_simulate(nexus, &instance_id).await;
    let instance_next =
        instance_wait_for_state(&client, instance_id, InstanceState::Stopped)
            .await;
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

    let _ = create_project(&client, PROJECT_NAME).await;

    // Create two IP pools.
    //
    // The first is given to the "default" pool, the provided to a distinct
    // explicit pool.
    let range1 = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 0, 0, 5),
        )
        .unwrap(),
    );
    let range2 = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 1, 0, 1),
            std::net::Ipv4Addr::new(10, 1, 0, 5),
        )
        .unwrap(),
    );

    // make first pool the default for the priv user's silo
    create_ip_pool(&client, "pool1", Some(range1)).await;
    link_ip_pool(&client, "pool1", &DEFAULT_SILO.id(), /*default*/ true).await;

    assert_ip_pool_utilization(client, "pool1", 0, 5, 0, 0).await;

    // second pool is associated with the silo but not default
    create_ip_pool(&client, "pool2", Some(range2)).await;
    link_ip_pool(&client, "pool2", &DEFAULT_SILO.id(), /*default*/ false).await;

    assert_ip_pool_utilization(client, "pool2", 0, 5, 0, 0).await;

    // Create an instance with pool name blank, expect IP from default pool
    create_instance_with_pool(client, "pool1-inst", None).await;

    let ip = fetch_instance_ephemeral_ip(client, "pool1-inst").await;
    assert!(
        ip.ip() >= range1.first_address() && ip.ip() <= range1.last_address(),
        "Expected ephemeral IP to come from pool1"
    );
    // 1 ephemeral + 1 snat
    assert_ip_pool_utilization(client, "pool1", 2, 5, 0, 0).await;
    // pool2 unaffected
    assert_ip_pool_utilization(client, "pool2", 0, 5, 0, 0).await;

    // Create an instance explicitly using the non-default "other-pool".
    create_instance_with_pool(client, "pool2-inst", Some("pool2")).await;
    let ip = fetch_instance_ephemeral_ip(client, "pool2-inst").await;
    assert!(
        ip.ip() >= range2.first_address() && ip.ip() <= range2.last_address(),
        "Expected ephemeral IP to come from pool2"
    );

    // SNAT comes from default pool, but count does not change because
    // SNAT IPs can be shared. https://github.com/oxidecomputer/omicron/issues/5043
    // is about getting SNAT IP from specified pool instead of default.
    assert_ip_pool_utilization(client, "pool1", 2, 5, 0, 0).await;

    // ephemeral IP comes from specified pool
    assert_ip_pool_utilization(client, "pool2", 1, 5, 0, 0).await;

    // make pool2 default and create instance with default pool. check that it now it comes from pool2
    let _: views::IpPoolSiloLink = object_put(
        client,
        &format!("/v1/system/ip-pools/pool2/silos/{}", DEFAULT_SILO.id()),
        &params::IpPoolSiloUpdate { is_default: true },
    )
    .await;

    create_instance_with_pool(client, "pool2-inst2", None).await;
    let ip = fetch_instance_ephemeral_ip(client, "pool2-inst2").await;
    assert!(
        ip.ip() >= range2.first_address() && ip.ip() <= range2.last_address(),
        "Expected ephemeral IP to come from pool2"
    );

    // pool1 unchanged
    assert_ip_pool_utilization(client, "pool1", 2, 5, 0, 0).await;
    // +1 snat (now that pool2 is default) and +1 ephemeral
    assert_ip_pool_utilization(client, "pool2", 3, 5, 0, 0).await;

    // try to delete association with pool1, but it fails because there is an
    // instance with an IP from the pool in this silo
    let pool1_silo_url =
        format!("/v1/system/ip-pools/pool1/silos/{}", DEFAULT_SILO.id());
    let error =
        object_delete_error(client, &pool1_silo_url, StatusCode::BAD_REQUEST)
            .await;
    assert_eq!(
        error.message,
        "IP addresses from this pool are in use in the linked silo"
    );

    // stop and delete instances with IPs from pool1. perhaps surprisingly, that
    // includes pool2-inst also because the SNAT IP comes from the default pool
    // even when different pool is specified for the ephemeral IP
    stop_and_delete_instance(&cptestctx, "pool1-inst").await;
    stop_and_delete_instance(&cptestctx, "pool2-inst").await;

    // pool1 is down to 0 because it had 1 snat + 1 ephemeral from pool1-inst
    // and 1 snat from pool2-inst
    assert_ip_pool_utilization(client, "pool1", 0, 5, 0, 0).await;
    // pool2 drops one because it had 1 ephemeral from pool2-inst
    assert_ip_pool_utilization(client, "pool2", 2, 5, 0, 0).await;

    // now unlink works
    object_delete(client, &pool1_silo_url).await;

    // create instance with pool1, expecting allocation to fail
    let instance_name = "pool1-inst-fail";
    let url = format!("/v1/instances?project={}", PROJECT_NAME);
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: format!("instance {:?}", instance_name),
        },
        ncpus: InstanceCpuCount(4),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "the-host".parse().unwrap(),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![params::ExternalIpCreate::Ephemeral {
            pool: Some("pool1".parse::<Name>().unwrap().into()),
        }],
        ssh_public_keys: None,
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };
    let error = object_create_error(
        client,
        &url,
        &instance_params,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(error.message, "not found: ip-pool with name \"pool1\"");
}

async fn stop_and_delete_instance(
    cptestctx: &ControlPlaneTestContext,
    instance_name: &str,
) {
    let client = &cptestctx.external_client;
    let instance =
        instance_post(&client, instance_name, InstanceOp::Stop).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;
    let url =
        format!("/v1/instances/{}?project={}", instance_name, PROJECT_NAME);
    object_delete(client, &url).await;
}

// IP pool that exists but is not associated with any silo (or with a silo other
// than the current user's) cannot be used to get IPs
#[nexus_test]
async fn test_instance_ephemeral_ip_from_orphan_pool(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _ = create_project(&client, PROJECT_NAME).await;

    // make first pool the default for the priv user's silo
    create_ip_pool(&client, "default", None).await;
    link_ip_pool(&client, "default", &DEFAULT_SILO.id(), true).await;

    let orphan_pool_range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 1, 0, 1),
            std::net::Ipv4Addr::new(10, 1, 0, 5),
        )
        .unwrap(),
    );
    create_ip_pool(&client, "orphan-pool", Some(orphan_pool_range)).await;

    let instance_name = "orphan-pool-inst";
    let body = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: format!("instance {:?}", instance_name),
        },
        ncpus: InstanceCpuCount(4),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "the-host".parse().unwrap(),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![params::ExternalIpCreate::Ephemeral {
            pool: Some("orphan-pool".parse::<Name>().unwrap().into()),
        }],
        ssh_public_keys: None,
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    // instance create 404s
    let url = format!("/v1/instances?project={}", PROJECT_NAME);
    let error =
        object_create_error(client, &url, &body, StatusCode::NOT_FOUND).await;

    assert_eq!(error.error_code.unwrap(), "ObjectNotFound".to_string());
    assert_eq!(
        error.message,
        "not found: ip-pool with name \"orphan-pool\"".to_string()
    );

    // associate the pool with a different silo and we should get the same
    // error on instance create
    let params = params::IpPoolLinkSilo {
        silo: NameOrId::Name(cptestctx.silo_name.clone()),
        is_default: false,
    };
    let _: views::IpPoolSiloLink =
        object_create(client, "/v1/system/ip-pools/orphan-pool/silos", &params)
            .await;

    let error =
        object_create_error(client, &url, &body, StatusCode::NOT_FOUND).await;

    assert_eq!(error.error_code.unwrap(), "ObjectNotFound".to_string());
    assert_eq!(
        error.message,
        "not found: ip-pool with name \"orphan-pool\"".to_string()
    );
}

// Test the error when creating an instance with an IP from the default pool,
// but there is no default pool
#[nexus_test]
async fn test_instance_ephemeral_ip_no_default_pool_error(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _ = create_project(&client, PROJECT_NAME).await;

    // important: no pool create, so there is no pool

    let body = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "no-default-pool".parse().unwrap(),
            description: "".to_string(),
        },
        ncpus: InstanceCpuCount(4),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "the-host".parse().unwrap(),
        user_data: vec![],
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![params::ExternalIpCreate::Ephemeral {
            pool: None, // <--- the only important thing here
        }],
        ssh_public_keys: None,
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let url = format!("/v1/instances?project={}", PROJECT_NAME);
    let error =
        object_create_error(client, &url, &body, StatusCode::NOT_FOUND).await;
    let msg = "not found: default IP pool for current silo".to_string();
    assert_eq!(error.message, msg);

    // same deal if you specify a pool that doesn't exist
    let body = params::InstanceCreate {
        external_ips: vec![params::ExternalIpCreate::Ephemeral {
            pool: Some("nonexistent-pool".parse::<Name>().unwrap().into()),
        }],
        ..body
    };
    let error =
        object_create_error(client, &url, &body, StatusCode::NOT_FOUND).await;
    assert_eq!(error.message, msg);
}

#[nexus_test]
async fn test_instance_attach_several_external_ips(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _ = create_project(&client, PROJECT_NAME).await;

    // Create a single (large) IP pool
    let default_pool_range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 0, 0, 10),
        )
        .unwrap(),
    );
    create_ip_pool(&client, "default", Some(default_pool_range)).await;
    link_ip_pool(&client, "default", &DEFAULT_SILO.id(), true).await;

    assert_ip_pool_utilization(client, "default", 0, 10, 0, 0).await;

    // Create several floating IPs for the instance, totalling 8 IPs.
    let mut external_ip_create =
        vec![params::ExternalIpCreate::Ephemeral { pool: None }];
    let mut fips = vec![];
    for i in 1..8 {
        let name = format!("fip-{i}");
        fips.push(
            create_floating_ip(&client, &name, PROJECT_NAME, None, None).await,
        );
        external_ip_create.push(params::ExternalIpCreate::Floating {
            floating_ip: name.parse::<Name>().unwrap().into(),
        });
    }

    // Create an instance with pool name blank, expect IP from default pool
    let instance_name = "many-fips";
    let instance = create_instance_with(
        &client,
        PROJECT_NAME,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::Default,
        vec![],
        external_ip_create,
        true,
        Default::default(),
    )
    .await;

    // 1 ephemeral + 7 floating + 1 SNAT
    assert_ip_pool_utilization(client, "default", 9, 10, 0, 0).await;

    // Verify that all external IPs are visible on the instance and have
    // been allocated in order.
    let external_ips =
        fetch_instance_external_ips(&client, instance_name, PROJECT_NAME).await;
    assert_eq!(external_ips.len(), 8);
    eprintln!("{external_ips:?}");
    for (i, eip) in external_ips
        .iter()
        .sorted_unstable_by(|a, b| a.ip().cmp(&b.ip()))
        .enumerate()
    {
        let last_octet = i + if i != external_ips.len() - 1 {
            assert_eq!(eip.kind(), IpKind::Floating);
            1
        } else {
            // SNAT will occupy 1.0.0.8 here, since it it alloc'd before
            // the ephemeral.
            assert_eq!(eip.kind(), IpKind::Ephemeral);
            2
        };
        assert_eq!(eip.ip(), Ipv4Addr::new(10, 0, 0, last_octet as u8));
    }

    // Verify that all floating IPs are bound to their parent instance.
    for fip in fips {
        let fetched_fip = floating_ip_get(
            &client,
            &get_floating_ip_by_id_url(&fip.identity.id),
        )
        .await;
        assert_eq!(fetched_fip.instance_id, Some(instance.identity.id));
    }
}

#[nexus_test]
async fn test_instance_allow_only_one_ephemeral_ip(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _ = create_project(&client, PROJECT_NAME).await;

    // don't need any IP pools because request fails at parse time

    let ephemeral_create = params::ExternalIpCreate::Ephemeral {
        pool: Some("default".parse::<Name>().unwrap().into()),
    };
    let create_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "default-pool-inst".parse().unwrap(),
            description: "instance default-pool-inst".into(),
        },
        ncpus: InstanceCpuCount(4),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "the-host".parse().unwrap(),
        user_data:
            b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                .to_vec(),
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![ephemeral_create.clone(), ephemeral_create],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };
    let error = object_create_error(
        client,
        &get_instances_url(),
        &create_params,
        StatusCode::BAD_REQUEST,
    )
    .await;

    assert_eq!(
        error.message,
        "An instance may not have more than 1 ephemeral IP address"
    );
}

async fn create_instance_with_pool(
    client: &ClientTestContext,
    instance_name: &str,
    pool_name: Option<&str>,
) -> Instance {
    create_instance_with(
        client,
        PROJECT_NAME,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::Default,
        vec![],
        vec![params::ExternalIpCreate::Ephemeral {
            pool: pool_name.map(|name| name.parse::<Name>().unwrap().into()),
        }],
        true,
        Default::default(),
    )
    .await
}

pub async fn fetch_instance_external_ips(
    client: &ClientTestContext,
    instance_name: &str,
    project_name: &str,
) -> Vec<views::ExternalIp> {
    let ips_url = format!(
        "/v1/instances/{instance_name}/external-ips?project={project_name}",
    );
    let ips = NexusRequest::object_get(client, &ips_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Failed to fetch external IPs")
        .parsed_body::<ResultsPage<views::ExternalIp>>()
        .expect("Failed to parse external IPs");
    ips.items
}

async fn fetch_instance_ephemeral_ip(
    client: &ClientTestContext,
    instance_name: &str,
) -> views::ExternalIp {
    fetch_instance_external_ips(client, instance_name, PROJECT_NAME)
        .await
        .into_iter()
        .find(|v| v.kind() == IpKind::Ephemeral)
        .unwrap()
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
        test_params::UserPassword::LoginDisallowed,
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

    // can't use create_default_ip_pool because we need to link to the silo we just made
    create_ip_pool(&client, "default", None).await;
    link_ip_pool(&client, "default", &silo.identity.id, true).await;

    assert_ip_pool_utilization(client, "default", 0, 65536, 0, 0).await;

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
    let instance_name = "collaborate-with-me";
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from(String::from(instance_name)).unwrap(),
            description: String::from("instance to test creation in a silo"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "inst".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![params::ExternalIpCreate::Ephemeral {
            pool: Some("default".parse::<Name>().unwrap().into()),
        }],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };
    let url_instances = format!("/v1/instances?project={}", PROJECT_NAME);
    NexusRequest::objects_post(client, &url_instances, &instance_params)
        .authn_as(AuthnMode::SiloUser(user_id))
        .execute()
        .await
        .expect("Failed to create instance")
        .parsed_body::<Instance>()
        .expect("Failed to parse instance");

    // Make sure the instance can actually start even though a collaborator
    // created it.
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let authn = AuthnMode::SiloUser(user_id);
    let instance_url = get_instance_url(instance_name);
    let instance = instance_get_as(&client, &instance_url, authn.clone()).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    info!(&cptestctx.logctx.log, "test got instance"; "instance" => ?instance);
    assert_eq!(instance.runtime.run_state, InstanceState::Starting);

    // The default instance simulation function uses a test user that, while
    // privileged, only has access to the default silo. Synthesize an operation
    // context that grants access to the silo under test.
    let opctx = OpContext::for_background(
        cptestctx.logctx.log.new(o!()),
        Arc::new(nexus_db_queries::authz::Authz::new(&cptestctx.logctx.log)),
        nexus_db_queries::authn::Context::for_test_user(
            user_id,
            silo.identity.id,
            nexus_db_queries::authn::SiloAuthnPolicy::try_from(&*DEFAULT_SILO)
                .unwrap(),
        ),
        nexus.datastore().clone(),
    );
    instance_simulate_with_opctx(nexus, &instance_id, &opctx).await;
    let instance = instance_get_as(&client, &instance_url, authn).await;
    assert_eq!(instance.runtime.run_state, InstanceState::Running);

    // Stop the instance
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &get_instance_stop_url(instance.identity.name.as_str()),
        )
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::SiloUser(user_id))
    .execute()
    .await
    .expect("Failed to stop the instance");

    instance_simulate_with_opctx(nexus, &instance_id, &opctx).await;
    instance_wait_for_state_as(
        client,
        AuthnMode::SiloUser(user_id),
        instance_id,
        InstanceState::Stopped,
    )
    .await;

    // Delete the instance
    NexusRequest::object_delete(client, &instance_url)
        .authn_as(AuthnMode::SiloUser(user_id))
        .execute()
        .await
        .expect("Failed to delete the instance");
}

/// Test that appropriate OPTE V2P mappings are created and deleted.
#[nexus_test(extra_sled_agents = 3)]
async fn test_instance_v2p_mappings(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    create_project_and_pool(client).await;

    // This test requires some additional sleds that can receive V2P mappings.
    let additional_sleds: Vec<_> = cptestctx.extra_sled_agents().collect();

    // Create an instance.
    let instance_name = "test-instance";
    let instance = create_instance(client, PROJECT_NAME, instance_name).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

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
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., authz_instance) = LookupPath::new(&opctx, datastore)
        .instance_id(instance.identity.id)
        .lookup_for(nexus_db_queries::authz::Action::Read)
        .await
        .unwrap();

    let guest_nics = datastore
        .derive_guest_network_interface_info(&opctx, &authz_instance)
        .await
        .unwrap();

    assert_eq!(guest_nics.len(), 1);

    let mut sled_agents: Vec<&Arc<SledAgent>> =
        additional_sleds.iter().map(|x| &x.sled_agent).collect();
    sled_agents.push(&cptestctx.first_sled_agent());

    for sled_agent in &sled_agents {
        assert_sled_v2p_mappings(sled_agent, &nics[0], guest_nics[0].vni).await;
    }

    // Delete the instance
    instance_simulate(nexus, &instance_id).await;
    instance_post(&client, instance_name, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    let instance_url = get_instance_url(instance_name);
    NexusRequest::object_delete(client, &instance_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // Validate that every sled no longer has the V2P mapping for this instance
    for sled_agent in &sled_agents {
        let condition = || async {
            let v2p_mappings = sled_agent.v2p_mappings.lock().unwrap();
            if v2p_mappings.is_empty() {
                Ok(())
            } else {
                Err(CondCheckError::NotYet::<()>)
            }
        };
        wait_for_condition(
            condition,
            &Duration::from_secs(1),
            &Duration::from_secs(30),
        )
        .await
        .expect("v2p mappings should be empty");
    }
}

async fn instance_get(
    client: &ClientTestContext,
    instance_url: &str,
) -> Instance {
    instance_get_as(client, instance_url, AuthnMode::PrivilegedUser).await
}

async fn instance_get_as(
    client: &ClientTestContext,
    instance_url: &str,
    authn_as: AuthnMode,
) -> Instance {
    NexusRequest::object_get(client, instance_url)
        .authn_as(authn_as)
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

pub async fn instance_wait_for_state(
    client: &ClientTestContext,
    instance_id: InstanceUuid,
    state: omicron_common::api::external::InstanceState,
) -> Instance {
    instance_wait_for_state_as(
        client,
        AuthnMode::PrivilegedUser,
        instance_id,
        state,
    )
    .await
}

/// Line [`instance_wait_for_state`], but with an [`AuthnMode`] parameter for
/// the instance lookup requests.
pub async fn instance_wait_for_state_as(
    client: &ClientTestContext,
    authn_as: AuthnMode,
    instance_id: InstanceUuid,
    state: omicron_common::api::external::InstanceState,
) -> Instance {
    const MAX_WAIT: Duration = Duration::from_secs(120);

    slog::info!(
        &client.client_log,
        "waiting for instance {instance_id} to transition to {state}...";
    );
    let url = format!("/v1/instances/{instance_id}");
    let result = wait_for_condition(
        || async {
            let instance: Instance = NexusRequest::object_get(client, &url)
                .authn_as(authn_as.clone())
                .execute()
                .await?
                .parsed_body()?;
            if instance.runtime.run_state == state {
                Ok(instance)
            } else {
                slog::info!(
                    &client.client_log,
                    "instance {instance_id} has not transitioned to {state}";
                    "instance_id" => %instance.identity.id,
                    "instance_runtime_state" => ?instance.runtime,
                );
                Err(CondCheckError::<anyhow::Error>::NotYet)
            }
        },
        &Duration::from_secs(1),
        &MAX_WAIT,
    )
    .await;
    match result {
        Ok(instance) => {
            slog::info!(
                &client.client_log,
                "instance {instance_id} has transitioned to {state}"
            );
            instance
        }
        Err(e) => panic!(
            "instance {instance_id} did not transition to {state:?} \
             after {MAX_WAIT:?}: {e}"
        ),
    }
}

/// Waits for an instance's VMM to be registered with the simulated sled-agent.
///
/// This is necessary when *restarting* an instance, as the instance's external
/// API state will be `Starting` as soon as a VMM record is created in the
/// database, even if that VMM's internal state is `VmmState::Creating` (which
/// means it has not yet been registered with the sled-agent). If we attempt to
/// simulate an instance before the simulated sled-agent has registered its VMM,
/// the simulated sled-agent will panic.
pub async fn instance_wait_for_vmm_registration(
    cptestctx: &ControlPlaneTestContext,
    instance_id: &InstanceUuid,
) {
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let log = &cptestctx.logctx.log;
    let opctx = OpContext::for_tests(log.clone(), datastore.clone());
    let (.., authz_instance) = LookupPath::new(&opctx, datastore)
        .instance_id(instance_id.into_untyped_uuid())
        .lookup_for(nexus_db_queries::authz::Action::Read)
        .await
        .expect("instance must exist to wait for its VMM to be registered");

    info!(
        log,
        "waiting for instance's VMM to be registered with sled-agent...";
        "instance_id" => %instance_id,
    );

    let result = wait_for_condition(
        || async {
            debug!(
                log,
                "checking if instance's active VMM has been registered with sled-agent";
                "instance_id" => %instance_id,
            );
            let gestalt = datastore
                .instance_fetch_all(&opctx, &authz_instance)
                .await
                .map_err(poll::CondCheckError::Failed)?;
            let vmm = match gestalt.active_vmm {
                Some(v) => v,
                None => {
                    warn!(
                        log,
                        "instance does not have an active VMM, hopefully \
                            it will soon...";
                        "instance_id" => %instance_id,
                    );
                    return Err(CondCheckError::<Error>::NotYet);
                }
            };

            if vmm.runtime.state == nexus_db_model::VmmState::Creating {
                debug!(
                    log,
                    "instance's active VMM is still Creating";
                    "instance_id" => %instance_id,
                );
                Err(poll::CondCheckError::<Error>::NotYet)
            } else {
                info!(
                    log,
                    "instance's active VMM is no longer Creating";
                    "instance_id" => %instance_id,
                    "vmm_id" => %vmm.id,
                    "vmm_state" => ?vmm.runtime.state,
                );
                Ok(())
            }
        },
        &Duration::from_secs(1),
        &Duration::from_secs(60),
    )
    .await;

    if let Err(err) = result {
        panic!("instance {instance_id}'s VMM was not registered: {err:?}")
    }
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

/// Asserts that supplied sled agent's most recent V2P mapping data for the
/// supplied guest network interface has properties that match the properties
/// stored in the interface itself.
async fn assert_sled_v2p_mappings(
    sled_agent: &Arc<SledAgent>,
    nic: &InstanceNetworkInterface,
    vni: Vni,
) {
    let condition = || async {
        let v2p_mappings = sled_agent.v2p_mappings.lock().unwrap();
        let mapping = v2p_mappings.iter().find(|mapping| {
            mapping.virtual_ip == nic.ip
                && mapping.virtual_mac == nic.mac
                && mapping.physical_host_ip == sled_agent.ip
                && mapping.vni == vni
        });

        if mapping.is_some() {
            Ok(())
        } else {
            Err(CondCheckError::NotYet::<()>)
        }
    };
    wait_for_condition(
        condition,
        &Duration::from_secs(1),
        &Duration::from_secs(30),
    )
    .await
    .expect("matching v2p mapping should be present");
}

/// Asserts that supplied sled agent's most recent VPC route sets
/// contain up-to-date routes for a known subnet.
pub async fn assert_sled_vpc_routes(
    sled_agent: &Arc<SledAgent>,
    opctx: &OpContext,
    datastore: &DataStore,
    subnet_id: Uuid,
    vni: Vni,
) -> (HashSet<ResolvedVpcRoute>, HashSet<ResolvedVpcRoute>) {
    let (.., authz_vpc, _, db_subnet) = LookupPath::new(opctx, datastore)
        .vpc_subnet_id(subnet_id)
        .fetch()
        .await
        .unwrap();

    let custom_routes: HashSet<_> =
        if let Some(router_id) = db_subnet.custom_router_id {
            datastore
                .vpc_resolve_router_rules(opctx, router_id)
                .await
                .unwrap()
                .into_iter()
                .collect()
        } else {
            Default::default()
        };

    let (.., vpc) = LookupPath::new(opctx, datastore)
        .vpc_id(authz_vpc.id())
        .fetch()
        .await
        .unwrap();

    let system_routes: HashSet<_> = datastore
        .vpc_resolve_router_rules(opctx, vpc.system_router_id)
        .await
        .unwrap()
        .into_iter()
        .collect();

    assert!(!system_routes.is_empty());

    let condition = || async {
        let sys_key = RouterId { vni, kind: RouterKind::System };
        let custom_key = RouterId {
            vni,
            kind: RouterKind::Custom(db_subnet.ipv4_block.0.into()),
        };

        let vpc_routes = sled_agent.vpc_routes.lock().unwrap();
        let sys_routes_found = vpc_routes
            .iter()
            .any(|(id, set)| *id == sys_key && set.routes == system_routes);
        let custom_routes_found = vpc_routes
            .iter()
            .any(|(id, set)| *id == custom_key && set.routes == custom_routes);

        if sys_routes_found && custom_routes_found {
            Ok(())
        } else {
            let found_system =
                vpc_routes.get(&sys_key).cloned().unwrap_or_default();
            let found_custom =
                vpc_routes.get(&custom_key).cloned().unwrap_or_default();

            println!("unexpected route setup");
            println!("vni: {vni:?}");
            println!("sled: {}", sled_agent.id);
            println!("subnet: {}", db_subnet.ipv4_block.0);
            println!("expected system: {system_routes:?}");
            println!("expected custom {custom_routes:?}");
            println!("found: {vpc_routes:?}");
            println!(
                "\n-----\nsystem diff (-): {:?}",
                system_routes.difference(&found_system.routes)
            );
            println!(
                "system diff (+): {:?}",
                found_system.routes.difference(&system_routes)
            );
            println!(
                "custom diff (-): {:?}",
                custom_routes.difference(&found_custom.routes)
            );
            println!(
                "custom diff (+): {:?}\n-----",
                found_custom.routes.difference(&custom_routes)
            );
            Err(CondCheckError::NotYet::<()>)
        }
    };
    wait_for_condition(
        condition,
        &Duration::from_secs(1),
        &Duration::from_secs(60),
    )
    .await
    .expect("matching vpc routes should be present");

    println!("success! VPC routes as expected for sled {}", sled_agent.id);

    (system_routes, custom_routes)
}

/// Simulate completion of an ongoing instance state transition.  To do this, we
/// have to look up the instance, then get the sled agent associated with that
/// instance, and then tell it to finish simulating whatever async transition is
/// going on.
pub async fn instance_simulate(nexus: &Arc<Nexus>, id: &InstanceUuid) {
    let sled_info = nexus
        .active_instance_info(id, None)
        .await
        .unwrap()
        .expect("instance must be on a sled to simulate a state change");

    sled_info.sled_client.vmm_finish_transition(sled_info.propolis_id).await;
}

/// Simulate one step of an ongoing instance state transition.  To do this, we
/// have to look up the instance, then get the sled agent associated with that
/// instance, and then tell it to finish simulating whatever async transition is
/// going on.
async fn vmm_single_step_on_sled(
    cptestctx: &ControlPlaneTestContext,
    nexus: &Arc<Nexus>,
    sled_id: SledUuid,
    propolis_id: PropolisUuid,
) {
    info!(&cptestctx.logctx.log, "Single-stepping simulated instance on sled";
          "propolis_id" => %propolis_id, "sled_id" => %sled_id);
    let sa = nexus.sled_client(&sled_id).await.unwrap();
    sa.vmm_single_step(propolis_id).await;
}

pub async fn instance_simulate_with_opctx(
    nexus: &Arc<Nexus>,
    id: &InstanceUuid,
    opctx: &OpContext,
) {
    let sled_info = nexus
        .active_instance_info(id, Some(opctx))
        .await
        .unwrap()
        .expect("instance must be on a sled to simulate a state change");

    sled_info.sled_client.vmm_finish_transition(sled_info.propolis_id).await;
}

/// Wait for an instance to complete a simulated state transition, repeatedly
/// poking the simulated sled-agent until the transition occurs.
///
/// This can be used to avoid races between Nexus processes (like sagas) which
/// trigger a state transition but cannot be easily awaited by the test, and the
/// actual request to simulate the state transition. However, it should be used
/// cautiously to avoid simulating multiple state transitions accidentally.
async fn instance_wait_for_simulated_transition(
    cptestctx: &ControlPlaneTestContext,
    id: &InstanceUuid,
    state: InstanceState,
) -> Instance {
    const MAX_WAIT: Duration = Duration::from_secs(120);
    let client = &cptestctx.external_client;
    slog::info!(
        &client.client_log,
        "waiting for instance {id} transition to {state} \
         (and poking simulated sled-agent)...";
    );
    let url = format!("/v1/instances/{id}");
    let result = wait_for_condition(
        || async {
            let instance: Instance = NexusRequest::object_get(&client, &url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await?
                .parsed_body()?;
            if instance.runtime.run_state == state {
                Ok(instance)
            } else {
                slog::info!(
                    &client.client_log,
                    "instance {id} has not transitioned to {state}, \
                     poking sled-agent";
                    "instance_id" => %instance.identity.id,
                    "instance_runtime_state" => ?instance.runtime,
                );
                instance_simulate(&cptestctx.server.server_context().nexus, id)
                    .await;
                Err(CondCheckError::<anyhow::Error>::NotYet)
            }
        },
        &Duration::from_secs(1),
        &MAX_WAIT,
    )
    .await;
    match result {
        Ok(instance) => {
            slog::info!(
                &client.client_log,
                "instance {id} has transitioned to {state}"
            );
            instance
        }
        Err(e) => panic!(
            "instance {id} did not transition to {state:?} \
             after {MAX_WAIT:?}: {e}"
        ),
    }
}

/// Simulates state transitions for the incarnation of the instance on the
/// supplied sled (which may not be the sled ID currently stored in the
/// instance's CRDB record).
async fn vmm_simulate_on_sled(
    cptestctx: &ControlPlaneTestContext,
    nexus: &Arc<Nexus>,
    sled_id: SledUuid,
    propolis_id: PropolisUuid,
) {
    info!(&cptestctx.logctx.log, "Poking simulated instance on sled";
          "propolis_id" => %propolis_id, "sled_id" => %sled_id);
    let sa = nexus.sled_client(&sled_id).await.unwrap();
    sa.vmm_finish_transition(propolis_id).await;
}

/// Simulates a migration source for the provided instance ID, sled ID, and
/// migration ID.
async fn instance_simulate_migration_source(
    cptestctx: &ControlPlaneTestContext,
    nexus: &Arc<Nexus>,
    sled_id: SledUuid,
    propolis_id: PropolisUuid,
    migration_id: Uuid,
) {
    info!(
        &cptestctx.logctx.log,
        "Simulating migration source sled";
        "propolis_id" => %propolis_id,
        "sled_id" => %sled_id,
        "migration_id" => %migration_id,
    );
    let sa = nexus.sled_client(&sled_id).await.unwrap();
    sa.vmm_simulate_migration_source(
        propolis_id,
        sled_agent_client::SimulateMigrationSource {
            migration_id,
            result: sled_agent_client::SimulatedMigrationResult::Success,
        },
    )
    .await;
}
