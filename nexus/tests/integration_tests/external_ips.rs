// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests Floating IP support in the API

use std::net::IpAddr;
use std::net::Ipv4Addr;

use crate::integration_tests::instances::fetch_instance_external_ips;
use crate::integration_tests::instances::instance_simulate;
use crate::integration_tests::instances::instance_wait_for_state;
use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
use http::Method;
use http::StatusCode;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::assert_ip_pool_utilization;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_floating_ip;
use nexus_test_utils::resource_helpers::create_instance_with;
use nexus_test_utils::resource_helpers::create_ip_pool;
use nexus_test_utils::resource_helpers::create_local_user;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::create_silo;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::link_ip_pool;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::object_create_error;
use nexus_test_utils::resource_helpers::object_delete;
use nexus_test_utils::resource_helpers::object_delete_error;
use nexus_test_utils::resource_helpers::object_get;
use nexus_test_utils::resource_helpers::object_put;
use nexus_test_utils::resource_helpers::test_params;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::SiloRole;
use nexus_types::external_api::views;
use nexus_types::external_api::views::FloatingIp;
use nexus_types::identity::Resource;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv4Range;
use omicron_common::address::NUM_SOURCE_NAT_PORTS;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use oxide_client::types::ExternalIpResultsPage;
use oxide_client::types::IpPoolRangeResultsPage;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const PROJECT_NAME: &str = "rootbeer-float";

const FIP_NAMES: &[&str] =
    &["vanilla", "chocolate", "strawberry", "pistachio", "caramel"];

const INSTANCE_NAMES: &[&str] = &["anonymous-diner", "anonymous-restaurant"];

pub fn get_floating_ips_url(project_name: &str) -> String {
    format!("/v1/floating-ips?project={project_name}")
}

pub fn instance_ephemeral_ip_url(
    instance_name: &str,
    project_name: &str,
) -> String {
    format!(
        "/v1/instances/{instance_name}/external-ips/ephemeral?project={project_name}"
    )
}

pub fn attach_floating_ip_url(
    floating_ip_name: &str,
    project_name: &str,
) -> String {
    format!("/v1/floating-ips/{floating_ip_name}/attach?project={project_name}")
}

pub fn attach_floating_ip_uuid(floating_ip_uuid: &Uuid) -> String {
    format!("/v1/floating-ips/{floating_ip_uuid}/attach")
}

pub fn detach_floating_ip_url(
    floating_ip_name: &str,
    project_name: &str,
) -> String {
    format!("/v1/floating-ips/{floating_ip_name}/detach?project={project_name}")
}

pub fn get_floating_ip_by_name_url(
    fip_name: &str,
    project_name: &str,
) -> String {
    format!("/v1/floating-ips/{fip_name}?project={project_name}")
}

pub fn get_floating_ip_by_id_url(fip_id: &Uuid) -> String {
    format!("/v1/floating-ips/{fip_id}")
}

#[nexus_test]
async fn test_floating_ip_access(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    // Create a floating IP from the default pool.
    let fip_name = FIP_NAMES[0];
    let fip = create_floating_ip(
        client,
        fip_name,
        &project.identity.id.to_string(),
        None,
        None,
    )
    .await;

    // Fetch floating IP by ID
    let fetched_fip =
        floating_ip_get(&client, &get_floating_ip_by_id_url(&fip.identity.id))
            .await;
    assert_eq!(fetched_fip.identity.id, fip.identity.id);

    // Fetch floating IP by name and project_id
    let fetched_fip = floating_ip_get(
        &client,
        &get_floating_ip_by_name_url(
            fip.identity.name.as_str(),
            &project.identity.id.to_string(),
        ),
    )
    .await;
    assert_eq!(fetched_fip.identity.id, fip.identity.id);

    // Fetch floating IP by name and project_name
    let fetched_fip = floating_ip_get(
        &client,
        &get_floating_ip_by_name_url(
            fip.identity.name.as_str(),
            project.identity.name.as_str(),
        ),
    )
    .await;
    assert_eq!(fetched_fip.identity.id, fip.identity.id);
}

#[nexus_test]
async fn test_floating_ip_create(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // automatically linked to current silo
    let default_pool = create_default_ip_pool(&client).await;

    const CAPACITY: f64 = 65536.0;
    assert_ip_pool_utilization(client, "default", 0, CAPACITY).await;

    let ipv4_range =
        Ipv4Range::new(Ipv4Addr::new(10, 1, 0, 1), Ipv4Addr::new(10, 1, 0, 5))
            .unwrap();
    let other_capacity = ipv4_range.len().into();
    let other_pool_range = IpRange::V4(ipv4_range);
    // not automatically linked to currently silo. see below
    let (other_pool, ..) =
        create_ip_pool(&client, "other-pool", Some(other_pool_range)).await;

    assert_ip_pool_utilization(client, "other-pool", 0, other_capacity).await;

    let project = create_project(client, PROJECT_NAME).await;

    // Create with no chosen IP and fallback to default pool.
    let fip_name = FIP_NAMES[0];
    let fip = create_floating_ip(
        client,
        fip_name,
        project.identity.name.as_str(),
        None,
        None,
    )
    .await;
    assert_eq!(fip.identity.name.as_str(), fip_name);
    assert_eq!(fip.project_id, project.identity.id);
    assert_eq!(fip.instance_id, None);
    assert_eq!(fip.ip, IpAddr::from(Ipv4Addr::new(10, 0, 0, 0)));
    assert_eq!(fip.ip_pool_id, default_pool.identity.id);

    assert_ip_pool_utilization(client, "default", 1, CAPACITY).await;

    // Create with chosen IP and fallback to default pool.
    let fip_name = FIP_NAMES[1];
    let ip_addr = "10.0.12.34".parse().unwrap();
    let fip = create_floating_ip(
        client,
        fip_name,
        project.identity.name.as_str(),
        Some(ip_addr),
        None,
    )
    .await;
    assert_eq!(fip.identity.name.as_str(), fip_name);
    assert_eq!(fip.project_id, project.identity.id);
    assert_eq!(fip.instance_id, None);
    assert_eq!(fip.ip, ip_addr);
    assert_eq!(fip.ip_pool_id, default_pool.identity.id);

    assert_ip_pool_utilization(client, "default", 2, CAPACITY).await;

    // Creating with other-pool fails with 404 until it is linked to the current silo
    let fip_name = FIP_NAMES[2];
    let params = params::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: fip_name.parse().unwrap(),
            description: String::from("a floating ip"),
        },
        ip: None,
        pool: Some(NameOrId::Name("other-pool".parse().unwrap())),
    };
    let url = format!("/v1/floating-ips?project={}", project.identity.name);
    let error =
        object_create_error(client, &url, &params, StatusCode::NOT_FOUND).await;
    assert_eq!(error.message, "not found: ip-pool with name \"other-pool\"");

    assert_ip_pool_utilization(client, "other-pool", 0, other_capacity).await;

    // now link the pool and everything should work with the exact same params
    let silo_id = DEFAULT_SILO.id();
    link_ip_pool(&client, "other-pool", &silo_id, false).await;

    // Create with no chosen IP from named pool.
    let fip: FloatingIp = object_create(client, &url, &params).await;
    assert_eq!(fip.identity.name.as_str(), fip_name);
    assert_eq!(fip.project_id, project.identity.id);
    assert_eq!(fip.instance_id, None);
    assert_eq!(fip.ip, IpAddr::from(Ipv4Addr::new(10, 1, 0, 1)));
    assert_eq!(fip.ip_pool_id, other_pool.identity.id);

    assert_ip_pool_utilization(client, "other-pool", 1, other_capacity).await;

    // Create with chosen IP from non-default pool.
    let fip_name = FIP_NAMES[3];
    let ip_addr = "10.1.0.5".parse().unwrap();
    let fip = create_floating_ip(
        client,
        fip_name,
        project.identity.name.as_str(),
        Some(ip_addr),
        Some("other-pool"),
    )
    .await;
    assert_eq!(fip.identity.name.as_str(), fip_name);
    assert_eq!(fip.project_id, project.identity.id);
    assert_eq!(fip.instance_id, None);
    assert_eq!(fip.ip, ip_addr);
    assert_eq!(fip.ip_pool_id, other_pool.identity.id);

    assert_ip_pool_utilization(client, "other-pool", 2, other_capacity).await;
}

#[nexus_test]
async fn test_floating_ip_create_non_admin(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let silo_url = format!("/v1/system/silos/{}", cptestctx.silo_name);
    let silo: views::Silo = object_get(client, &silo_url).await;

    // manually create default pool and link to test silo, as opposed to default
    // silo, which is what the helper would do
    let _ = create_ip_pool(&client, "default", None).await;
    link_ip_pool(&client, "default", &silo.identity.id, true).await;

    // create other pool and link to silo
    let other_pool_range = IpRange::V4(
        Ipv4Range::new(Ipv4Addr::new(10, 1, 0, 1), Ipv4Addr::new(10, 1, 0, 5))
            .unwrap(),
    );
    create_ip_pool(&client, "other-pool", Some(other_pool_range)).await;
    link_ip_pool(&client, "other-pool", &silo.identity.id, false).await;

    // create third pool and don't link to silo
    let unlinked_pool_range = IpRange::V4(
        Ipv4Range::new(Ipv4Addr::new(10, 2, 0, 1), Ipv4Addr::new(10, 2, 0, 5))
            .unwrap(),
    );
    create_ip_pool(&client, "unlinked-pool", Some(unlinked_pool_range)).await;

    // Create a silo user
    let user = create_local_user(
        client,
        &silo,
        &"user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    // Make silo collaborator
    grant_iam(
        client,
        &silo_url,
        SiloRole::Collaborator,
        user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // create project as user (i.e., in their silo)
    NexusRequest::objects_post(
        client,
        "/v1/projects",
        &params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: PROJECT_NAME.parse().unwrap(),
                description: "floating ip project".to_string(),
            },
        },
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .expect("Failed to create project");

    let create_url = get_floating_ips_url(PROJECT_NAME);

    // create a floating IP as this user, first with default pool
    let body = params::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: "root-beer".parse().unwrap(),
            description: String::from("a floating ip"),
        },
        pool: None,
        ip: None,
    };
    let fip: views::FloatingIp =
        NexusRequest::objects_post(client, &create_url, &body)
            .authn_as(AuthnMode::SiloUser(user.id))
            .execute_and_parse_unwrap()
            .await;
    assert_eq!(fip.identity.name.to_string(), "root-beer");

    // now with other pool linked to my silo
    let body = params::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: "another-soda".parse().unwrap(),
            description: String::from("a floating ip"),
        },
        pool: Some(NameOrId::Name("other-pool".parse().unwrap())),
        ip: None,
    };
    let fip: views::FloatingIp =
        NexusRequest::objects_post(client, &create_url, &body)
            .authn_as(AuthnMode::SiloUser(user.id))
            .execute_and_parse_unwrap()
            .await;
    assert_eq!(fip.identity.name.to_string(), "another-soda");

    // now with pool not linked to my silo (fails with 404)
    let body = params::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: "secret-third-soda".parse().unwrap(),
            description: String::from("a floating ip"),
        },
        pool: Some(NameOrId::Name("unlinked-pool".parse().unwrap())),
        ip: None,
    };
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &create_url)
            .body(Some(&body))
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .unwrap()
    .parsed_body::<HttpErrorResponseBody>()
    .unwrap();

    assert_eq!(error.message, "not found: ip-pool with name \"unlinked-pool\"");
}

#[nexus_test]
async fn test_floating_ip_create_fails_in_other_silo_pool(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let project = create_project(client, PROJECT_NAME).await;

    // Create other silo and pool linked to that silo
    let other_silo = create_silo(
        &client,
        "not-my-silo",
        true,
        shared::SiloIdentityMode::SamlJit,
    )
    .await;
    let other_pool_range = IpRange::V4(
        Ipv4Range::new(Ipv4Addr::new(10, 2, 0, 1), Ipv4Addr::new(10, 2, 0, 5))
            .unwrap(),
    );
    create_ip_pool(&client, "external-silo-pool", Some(other_pool_range)).await;
    // don't link pool to silo yet

    let fip_name = FIP_NAMES[4];

    // creating a floating IP should fail with a 404 as if the specified pool
    // does not exist
    let url =
        format!("/v1/floating-ips?project={}", project.identity.name.as_str());
    let body = params::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: fip_name.parse().unwrap(),
            description: String::from("a floating ip"),
        },
        ip: None,
        pool: Some(NameOrId::Name("external-silo-pool".parse().unwrap())),
    };

    let error =
        object_create_error(client, &url, &body, StatusCode::NOT_FOUND).await;
    assert_eq!(
        error.message,
        "not found: ip-pool with name \"external-silo-pool\""
    );

    // error is the same after linking the pool to the other silo
    link_ip_pool(&client, "external-silo-pool", &other_silo.identity.id, false)
        .await;

    let error =
        object_create_error(client, &url, &body, StatusCode::NOT_FOUND).await;
    assert_eq!(
        error.message,
        "not found: ip-pool with name \"external-silo-pool\""
    );
}

#[nexus_test]
async fn test_floating_ip_create_ip_in_use(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;

    let project = create_project(client, PROJECT_NAME).await;
    let contested_ip = "10.0.0.0".parse().unwrap();

    // First create will succeed.
    create_floating_ip(
        client,
        FIP_NAMES[0],
        project.identity.name.as_str(),
        Some(contested_ip),
        None,
    )
    .await;

    // Second will fail as the requested IP is in use in the selected
    // (default) pool.
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &get_floating_ips_url(PROJECT_NAME),
        )
        .body(Some(&params::FloatingIpCreate {
            identity: IdentityMetadataCreateParams {
                name: FIP_NAMES[1].parse().unwrap(),
                description: "another fip".into(),
            },
            ip: Some(contested_ip),
            pool: None,
        }))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "Requested external IP address not available");
}

#[nexus_test]
async fn test_floating_ip_create_name_in_use(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;

    let project = create_project(client, PROJECT_NAME).await;
    let contested_name = FIP_NAMES[0];

    // First create will succeed.
    create_floating_ip(
        client,
        contested_name,
        project.identity.name.as_str(),
        None,
        None,
    )
    .await;

    // Second will fail as the requested name is in use within this
    // project.
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &get_floating_ips_url(PROJECT_NAME),
        )
        .body(Some(&params::FloatingIpCreate {
            identity: IdentityMetadataCreateParams {
                name: contested_name.parse().unwrap(),
                description: "another fip".into(),
            },
            ip: None,
            pool: None,
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
        format!("already exists: floating-ip \"{contested_name}\""),
    );
}

#[nexus_test]
async fn test_floating_ip_update(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    // Create the Floating IP
    let fip = create_floating_ip(
        client,
        FIP_NAMES[0],
        project.identity.name.as_str(),
        None,
        None,
    )
    .await;

    let floating_ip_url = get_floating_ip_by_id_url(&fip.identity.id);

    // Verify that the Floating IP was created correctly
    let fetched_floating_ip: FloatingIp =
        object_get(client, &floating_ip_url).await;

    assert_eq!(fip.identity, fetched_floating_ip.identity);

    // Set up the updated values
    let new_fip_name: &str = "updated";
    let new_fip_desc: &str = "updated description";
    let updates: params::FloatingIpUpdate = params::FloatingIpUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some(String::from(new_fip_name).parse().unwrap()),
            description: Some(String::from(new_fip_desc).parse().unwrap()),
        },
    };

    // Update the Floating IP
    let new_fip: FloatingIp =
        object_put(client, &floating_ip_url, &updates).await;

    assert_eq!(new_fip.identity.name.as_str(), new_fip_name);
    assert_eq!(new_fip.identity.description, new_fip_desc);
    assert_eq!(new_fip.project_id, project.identity.id);
    assert_eq!(new_fip.identity.time_created, fip.identity.time_created);
    assert_ne!(new_fip.identity.time_modified, fip.identity.time_modified);

    // Verify that the Floating IP was updated correctly
    let fetched_modified_floating_ip: FloatingIp =
        object_get(client, &floating_ip_url).await;

    assert_eq!(new_fip.identity, fetched_modified_floating_ip.identity);
}

#[nexus_test]
async fn test_floating_ip_delete(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    let fip = create_floating_ip(
        client,
        FIP_NAMES[0],
        project.identity.name.as_str(),
        None,
        None,
    )
    .await;

    // unlink fails because there are outstanding IPs
    let silo_id = DEFAULT_SILO.id();
    let silo_link_url =
        format!("/v1/system/ip-pools/default/silos/{}", silo_id);
    let error =
        object_delete_error(client, &silo_link_url, StatusCode::BAD_REQUEST)
            .await;
    assert_eq!(
        error.message,
        "IP addresses from this pool are in use in the linked silo"
    );

    // Delete the floating IP.
    let floating_ip_url = get_floating_ip_by_id_url(&fip.identity.id);
    object_delete(client, &floating_ip_url).await;

    // now unlink works
    object_delete(client, &silo_link_url).await;
}

#[nexus_test]
async fn test_floating_ip_create_attachment(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;

    create_default_ip_pool(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    let fip = create_floating_ip(
        client,
        FIP_NAMES[0],
        project.identity.name.as_str(),
        None,
        None,
    )
    .await;

    // Bind the floating IP to an instance at create time.
    let instance_name = INSTANCE_NAMES[0];
    let instance = instance_for_external_ips(
        client,
        instance_name,
        true,
        false,
        &FIP_NAMES[..1],
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Reacquire FIP: parent ID must have updated to match instance.
    let fetched_fip =
        floating_ip_get(&client, &get_floating_ip_by_id_url(&fip.identity.id))
            .await;
    assert_eq!(fetched_fip.instance_id, Some(instance.identity.id));

    // Try to delete the floating IP, which should fail.
    let error = object_delete_error(
        client,
        &get_floating_ip_by_id_url(&fip.identity.id),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        error.message,
        format!("Floating IP cannot be deleted while attached to an instance"),
    );

    // Stop and delete the instance.
    instance_simulate(nexus, &instance_id).await;
    instance_simulate(nexus, &instance_id).await;

    let _: Instance = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/v1/instances/{}/stop", instance.identity.id),
        )
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    NexusRequest::object_delete(
        &client,
        &format!("/v1/instances/{instance_name}?project={PROJECT_NAME}"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Reacquire FIP again: parent ID must now be unset.
    let fetched_fip =
        floating_ip_get(&client, &get_floating_ip_by_id_url(&fip.identity.id))
            .await;
    assert_eq!(fetched_fip.instance_id, None);

    // Delete the floating IP.
    NexusRequest::object_delete(
        client,
        &get_floating_ip_by_id_url(&fip.identity.id),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn test_external_ip_live_attach_detach(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;

    create_default_ip_pool(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    const CAPACITY: f64 = 65536.0;
    assert_ip_pool_utilization(client, "default", 0, CAPACITY).await;

    // Create 2 instances, and a floating IP for each instance.
    // One instance will be started, and one will be stopped.
    let mut fips = vec![];
    for i in 0..2 {
        fips.push(
            create_floating_ip(
                client,
                FIP_NAMES[i],
                project.identity.name.as_str(),
                None,
                None,
            )
            .await,
        );
    }

    // 2 floating IPs have been allocated
    assert_ip_pool_utilization(client, "default", 2, CAPACITY).await;

    let mut instances = vec![];
    for (i, start) in [false, true].iter().enumerate() {
        let instance = instance_for_external_ips(
            client,
            INSTANCE_NAMES[i],
            *start,
            false,
            &[],
        )
        .await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

        if *start {
            instance_simulate(nexus, &instance_id).await;
            instance_simulate(nexus, &instance_id).await;
        }

        // Verify that each instance has only an SNAT external IP.
        let eips = fetch_instance_external_ips(
            client,
            INSTANCE_NAMES[i],
            PROJECT_NAME,
        )
        .await;
        assert_eq!(eips.len(), 1, "Expected exactly 1 SNAT external IP");
        assert_eq!(
            eips[0].kind(),
            shared::IpKind::SNat,
            "Expected exactly 1 SNAT external IP"
        );
        instances.push(instance);
    }

    // the two instances above were deliberately not given ephemeral IPs, but
    // they still always get SNAT IPs, but they share one, so we go from 2 to 3
    assert_ip_pool_utilization(client, "default", 3, CAPACITY).await;

    // Attach a floating IP and ephemeral IP to each instance.
    let mut recorded_ephs = vec![];
    for (instance, fip) in instances.iter().zip(&fips) {
        let instance_name = instance.identity.name.as_str();
        let eph_resp = ephemeral_ip_attach(client, instance_name, None).await;
        let fip_resp = floating_ip_attach(
            client,
            instance_name,
            fip.identity.name.as_str(),
        )
        .await;

        // Verify both appear correctly.
        // This implicitly checks FIP parent_id matches the instance,
        // and state has fully moved into 'Attached'.
        let eip_list =
            fetch_instance_external_ips(client, instance_name, PROJECT_NAME)
                .await;

        assert_eq!(eip_list.len(), 3);
        assert!(eip_list.contains(&eph_resp));
        assert!(
            eip_list
                .iter()
                .any(|v| matches!(v, views::ExternalIp::Floating(..))
                    && v.ip() == fip_resp.ip)
        );
        assert_eq!(fip.ip, fip_resp.ip);

        // Check for idempotency: repeat requests should return same values.
        let eph_resp_2 = ephemeral_ip_attach(client, instance_name, None).await;
        let fip_resp_2 = floating_ip_attach(
            client,
            instance_name,
            fip.identity.name.as_str(),
        )
        .await;

        assert_eq!(eph_resp, eph_resp_2);
        assert_eq!(fip_resp.ip, fip_resp_2.ip);

        recorded_ephs.push(eph_resp);
    }

    // now 5 because an ephemeral IP was added for each instance. floating IPs
    // were attached, but they were already allocated
    assert_ip_pool_utilization(client, "default", 5, CAPACITY).await;

    // Detach a floating IP and ephemeral IP from each instance.
    for (instance, fip) in instances.iter().zip(&fips) {
        let instance_name = instance.identity.name.as_str();
        ephemeral_ip_detach(client, instance_name).await;
        let fip_resp =
            floating_ip_detach(client, fip.identity.name.as_str()).await;

        // Verify both are removed, and that their bodies match the known FIP/EIP combo.
        let eip_list =
            fetch_instance_external_ips(client, instance_name, PROJECT_NAME)
                .await;

        // We still have 1 SNAT IP
        assert_eq!(eip_list.len(), 1);
        assert_eq!(fip.ip, fip_resp.ip);

        // Check for idempotency: repeat requests should return same values for FIP,
        // but in ephemeral case there is no currently known IP so we return an error.
        let fip_resp_2 =
            floating_ip_detach(client, fip.identity.name.as_str()).await;
        assert_eq!(fip_resp.ip, fip_resp_2.ip);

        let url = instance_ephemeral_ip_url(instance_name, PROJECT_NAME);
        let error =
            object_delete_error(client, &url, StatusCode::BAD_REQUEST).await;
        assert_eq!(
            error.message,
            "instance does not have an ephemeral IP attached".to_string()
        );
    }

    // 2 ephemeral go away on detachment but still 2 floating and 1 SNAT
    assert_ip_pool_utilization(client, "default", 3, CAPACITY).await;

    // Finally, two kind of funny tests. There is special logic in the handler
    // for the case where the floating IP is specified by name but the instance
    // by ID and vice versa, so we want to test both combinations.

    // Attach to an instance by instance ID with floating IP selected by name
    let floating_ip_name = fips[0].identity.name.as_str();
    let instance_id = instances[0].identity.id;
    let url = attach_floating_ip_url(floating_ip_name, PROJECT_NAME);
    let body = params::FloatingIpAttach {
        kind: params::FloatingIpParentKind::Instance,
        parent: instance_id.into(),
    };
    let attached: views::FloatingIp = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&body))
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(attached.identity.name.as_str(), floating_ip_name);

    let instance_name = instances[0].identity.name.as_str();
    let eip_list =
        fetch_instance_external_ips(client, instance_name, PROJECT_NAME).await;
    assert_eq!(eip_list.len(), 2, "Expected a Floating and SNAT IP");
    assert_eq!(
        eip_list
            .iter()
            .find_map(|eip| {
                if eip.kind() == shared::IpKind::Floating {
                    Some(eip.ip())
                } else {
                    None
                }
            })
            .expect("Should have a Floating IP"),
        fips[0].ip,
        "Floating IP object's IP address is not correct",
    );

    // now the other way: floating IP by ID and instance by name
    let floating_ip_id = fips[1].identity.id;
    let instance_name = instances[1].identity.name.as_str();
    let url = format!("/v1/floating-ips/{floating_ip_id}/attach");
    let body = params::FloatingIpAttach {
        kind: params::FloatingIpParentKind::Instance,
        parent: instance_name.parse::<Name>().unwrap().into(),
    };
    let attached: views::FloatingIp = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&body))
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(attached.identity.id, floating_ip_id);

    let eip_list =
        fetch_instance_external_ips(client, instance_name, PROJECT_NAME).await;
    assert_eq!(eip_list.len(), 2, "Expect a Floating and SNAT IP");
    assert_eq!(
        eip_list
            .iter()
            .find_map(|eip| {
                if eip.kind() == shared::IpKind::Floating {
                    Some(eip.ip())
                } else {
                    None
                }
            })
            .expect("Should have a Floating IP"),
        fips[1].ip,
        "Floating IP object's IP address is not correct",
    );

    // none of that changed the number of allocated IPs
    assert_ip_pool_utilization(client, "default", 3, CAPACITY).await;
}

#[nexus_test]
async fn test_floating_ip_attach_fail_between_projects(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let _nexus = &apictx.nexus;

    create_default_ip_pool(&client).await;
    let _project = create_project(client, PROJECT_NAME).await;
    let _project2 = create_project(client, "proj2").await;

    // Create a floating IP in another project.
    let fip =
        create_floating_ip(client, FIP_NAMES[0], "proj2", None, None).await;

    // Create a new instance *then* bind the FIP to it, both by ID.
    let instance =
        instance_for_external_ips(client, INSTANCE_NAMES[0], true, false, &[])
            .await;

    let url = attach_floating_ip_uuid(&fip.identity.id);
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&params::FloatingIpAttach {
                kind: params::FloatingIpParentKind::Instance,
                parent: instance.identity.id.into(),
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
        "floating IP must be in the same project as the instance".to_string()
    );

    // Create a new instance with a FIP, referenced by ID.
    let url = format!("/v1/instances?project={PROJECT_NAME}");
    let error = object_create_error(
        client,
        &url,
        &params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: INSTANCE_NAMES[1].parse().unwrap(),
                description: "".into(),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: "the-host".parse().unwrap(),
            user_data:
                b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                    .to_vec(),
            ssh_public_keys: Some(Vec::new()),
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::Default,
            external_ips: vec![params::ExternalIpCreate::Floating {
                floating_ip: fip.identity.id.into(),
            }],
            disks: vec![],
            boot_disk: None,
            start: true,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
        },
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        error.message,
        "floating IP must be in the same project as the instance".to_string()
    );
}

#[nexus_test]
async fn test_external_ip_attach_fail_if_in_use_by_other(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;

    create_default_ip_pool(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    // Create 2 instances, bind a FIP to each.
    let mut instances = vec![];
    let mut fips = vec![];
    for i in 0..2 {
        let fip = create_floating_ip(
            client,
            FIP_NAMES[i],
            project.identity.name.as_str(),
            None,
            None,
        )
        .await;
        let instance = instance_for_external_ips(
            client,
            INSTANCE_NAMES[i],
            true,
            false,
            &[FIP_NAMES[i]],
        )
        .await;
        let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

        instance_simulate(nexus, &instance_id).await;
        instance_simulate(nexus, &instance_id).await;

        instances.push(instance);
        fips.push(fip);
    }

    // Attach in-use FIP to *other* instance should fail.
    let url =
        attach_floating_ip_url(fips[1].identity.name.as_str(), PROJECT_NAME);
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&params::FloatingIpAttach {
                kind: params::FloatingIpParentKind::Instance,
                parent: INSTANCE_NAMES[0].parse::<Name>().unwrap().into(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "floating IP cannot be attached to one instance while still attached to another".to_string());
}

#[nexus_test]
async fn test_external_ip_attach_fails_after_maximum(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    // Create 33 floating IPs, and bind the first 32 to an instance.
    let mut fip_names = vec![];
    for i in 0..33 {
        let fip_name = format!("fip-{i}");
        create_floating_ip(
            client,
            &fip_name,
            project.identity.name.as_str(),
            None,
            None,
        )
        .await;
        fip_names.push(fip_name);
    }

    let fip_name_slice =
        fip_names.iter().map(String::as_str).collect::<Vec<_>>();
    let instance_name = INSTANCE_NAMES[0];
    instance_for_external_ips(
        client,
        instance_name,
        true,
        false,
        &fip_name_slice[..32],
    )
    .await;

    // Attempt to attach the final FIP should fail.
    let url = attach_floating_ip_url(fip_name_slice[32], PROJECT_NAME);
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&params::FloatingIpAttach {
                kind: params::FloatingIpParentKind::Instance,
                parent: instance_name.parse::<Name>().unwrap().into(),
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
        "an instance may not have more than 32 external IP addresses"
            .to_string()
    );

    // Attempt to attach an ephemeral IP should fail.
    let url = instance_ephemeral_ip_url(instance_name, PROJECT_NAME);
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&params::EphemeralIpCreate { pool: None }))
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
        "an instance may not have more than 32 external IP addresses"
            .to_string()
    );
}

#[nexus_test]
async fn test_external_ip_attach_ephemeral_at_pool_exhaustion(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;
    let other_pool_range = IpRange::V4(
        Ipv4Range::new(Ipv4Addr::new(10, 1, 0, 1), Ipv4Addr::new(10, 1, 0, 1))
            .unwrap(),
    );
    create_ip_pool(&client, "other-pool", Some(other_pool_range)).await;
    let silo_id = DEFAULT_SILO.id();
    link_ip_pool(&client, "other-pool", &silo_id, false).await;

    create_project(client, PROJECT_NAME).await;

    // Create two instances, to which we will later add eph IPs from 'other-pool'.
    for name in &INSTANCE_NAMES[..2] {
        instance_for_external_ips(client, name, false, false, &[]).await;
    }

    let pool_name: Name = "other-pool".parse().unwrap();

    // Attach a new EIP from other-pool to both instances.
    // This should succeed for the first, and fail for the second
    // due to pool exhaustion.
    let eph_resp = ephemeral_ip_attach(
        client,
        INSTANCE_NAMES[0],
        Some(pool_name.as_str()),
    )
    .await;
    assert_eq!(eph_resp.ip(), other_pool_range.first_address());
    assert_eq!(eph_resp.ip(), other_pool_range.last_address());

    let url = instance_ephemeral_ip_url(INSTANCE_NAMES[1], PROJECT_NAME);
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&params::ExternalIpCreate::Ephemeral {
                pool: Some(pool_name.clone().into()),
            }))
            .expect_status(Some(StatusCode::INSUFFICIENT_STORAGE)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        "Insufficient capacity: No external IP addresses available".to_string()
    );

    // Idempotent re-add to the first instance should succeed even if
    // an internal attempt to alloc a new EIP would fail.
    let eph_resp_2 = ephemeral_ip_attach(
        client,
        INSTANCE_NAMES[0],
        Some(pool_name.as_str()),
    )
    .await;
    assert_eq!(eph_resp_2, eph_resp);
}

#[nexus_test]
async fn can_list_instance_snat_ip(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let pool = create_default_ip_pool(&client).await;
    let _project = create_project(client, PROJECT_NAME).await;

    // Get the first address in the pool.
    let range = NexusRequest::object_get(
        client,
        &format!("/v1/system/ip-pools/{}/ranges", pool.identity.id),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|e| panic!("failed to get IP pool range: {e}"))
    .parsed_body::<IpPoolRangeResultsPage>()
    .unwrap_or_else(|e| panic!("failed to parse IP pool range: {e}"));
    assert_eq!(range.items.len(), 1, "Should have 1 range in the pool");
    let oxide_client::types::IpRange::V4(oxide_client::types::Ipv4Range {
        first,
        ..
    }) = &range.items[0].range
    else {
        panic!("Expected IPv4 range, found {:?}", &range.items[0]);
    };
    let expected_ip = IpAddr::V4(*first);

    // Create a running instance with only an SNAT IP address.
    let instance_name = INSTANCE_NAMES[0];
    let instance =
        instance_for_external_ips(client, instance_name, true, false, &[])
            .await;
    let url = format!("/v1/instances/{}/external-ips", instance.identity.id);
    let page = NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap_or_else(|e| {
            panic!("failed to make \"get\" request to {url}: {e}")
        })
        .parsed_body::<ExternalIpResultsPage>()
        .unwrap_or_else(|e| {
            panic!("failed to make \"get\" request to {url}: {e}")
        });
    let ips = page.items;
    assert_eq!(
        ips.len(),
        1,
        "Instance should have been created with exactly 1 IP"
    );
    let oxide_client::types::ExternalIp::Snat {
        ip,
        ip_pool_id,
        first_port,
        last_port,
    } = &ips[0]
    else {
        panic!("Expected an SNAT external IP, found {:?}", &ips[0]);
    };
    assert_eq!(ip_pool_id, &pool.identity.id);
    assert_eq!(ip, &expected_ip);

    // Port ranges are half-open on the right, e.g., [0, 16384).
    assert_eq!(*first_port, 0);
    assert_eq!(*last_port, NUM_SOURCE_NAT_PORTS - 1);
}

pub async fn floating_ip_get(
    client: &ClientTestContext,
    fip_url: &str,
) -> FloatingIp {
    floating_ip_get_as(client, fip_url, AuthnMode::PrivilegedUser).await
}

async fn floating_ip_get_as(
    client: &ClientTestContext,
    fip_url: &str,
    authn_as: AuthnMode,
) -> FloatingIp {
    NexusRequest::object_get(client, fip_url)
        .authn_as(authn_as)
        .execute()
        .await
        .unwrap_or_else(|e| {
            panic!("failed to make \"get\" request to {fip_url}: {e}")
        })
        .parsed_body()
        .unwrap_or_else(|e| {
            panic!("failed to make \"get\" request to {fip_url}: {e}")
        })
}

async fn instance_for_external_ips(
    client: &ClientTestContext,
    instance_name: &str,
    start: bool,
    use_ephemeral_ip: bool,
    floating_ip_names: &[&str],
) -> Instance {
    let mut fips: Vec<_> = floating_ip_names
        .iter()
        .map(|s| params::ExternalIpCreate::Floating {
            floating_ip: s.parse::<Name>().unwrap().into(),
        })
        .collect();
    if use_ephemeral_ip {
        fips.push(params::ExternalIpCreate::Ephemeral { pool: None })
    }
    create_instance_with(
        &client,
        PROJECT_NAME,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::Default,
        vec![],
        fips,
        start,
        Default::default(),
    )
    .await
}

async fn ephemeral_ip_attach(
    client: &ClientTestContext,
    instance_name: &str,
    pool_name: Option<&str>,
) -> views::ExternalIp {
    let url = instance_ephemeral_ip_url(instance_name, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&params::EphemeralIpCreate {
                pool: pool_name.map(|v| v.parse::<Name>().unwrap().into()),
            }))
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

async fn ephemeral_ip_detach(client: &ClientTestContext, instance_name: &str) {
    let url = instance_ephemeral_ip_url(instance_name, PROJECT_NAME);
    object_delete(client, &url).await;
}

async fn floating_ip_attach(
    client: &ClientTestContext,
    instance_name: &str,
    floating_ip_name: &str,
) -> views::FloatingIp {
    let url = attach_floating_ip_url(floating_ip_name, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&params::FloatingIpAttach {
                kind: params::FloatingIpParentKind::Instance,
                parent: instance_name.parse::<Name>().unwrap().into(),
            }))
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

async fn floating_ip_detach(
    client: &ClientTestContext,
    floating_ip_name: &str,
) -> views::FloatingIp {
    let url = detach_floating_ip_url(floating_ip_name, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}
