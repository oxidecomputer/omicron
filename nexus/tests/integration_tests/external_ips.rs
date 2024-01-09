// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests Floating IP support in the API

use std::net::IpAddr;
use std::net::Ipv4Addr;

use crate::integration_tests::instances::instance_simulate;
use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use http::Method;
use http::StatusCode;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_floating_ip;
use nexus_test_utils::resource_helpers::create_instance_with;
use nexus_test_utils::resource_helpers::create_ip_pool;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::create_silo;
use nexus_test_utils::resource_helpers::link_ip_pool;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::object_create_error;
use nexus_test_utils::resource_helpers::object_delete;
use nexus_test_utils::resource_helpers::object_delete_error;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::views::FloatingIp;
use nexus_types::identity::Resource;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv4Range;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::NameOrId;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const PROJECT_NAME: &str = "rootbeer-float";

const FIP_NAMES: &[&str] =
    &["vanilla", "chocolate", "strawberry", "pistachio", "caramel"];

pub fn get_floating_ips_url(project_name: &str) -> String {
    format!("/v1/floating-ips?project={project_name}")
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
    create_default_ip_pool(&client).await;

    let other_pool_range = IpRange::V4(
        Ipv4Range::new(Ipv4Addr::new(10, 1, 0, 1), Ipv4Addr::new(10, 1, 0, 5))
            .unwrap(),
    );
    // not automatically linked to currently silo. see below
    create_ip_pool(&client, "other-pool", Some(other_pool_range)).await;

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

    // Creating with other-pool fails with 404 until it is linked to the current silo
    let fip_name = FIP_NAMES[2];
    let params = params::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: fip_name.parse().unwrap(),
            description: String::from("a floating ip"),
        },
        address: None,
        pool: Some(NameOrId::Name("other-pool".parse().unwrap())),
    };
    let url = format!("/v1/floating-ips?project={}", project.identity.name);
    let error =
        object_create_error(client, &url, &params, StatusCode::NOT_FOUND).await;
    assert_eq!(error.message, "not found: ip-pool with name \"other-pool\"");

    // now link the pool and everything should work with the exact same params
    let silo_id = DEFAULT_SILO.id();
    link_ip_pool(&client, "other-pool", &silo_id, false).await;

    // Create with no chosen IP from named pool.
    let fip: FloatingIp = object_create(client, &url, &params).await;
    assert_eq!(fip.identity.name.as_str(), fip_name);
    assert_eq!(fip.project_id, project.identity.id);
    assert_eq!(fip.instance_id, None);
    assert_eq!(fip.ip, IpAddr::from(Ipv4Addr::new(10, 1, 0, 1)));

    // Create with chosen IP from fleet-scoped named pool.
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
        address: None,
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
            address: Some(contested_ip),
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
            address: None,
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
async fn test_floating_ip_attachment(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx();
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
    let instance_name = "anonymous-diner";
    let instance = create_instance_with(
        &client,
        PROJECT_NAME,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::Default,
        vec![],
        vec![params::ExternalIpCreate::Floating {
            floating_ip_name: FIP_NAMES[0].parse().unwrap(),
        }],
    )
    .await;

    // Reacquire FIP: parent ID must have updated to match instance.
    let fetched_fip =
        floating_ip_get(&client, &get_floating_ip_by_id_url(&fip.identity.id))
            .await;
    assert_eq!(fetched_fip.instance_id, Some(instance.identity.id));

    // Try to delete the floating IP, which should fail.
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::DELETE,
            &get_floating_ip_by_id_url(&fip.identity.id),
        )
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
        format!("Floating IP cannot be deleted while attached to an instance"),
    );

    // Stop and delete the instance.
    instance_simulate(nexus, &instance.identity.id).await;
    instance_simulate(nexus, &instance.identity.id).await;

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

    instance_simulate(nexus, &instance.identity.id).await;

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
