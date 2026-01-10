// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests Floating IP support in the API

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

use crate::integration_tests::instances::fetch_instance_external_ips;
use crate::integration_tests::instances::instance_simulate;
use crate::integration_tests::instances::instance_wait_for_state;
use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
use http::Method;
use http::StatusCode;
use nexus_auth::authz;
use nexus_db_model::IpPoolResourceType;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_db_queries::db::model::IncompleteIpPoolResource;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::assert_ip_pool_utilization;
use nexus_test_utils::resource_helpers::create_default_ip_pools;
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
use nexus_types::external_api::external_ip;
use nexus_types::external_api::floating_ip;
use nexus_types::external_api::floating_ip::FloatingIp;
use nexus_types::external_api::instance;
use nexus_types::external_api::instance::InstanceNetworkInterfaceAttachment;
use nexus_types::external_api::ip_pool;
use nexus_types::external_api::policy::SiloRole;
use nexus_types::external_api::project;
use nexus_types::external_api::silo;
use nexus_types::identity::Resource;
use omicron_common::address::IpRange;
use omicron_common::address::IpVersion;
use omicron_common::address::Ipv4Range;
use omicron_common::address::Ipv6Range;
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

    let (_v4_pool, v6_pool) = create_default_ip_pools(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    // Create a floating IP from the default pool.
    let fip_name = FIP_NAMES[0];
    let fip = create_floating_ip(
        client,
        fip_name,
        &project.identity.id.to_string(),
        None,
        Some(v6_pool.identity.name.as_str()),
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
    let (default_pool, _default_v6_pool) =
        create_default_ip_pools(&client).await;

    const CAPACITY: f64 = 65536.0;
    let pool_name = default_pool.identity.name.as_str();
    assert_ip_pool_utilization(client, pool_name, 0, CAPACITY).await;

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
        Some(default_pool.identity.name.as_str()),
    )
    .await;
    assert_eq!(fip.identity.name.as_str(), fip_name);
    assert_eq!(fip.project_id, project.identity.id);
    assert_eq!(fip.instance_id, None);
    assert_eq!(fip.ip, IpAddr::from(Ipv4Addr::new(10, 0, 0, 0)));
    assert_eq!(fip.ip_pool_id, default_pool.identity.id);

    assert_ip_pool_utilization(client, pool_name, 1, CAPACITY).await;

    // Create with chosen IP and fallback to default pool.
    let fip_name = FIP_NAMES[1];
    let ip_addr = "10.0.12.34".parse().unwrap();
    let fip = create_floating_ip(
        client,
        fip_name,
        project.identity.name.as_str(),
        Some(ip_addr),
        Some(default_pool.identity.name.as_str()),
    )
    .await;
    assert_eq!(fip.identity.name.as_str(), fip_name);
    assert_eq!(fip.project_id, project.identity.id);
    assert_eq!(fip.instance_id, None);
    assert_eq!(fip.ip, ip_addr);
    assert_eq!(fip.ip_pool_id, default_pool.identity.id);

    assert_ip_pool_utilization(client, pool_name, 2, CAPACITY).await;

    // Creating with other-pool fails with 404 until it is linked to the current silo
    let fip_name = FIP_NAMES[2];
    let params = floating_ip::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: fip_name.parse().unwrap(),
            description: String::from("a floating ip"),
        },
        address_selector: floating_ip::AddressSelector::Auto {
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name("other-pool".parse().unwrap()),
            },
        },
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
    let silo: silo::Silo = object_get(client, &silo_url).await;

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
        &project::ProjectCreate {
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
    let body = floating_ip::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: "root-beer".parse().unwrap(),
            description: String::from("a floating ip"),
        },
        address_selector: floating_ip::AddressSelector::Auto {
            pool_selector: ip_pool::PoolSelector::Auto { ip_version: None },
        },
    };
    let fip: floating_ip::FloatingIp =
        NexusRequest::objects_post(client, &create_url, &body)
            .authn_as(AuthnMode::SiloUser(user.id))
            .execute_and_parse_unwrap()
            .await;
    assert_eq!(fip.identity.name.to_string(), "root-beer");

    // now with other pool linked to my silo
    let body = floating_ip::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: "another-soda".parse().unwrap(),
            description: String::from("a floating ip"),
        },
        address_selector: floating_ip::AddressSelector::Auto {
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name("other-pool".parse().unwrap()),
            },
        },
    };
    let fip: floating_ip::FloatingIp =
        NexusRequest::objects_post(client, &create_url, &body)
            .authn_as(AuthnMode::SiloUser(user.id))
            .execute_and_parse_unwrap()
            .await;
    assert_eq!(fip.identity.name.to_string(), "another-soda");

    // now with pool not linked to my silo (fails with 404)
    let body = floating_ip::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: "secret-third-soda".parse().unwrap(),
            description: String::from("a floating ip"),
        },
        address_selector: floating_ip::AddressSelector::Auto {
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name("unlinked-pool".parse().unwrap()),
            },
        },
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
        silo::SiloIdentityMode::SamlJit,
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
    let body = floating_ip::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: fip_name.parse().unwrap(),
            description: String::from("a floating ip"),
        },
        address_selector: floating_ip::AddressSelector::Auto {
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: NameOrId::Name("external-silo-pool".parse().unwrap()),
            },
        },
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

    let (v4_pool, _v6_pool) = create_default_ip_pools(&client).await;

    let project = create_project(client, PROJECT_NAME).await;
    let contested_ip = "10.0.0.0".parse().unwrap();

    // First create will succeed.
    create_floating_ip(
        client,
        FIP_NAMES[0],
        project.identity.name.as_str(),
        Some(contested_ip),
        Some(v4_pool.identity.name.as_str()),
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
        .body(Some(&floating_ip::FloatingIpCreate {
            identity: IdentityMetadataCreateParams {
                name: FIP_NAMES[1].parse().unwrap(),
                description: "another fip".into(),
            },
            address_selector: floating_ip::AddressSelector::Explicit {
                ip: contested_ip,
                pool: Some(v4_pool.identity.name.clone().into()),
            },
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

    let (v4_pool, _v6_pool) = create_default_ip_pools(&client).await;

    let project = create_project(client, PROJECT_NAME).await;
    let contested_name = FIP_NAMES[0];

    // First create will succeed.
    create_floating_ip(
        client,
        contested_name,
        project.identity.name.as_str(),
        None,
        Some(v4_pool.identity.name.as_str()),
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
        .body(Some(&floating_ip::FloatingIpCreate {
            identity: IdentityMetadataCreateParams {
                name: contested_name.parse().unwrap(),
                description: "another fip".into(),
            },
            address_selector: floating_ip::AddressSelector::Auto {
                pool_selector: ip_pool::PoolSelector::Explicit {
                    pool: v4_pool.identity.name.clone().into(),
                },
            },
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

    let (_v4_pool, v6_pool) = create_default_ip_pools(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    // Create the Floating IP
    let fip = create_floating_ip(
        client,
        FIP_NAMES[0],
        project.identity.name.as_str(),
        None,
        Some(v6_pool.identity.name.as_str()),
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
    let updates: floating_ip::FloatingIpUpdate =
        floating_ip::FloatingIpUpdate {
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

    let (v4_pool, _v6_pool) = create_default_ip_pools(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    let fip = create_floating_ip(
        client,
        FIP_NAMES[0],
        project.identity.name.as_str(),
        None,
        Some(v4_pool.identity.name.as_str()),
    )
    .await;

    // unlink fails because there are outstanding IPs
    let silo_id = DEFAULT_SILO.id();
    let silo_link_url = format!(
        "/v1/system/ip-pools/{}/silos/{}",
        v4_pool.identity.name, silo_id
    );
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

    let (_v4_pool, v6_pool) = create_default_ip_pools(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    let fip = create_floating_ip(
        client,
        FIP_NAMES[0],
        project.identity.name.as_str(),
        None,
        Some(v6_pool.identity.name.as_str()),
    )
    .await;

    // Bind the floating IP to an instance at create time.
    let instance_name = INSTANCE_NAMES[0];
    let instance = instance_for_external_ips(
        client,
        instance_name,
        true,
        &InstanceNetworkInterfaceAttachment::DefaultDualStack,
        None,
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

    let (v4_pool, _v6_pool) = create_default_ip_pools(&client).await;
    let pool_name = v4_pool.identity.name.as_str();
    let project = create_project(client, PROJECT_NAME).await;

    const CAPACITY: f64 = 65536.0;
    assert_ip_pool_utilization(client, pool_name, 0, CAPACITY).await;

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
                Some(v4_pool.identity.name.as_str()),
            )
            .await,
        );
    }

    // 2 floating IPs have been allocated
    assert_ip_pool_utilization(client, pool_name, 2, CAPACITY).await;

    let mut instances = vec![];
    for (i, start) in [false, true].iter().enumerate() {
        let instance = instance_for_external_ips(
            client,
            INSTANCE_NAMES[i],
            *start,
            &InstanceNetworkInterfaceAttachment::DefaultIpv4,
            None,
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
            external_ip::IpKind::SNat,
            "Expected exactly 1 SNAT external IP"
        );
        instances.push(instance);
    }

    // the two instances above were deliberately not given ephemeral IPs, but
    // they still always get SNAT IPs, but they share one, so we go from 2 to 3
    assert_ip_pool_utilization(client, pool_name, 3, CAPACITY).await;

    // Attach a floating IP and ephemeral IP to each instance.
    let mut recorded_ephs = vec![];
    for (instance, fip) in instances.iter().zip(&fips) {
        let instance_name = instance.identity.name.as_str();
        let eph_resp = ephemeral_ip_attach(
            client,
            instance_name,
            Some(v4_pool.identity.name.as_str()),
        )
        .await;
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
                .any(|v| matches!(v, external_ip::ExternalIp::Floating(..))
                    && v.ip() == fip_resp.ip)
        );
        assert_eq!(fip.ip, fip_resp.ip);

        // Check for idempotency: repeat requests should return same values.
        let eph_resp_2 = ephemeral_ip_attach(
            client,
            instance_name,
            Some(v4_pool.identity.name.as_str()),
        )
        .await;
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
    assert_ip_pool_utilization(client, pool_name, 5, CAPACITY).await;

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
    assert_ip_pool_utilization(client, pool_name, 3, CAPACITY).await;

    // Finally, two kind of funny tests. There is special logic in the handler
    // for the case where the floating IP is specified by name but the instance
    // by ID and vice versa, so we want to test both combinations.

    // Attach to an instance by instance ID with floating IP selected by name
    let floating_ip_name = fips[0].identity.name.as_str();
    let instance_id = instances[0].identity.id;
    let url = attach_floating_ip_url(floating_ip_name, PROJECT_NAME);
    let body = floating_ip::FloatingIpAttach {
        kind: floating_ip::FloatingIpParentKind::Instance,
        parent: instance_id.into(),
    };
    let attached: floating_ip::FloatingIp = NexusRequest::new(
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
                if eip.kind() == external_ip::IpKind::Floating {
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
    let body = floating_ip::FloatingIpAttach {
        kind: floating_ip::FloatingIpParentKind::Instance,
        parent: instance_name.parse::<Name>().unwrap().into(),
    };
    let attached: floating_ip::FloatingIp = NexusRequest::new(
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
                if eip.kind() == external_ip::IpKind::Floating {
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
    assert_ip_pool_utilization(client, pool_name, 3, CAPACITY).await;
}

#[nexus_test]
async fn test_floating_ip_attach_fail_between_projects(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let _nexus = &apictx.nexus;

    let (v4_pool, _v6_pool) = create_default_ip_pools(&client).await;
    let _project = create_project(client, PROJECT_NAME).await;
    let _project2 = create_project(client, "proj2").await;

    // Create a floating IP in another project.
    let fip = create_floating_ip(
        client,
        FIP_NAMES[0],
        "proj2",
        None,
        Some(v4_pool.identity.name.as_str()),
    )
    .await;

    // Create a new instance *then* bind the FIP to it, both by ID.
    let instance = instance_for_external_ips(
        client,
        INSTANCE_NAMES[0],
        true,
        &InstanceNetworkInterfaceAttachment::DefaultDualStack,
        None,
        &[],
    )
    .await;

    let url = attach_floating_ip_uuid(&fip.identity.id);
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&floating_ip::FloatingIpAttach {
                kind: floating_ip::FloatingIpParentKind::Instance,
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
        &instance::InstanceCreate {
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
                instance::InstanceNetworkInterfaceAttachment::DefaultIpv4,
            external_ips: vec![instance::ExternalIpCreate::Floating {
                floating_ip: fip.identity.id.into(),
            }],
            disks: vec![],
            boot_disk: None,
            cpu_platform: None,
            start: true,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
            multicast_groups: Vec::new(),
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

    let (_v4_pool, v6_pool) = create_default_ip_pools(&client).await;
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
            Some(v6_pool.identity.name.as_str()),
        )
        .await;
        let instance = instance_for_external_ips(
            client,
            INSTANCE_NAMES[i],
            true,
            &InstanceNetworkInterfaceAttachment::DefaultDualStack,
            None,
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
            .body(Some(&floating_ip::FloatingIpAttach {
                kind: floating_ip::FloatingIpParentKind::Instance,
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
    assert_eq!(
        error.message,
        "floating IP cannot be attached to one instance while still attached to another",
    );
}

#[nexus_test]
async fn test_external_ip_attach_fails_after_maximum(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let (v4_pool, v6_pool) = create_default_ip_pools(&client).await;
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
            Some(v4_pool.identity.name.as_str()),
        )
        .await;
        fip_names.push(fip_name);
    }

    let fip_name_slice =
        fip_names.iter().map(String::as_str).collect::<Vec<_>>();
    let instance_name = INSTANCE_NAMES[0];

    // Here, we want to check an exact number of IPs. It's possible to do that
    // with a dual-stack instance, but a bit more subtle and complicated. See
    // the note on the constant `MAX_EXTERNAL_IPS_PLUS_SNAT` in
    // `nexus/db-queries/src/db/datastore/external_ip.rs` for more details.
    //
    // The fix is to resolve
    // https://github.com/oxidecomputer/omicron/issues/9003, but in the
    // meantime, test on a single-stack instance.
    instance_for_external_ips(
        client,
        instance_name,
        true,
        &InstanceNetworkInterfaceAttachment::DefaultIpv4,
        None,
        &fip_name_slice[..32],
    )
    .await;

    // Attempt to attach the final FIP should fail.
    let url = attach_floating_ip_url(fip_name_slice[32], PROJECT_NAME);
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&floating_ip::FloatingIpAttach {
                kind: floating_ip::FloatingIpParentKind::Instance,
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
            .body(Some(&instance::EphemeralIpCreate {
                pool_selector: ip_pool::PoolSelector::Explicit {
                    pool: v6_pool
                        .identity
                        .name
                        .as_str()
                        .parse::<Name>()
                        .unwrap()
                        .into(),
                },
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
}

#[nexus_test]
async fn test_external_ip_attach_ephemeral_at_pool_exhaustion(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pools(&client).await;
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
        instance_for_external_ips(
            client,
            name,
            false,
            &InstanceNetworkInterfaceAttachment::DefaultDualStack,
            None,
            &[],
        )
        .await;
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
            .body(Some(&instance::ExternalIpCreate::Ephemeral {
                pool_selector: ip_pool::PoolSelector::Explicit {
                    pool: pool_name.clone().into(),
                },
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

/// Test that creating a floating IP fails when both IPv4 and IPv6 default pools
/// exist and IP version is not specified, but succeeds with explicit IP version.
#[nexus_test]
async fn test_floating_ip_ip_version_conflict(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let silo_id = DEFAULT_SILO.id();
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let log = cptestctx.logctx.log.new(o!());
    let opctx = OpContext::for_tests(log, datastore.clone());

    create_project(&client, PROJECT_NAME).await;

    // Create IPv4 default pool via API
    let v4_range = IpRange::V4(
        Ipv4Range::new(Ipv4Addr::new(10, 0, 0, 1), Ipv4Addr::new(10, 0, 0, 10))
            .unwrap(),
    );
    create_ip_pool(&client, "v4-pool", Some(v4_range)).await;
    link_ip_pool(&client, "v4-pool", &silo_id, true).await;

    // Create IPv6 default pool via datastore (API rejects IPv6 ranges per PR #5107 atm)
    let v6_identity = IdentityMetadataCreateParams {
        name: "v6-pool".parse().unwrap(),
        description: String::new(),
    };
    let v6_pool = datastore
        .ip_pool_create(
            &opctx,
            nexus_db_model::IpPool::new(
                &v6_identity,
                nexus_db_model::IpVersion::V6,
                nexus_db_model::IpPoolReservationType::ExternalSilos,
            ),
        )
        .await
        .expect("Should be able to create IPv6 pool");

    // Add IPv6 range via datastore
    let by_id = NameOrId::Id(v6_pool.id());
    let (authz_pool, db_pool) = nexus
        .ip_pool_lookup(&opctx, &by_id)
        .expect("Should be able to lookup pool we just created")
        .fetch_for(authz::Action::CreateChild)
        .await
        .expect("Should be able to fetch pool we just created");
    let v6_range = IpRange::V6(
        Ipv6Range::new(
            Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1),
            Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 10),
        )
        .unwrap(),
    );
    datastore
        .ip_pool_add_range(&opctx, &authz_pool, &db_pool, &v6_range)
        .await
        .expect("Should be able to add IPv6 range");

    // Link IPv6 pool to silo as default via datastore
    let link = IncompleteIpPoolResource {
        ip_pool_id: v6_pool.id(),
        resource_type: IpPoolResourceType::Silo,
        resource_id: silo_id,
        is_default: true,
    };
    datastore
        .ip_pool_link_silo(&opctx, link)
        .await
        .expect("Should be able to link IPv6 pool to silo");

    let url = get_floating_ips_url(PROJECT_NAME);

    // Without `ip_version`, should fail with conflict error
    let fip_params = floating_ip::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: "should-fail".parse().unwrap(),
            description: "this should fail".to_string(),
        },
        address_selector: floating_ip::AddressSelector::Auto {
            pool_selector: ip_pool::PoolSelector::Auto { ip_version: None },
        },
    };
    let error =
        object_create_error(client, &url, &fip_params, StatusCode::BAD_REQUEST)
            .await;

    assert!(
        error.message.contains("Multiple"),
        "Expected conflict error, got: {}",
        error.message
    );

    // With explicit `ip_version` (V4), this should succeed
    let fip_v4_params = floating_ip::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: "fip-v4".parse().unwrap(),
            description: "IPv4 floating IP".to_string(),
        },
        address_selector: floating_ip::AddressSelector::Auto {
            pool_selector: ip_pool::PoolSelector::Auto {
                ip_version: Some(IpVersion::V4),
            },
        },
    };
    let fip_v4: FloatingIp = object_create(client, &url, &fip_v4_params).await;
    assert!(fip_v4.ip.is_ipv4(), "Expected IPv4 address");

    // With explicit `ip_version` (V6), this should succeed
    let fip_v6_params = floating_ip::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: "fip-v6".parse().unwrap(),
            description: "IPv6 floating IP".to_string(),
        },
        address_selector: floating_ip::AddressSelector::Auto {
            pool_selector: ip_pool::PoolSelector::Auto {
                ip_version: Some(IpVersion::V6),
            },
        },
    };
    let fip_v6: FloatingIp = object_create(client, &url, &fip_v6_params).await;
    assert!(fip_v6.ip.is_ipv6(), "Expected IPv6 address");
}

/// Test that attaching an ephemeral IP fails when both IPv4 and IPv6 default
/// pools exist and ip_version is not specified, but succeeds with explicit ip_version.
#[nexus_test]
async fn test_ephemeral_ip_ip_version_conflict(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project(&client, PROJECT_NAME).await;
    create_default_ip_pools(client).await;

    // Without IP version, this should fail with conflict error
    let _inst = instance_for_external_ips(
        client,
        INSTANCE_NAMES[0],
        false,
        &InstanceNetworkInterfaceAttachment::DefaultDualStack,
        None,
        &[],
    )
    .await;
    let url = instance_ephemeral_ip_url(INSTANCE_NAMES[0], PROJECT_NAME);
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&instance::EphemeralIpCreate {
                pool_selector: ip_pool::PoolSelector::Auto { ip_version: None },
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert!(
        error.message.contains("Multiple"),
        "Expected conflict error, got: {}",
        error.message
    );

    // With explicit IP version: V4, should succeed
    let eph_v4: external_ip::ExternalIp = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&instance::EphemeralIpCreate {
                pool_selector: ip_pool::PoolSelector::Auto {
                    ip_version: Some(IpVersion::V4),
                },
            }))
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert!(
        matches!(&eph_v4, external_ip::ExternalIp::Ephemeral { ip, .. } if ip.is_ipv4()),
        "Expected IPv4 ephemeral IP"
    );

    // Detach the V4 ephemeral so we can test V6
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // With explicit IP version: V6, should succeed
    let eph_v6: external_ip::ExternalIp = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&instance::EphemeralIpCreate {
                pool_selector: ip_pool::PoolSelector::Auto {
                    ip_version: Some(IpVersion::V6),
                },
            }))
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert!(
        matches!(&eph_v6, external_ip::ExternalIp::Ephemeral { ip, .. } if ip.is_ipv6()),
        "Expected IPv6 ephemeral IP"
    );
}

#[nexus_test]
async fn cannot_attach_floating_ipv4_to_instance_missing_ipv4_stack(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let (v4_pool, _v6_pool) = create_default_ip_pools(&client).await;
    create_project(client, PROJECT_NAME).await;

    // Create an instance with only a private IPv6 address.
    let instance_name = &INSTANCE_NAMES[0];
    let inst = create_instance_with(
        client,
        PROJECT_NAME,
        instance_name,
        &InstanceNetworkInterfaceAttachment::DefaultIpv6,
        vec![],
        vec![],
        false,
        None,
        None,
        vec![],
    )
    .await;

    // Create and attempt to attach a Floating IPv4 address.
    let fip_name = "my-fip";
    let _fip = create_floating_ip(
        client,
        fip_name,
        PROJECT_NAME,
        None,
        Some(v4_pool.identity.name.as_str()),
    )
    .await;
    let url = attach_floating_ip_url(fip_name, PROJECT_NAME);
    let result = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&floating_ip::FloatingIpAttach {
                kind: floating_ip::FloatingIpParentKind::Instance,
                parent: instance_name.parse::<Name>().unwrap().into(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect(
        "Should fail to attach IPv4 Floating IP to instance without private IPv4 stack"
    )
    .parsed_body::<HttpErrorResponseBody>()
    .expect("an HTTP error making this API call");
    assert_eq!(
        result.message,
        format!(
            "The floating external IP is an IPv4 address, but \
            the instance with ID {} does not have a primary \
            network interface with a VPC-private IPv4 address. \
            Add a VPC-private IPv4 address to the interface, \
            or attach a different IP address",
            inst.identity.id,
        ),
    );
}

#[nexus_test]
async fn cannot_attach_floating_ipv6_to_instance_missing_ipv6_stack(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let (_v4_pool, v6_pool) = create_default_ip_pools(&client).await;
    create_project(client, PROJECT_NAME).await;

    // Create an instance with only a private IPv4 address.
    let instance_name = &INSTANCE_NAMES[0];
    let inst = create_instance_with(
        client,
        PROJECT_NAME,
        instance_name,
        &InstanceNetworkInterfaceAttachment::DefaultIpv4,
        vec![],
        vec![],
        false,
        None,
        None,
        vec![],
    )
    .await;

    // Create and attempt to attach a Floating IPv6 address.
    let fip_name = "my-fip";
    let _fip = create_floating_ip(
        client,
        fip_name,
        PROJECT_NAME,
        None,
        Some(v6_pool.identity.name.as_str()),
    )
    .await;
    let url = attach_floating_ip_url(fip_name, PROJECT_NAME);
    let result = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&floating_ip::FloatingIpAttach {
                kind: floating_ip::FloatingIpParentKind::Instance,
                parent: instance_name.parse::<Name>().unwrap().into(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .expect("an HTTP error making this API call");
    assert_eq!(
        result.message,
        format!(
            "The floating external IP is an IPv6 address, but \
            the instance with ID {} does not have a primary \
            network interface with a VPC-private IPv6 address. \
            Add a VPC-private IPv6 address to the interface, \
            or attach a different IP address",
            inst.identity.id,
        ),
    );
}

#[nexus_test]
async fn cannot_attach_ephemeral_ipv4_to_instance_missing_ipv4_stack(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let (v4_pool, _v6_pool) = create_default_ip_pools(&client).await;
    create_project(client, PROJECT_NAME).await;

    // Create an instance with only a private IPv6 address.
    let instance_name = &INSTANCE_NAMES[0];
    let inst = create_instance_with(
        client,
        PROJECT_NAME,
        instance_name,
        &InstanceNetworkInterfaceAttachment::DefaultIpv6,
        vec![],
        vec![],
        false,
        None,
        None,
        vec![],
    )
    .await;

    // Now try to attach an Ephemeral IPv4 address.
    let url = instance_ephemeral_ip_url(instance_name, PROJECT_NAME);
    let result = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&instance::EphemeralIpCreate {
                pool_selector: ip_pool::PoolSelector::Explicit {
                    pool: v4_pool.identity.name.clone().into(),
                },
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should fail attaching Ephemeral IPv4 address")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .expect("an error response");
    assert_eq!(
        result.message,
        format!(
            "The ephemeral external IP is an IPv4 address, but \
            the instance with ID {} does not have a primary \
            network interface with a VPC-private IPv4 address. \
            Add a VPC-private IPv4 address to the interface, \
            or attach a different IP address",
            inst.identity.id,
        ),
    );
}

#[nexus_test]
async fn cannot_attach_ephemeral_ipv6_to_instance_missing_ipv6_stack(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let (_v4_pool, v6_pool) = create_default_ip_pools(&client).await;
    create_project(client, PROJECT_NAME).await;

    // Create an instance with only a private IPv4 address.
    let instance_name = &INSTANCE_NAMES[0];
    let inst = create_instance_with(
        client,
        PROJECT_NAME,
        instance_name,
        &InstanceNetworkInterfaceAttachment::DefaultIpv4,
        vec![],
        vec![],
        false,
        None,
        None,
        vec![],
    )
    .await;

    // Now try to attach an Ephemeral IPv6 address.
    let url = instance_ephemeral_ip_url(instance_name, PROJECT_NAME);
    let result = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&instance::EphemeralIpCreate {
                pool_selector: ip_pool::PoolSelector::Explicit {
                    pool: v6_pool.identity.name.clone().into(),
                },
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        result.message,
        format!(
            "The ephemeral external IP is an IPv6 address, but \
            the instance with ID {} does not have a primary \
            network interface with a VPC-private IPv6 address. \
            Add a VPC-private IPv6 address to the interface, \
            or attach a different IP address",
            inst.identity.id,
        ),
    );
}

#[nexus_test]
async fn can_list_instance_snat_ip(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let (v4_pool, v6_pool) = create_default_ip_pools(&client).await;
    let _project = create_project(client, PROJECT_NAME).await;

    // Get the first address in the v4 pool.
    let range = NexusRequest::object_get(
        client,
        &format!("/v1/system/ip-pools/{}/ranges", v4_pool.identity.id),
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
    let expected_v4_ip = IpAddr::V4(*first);

    // Get the first address in the vs6 pool.
    let range = NexusRequest::object_get(
        client,
        &format!("/v1/system/ip-pools/{}/ranges", v6_pool.identity.id),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|e| panic!("failed to get IP pool range: {e}"))
    .parsed_body::<IpPoolRangeResultsPage>()
    .unwrap_or_else(|e| panic!("failed to parse IP pool range: {e}"));
    assert_eq!(range.items.len(), 1, "Should have 1 range in the pool");
    let oxide_client::types::IpRange::V6(oxide_client::types::Ipv6Range {
        first,
        ..
    }) = &range.items[0].range
    else {
        panic!("Expected IPv6 range, found {:?}", &range.items[0]);
    };
    let expected_v6_ip = IpAddr::V6(*first);

    // Create a running instance with only an SNAT IP address, for each IP
    // stack.
    let instance_name = INSTANCE_NAMES[0];
    let instance = instance_for_external_ips(
        client,
        instance_name,
        true,
        &InstanceNetworkInterfaceAttachment::DefaultDualStack,
        None,
        &[],
    )
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
        2,
        "Instance should have been created with exactly 2 IPs"
    );

    // Find the IPv4 IP and check it.
    let res = ips
        .iter()
        .find(|ip| match ip {
            oxide_client::types::ExternalIp::Snat { ip, .. }
                if ip.is_ipv4() =>
            {
                true
            }
            _ => false,
        })
        .expect("Expected to find IPv4 SNAT IP");
    let oxide_client::types::ExternalIp::Snat {
        ip,
        ip_pool_id,
        first_port,
        last_port,
    } = res
    else {
        panic!("Expected an SNAT external IP, found {:?}", res);
    };
    assert_eq!(ip_pool_id, &v4_pool.identity.id);
    assert_eq!(ip, &expected_v4_ip);

    // Port ranges are half-open on the right, e.g., [0, 16384).
    assert_eq!(*first_port, 0);
    assert_eq!(*last_port, NUM_SOURCE_NAT_PORTS - 1);

    // Find the IPv6 IP and check it.
    let res = ips
        .iter()
        .find(|ip| match ip {
            oxide_client::types::ExternalIp::Snat { ip, .. }
                if ip.is_ipv6() =>
            {
                true
            }
            _ => false,
        })
        .expect("Expected to find IPv6 SNAT IP");
    let oxide_client::types::ExternalIp::Snat {
        ip,
        ip_pool_id,
        first_port,
        last_port,
    } = res
    else {
        panic!("Expected an SNAT external IP, found {:?}", res);
    };
    assert_eq!(ip_pool_id, &v6_pool.identity.id);
    assert_eq!(ip, &expected_v6_ip);

    // Port ranges are half-open on the right, e.g., [0, 16384).
    assert_eq!(*first_port, 0);
    assert_eq!(*last_port, NUM_SOURCE_NAT_PORTS - 1);
}

// Sanity check that we can just attach an IPv6 Ephemeral address to an instance
// with a private IPv6 stack.
#[nexus_test]
async fn can_create_instance_with_ephemeral_ipv6_address(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let (_pool, v6_pool) = create_default_ip_pools(&client).await;
    let _project = create_project(client, PROJECT_NAME).await;

    // Get the first address in the IPv6 pool.
    let range = NexusRequest::object_get(
        client,
        &format!("/v1/system/ip-pools/{}/ranges", v6_pool.identity.id),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|e| panic!("failed to get IP pool range: {e}"))
    .parsed_body::<IpPoolRangeResultsPage>()
    .unwrap_or_else(|e| panic!("failed to parse IP pool range: {e}"));
    assert_eq!(range.items.len(), 1, "Should have 1 range in the pool");
    let oxide_client::types::IpRange::V6(oxide_client::types::Ipv6Range {
        first,
        ..
    }) = &range.items[0].range
    else {
        panic!("Expected IPv6 range, found {:?}", &range.items[0]);
    };
    let expected_ip = IpAddr::V6(*first);

    // Create a running instance with an Ephemeral IPv6 address.
    let instance_name = INSTANCE_NAMES[0];
    let instance = create_instance_with(
        &client,
        PROJECT_NAME,
        instance_name,
        &InstanceNetworkInterfaceAttachment::DefaultIpv6,
        /* disks = */ vec![],
        vec![instance::ExternalIpCreate::Ephemeral {
            pool_selector: ip_pool::PoolSelector::Explicit {
                pool: v6_pool.identity.id.into(),
            },
        }],
        /* start = */ false,
        /* auto_restart_policy = */ Default::default(),
        /* instance_cpu_platform = */ None,
        /* multicast_groups = */ vec![],
    )
    .await;

    // First, sanity check the SNAT IPv6 address. These are currently created
    // unconditionally, but see
    // https://github.com/oxidecomputer/omicron/issues/4317 for more details.
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
        2,
        "Instance should have been created with exactly 2 external IPs"
    );
    let res = ips
        .iter()
        .find(|ip| matches!(ip, oxide_client::types::ExternalIp::Snat { .. }))
        .expect("An SNAT IP");
    let oxide_client::types::ExternalIp::Snat {
        ip,
        ip_pool_id,
        first_port,
        last_port,
    } = res
    else {
        panic!("Expected an SNAT external IP, found {:?}", res);
    };
    assert_eq!(ip_pool_id, &v6_pool.identity.id);
    assert_eq!(ip, &expected_ip);

    // Port ranges are half-open on the right, e.g., [0, 16384).
    assert_eq!(*first_port, 0);
    assert_eq!(*last_port, NUM_SOURCE_NAT_PORTS - 1);

    // Now check the Ephemeral IPv6 address.
    let res = ips
        .iter()
        .find(|ip| {
            matches!(ip, oxide_client::types::ExternalIp::Ephemeral { .. })
        })
        .expect("An Ephemeral IP");
    let oxide_client::types::ExternalIp::Ephemeral { ip, ip_pool_id } = res
    else {
        panic!("Expected an Ephemeral external IP, found {:?}", res);
    };
    assert_eq!(ip_pool_id, &v6_pool.identity.id);
    let expected_ip = IpAddr::V6(Ipv6Addr::from_bits(first.to_bits() + 1));
    assert_eq!(ip, &expected_ip);
}

// Sanity check that we can just attach an IPv6 Floating address to an instance
// with a private IPv6 stack.
#[nexus_test]
async fn can_create_instance_with_floating_ipv6_address(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let (_pool, v6_pool) = create_default_ip_pools(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

    // Get the first address in the IPv6 pool.
    let range = NexusRequest::object_get(
        client,
        &format!("/v1/system/ip-pools/{}/ranges", v6_pool.identity.id),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|e| panic!("failed to get IP pool range: {e}"))
    .parsed_body::<IpPoolRangeResultsPage>()
    .unwrap_or_else(|e| panic!("failed to parse IP pool range: {e}"));
    assert_eq!(range.items.len(), 1, "Should have 1 range in the pool");
    let oxide_client::types::IpRange::V6(oxide_client::types::Ipv6Range {
        first,
        ..
    }) = &range.items[0].range
    else {
        panic!("Expected IPv6 range, found {:?}", &range.items[0]);
    };
    let expected_ip = IpAddr::V6(*first);

    // We're creating the FIP first, explicity. The SNAT is allocated
    // automatically during instance creation, and so takes the next address.
    let expected_snat_ip = IpAddr::V6(Ipv6Addr::from(u128::from(*first) + 1));

    // Create a floating IP, from the IPv6 Pool.
    let fip_name = FIP_NAMES[0];
    let fip = create_floating_ip(
        client,
        fip_name,
        &project.identity.id.to_string(),
        None,
        Some(v6_pool.identity.name.as_str()),
    )
    .await;

    // Create a running instance with that Floating IPv6 address.
    let instance_name = INSTANCE_NAMES[0];
    let instance = create_instance_with(
        &client,
        PROJECT_NAME,
        instance_name,
        &InstanceNetworkInterfaceAttachment::DefaultIpv6,
        /* disks = */ vec![],
        vec![instance::ExternalIpCreate::Floating {
            floating_ip: NameOrId::Id(fip.identity.id),
        }],
        /* start = */ false,
        /* auto_restart_policy = */ Default::default(),
        /* instance_cpu_platform = */ None,
        /* multicast_groups = */ vec![],
    )
    .await;

    // First, sanity check the SNAT IPv6 address. These are currently created
    // unconditionally, but see
    // https://github.com/oxidecomputer/omicron/issues/4317 for more details.
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
        2,
        "Instance should have been created with exactly 2 external IPs"
    );

    let ip = ips
        .iter()
        .find(|ip| matches!(ip, oxide_client::types::ExternalIp::Snat { .. }))
        .expect("Should contain an SNAT IP");
    let oxide_client::types::ExternalIp::Snat {
        ip,
        ip_pool_id,
        first_port,
        last_port,
    } = ip
    else {
        panic!("Expected an SNAT external IP, found {:?}", &ips[0]);
    };
    assert_eq!(ip_pool_id, &v6_pool.identity.id);
    assert_eq!(ip, &expected_snat_ip);

    // Port ranges are half-open on the right, e.g., [0, 16384).
    assert_eq!(*first_port, 0);
    assert_eq!(*last_port, NUM_SOURCE_NAT_PORTS - 1);

    // Then check the Floating IPv6 address.
    let ip = ips
        .iter()
        .find(|ip| {
            matches!(ip, oxide_client::types::ExternalIp::Floating { .. })
        })
        .expect("Should contain an SNAT IP");
    let oxide_client::types::ExternalIp::Floating {
        id,
        instance_id,
        ip,
        ip_pool_id,
        ..
    } = ip
    else {
        panic!("Expected a Floating external IP, found {:?}", &ips[1]);
    };
    assert_eq!(id, &fip.identity.id);
    assert_eq!(instance_id, &Some(instance.identity.id));
    assert_eq!(ip_pool_id, &v6_pool.identity.id);
    assert_eq!(ip, &expected_ip);
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
    nic: &InstanceNetworkInterfaceAttachment,
    ephemeral_ip_version: Option<IpVersion>,
    floating_ip_names: &[&str],
) -> Instance {
    let mut eips: Vec<_> = floating_ip_names
        .iter()
        .map(|s| instance::ExternalIpCreate::Floating {
            floating_ip: s.parse::<Name>().unwrap().into(),
        })
        .collect();
    if let Some(ip_version) = ephemeral_ip_version {
        eips.push(instance::ExternalIpCreate::Ephemeral {
            pool_selector: ip_pool::PoolSelector::Auto {
                ip_version: Some(ip_version),
            },
        })
    }
    create_instance_with(
        &client,
        PROJECT_NAME,
        instance_name,
        nic,
        vec![],
        eips,
        start,
        Default::default(),
        None,
        Vec::new(),
    )
    .await
}

async fn ephemeral_ip_attach(
    client: &ClientTestContext,
    instance_name: &str,
    pool_name: Option<&str>,
) -> external_ip::ExternalIp {
    let url = instance_ephemeral_ip_url(instance_name, PROJECT_NAME);
    let pool_selector = match pool_name {
        Some(name) => ip_pool::PoolSelector::Explicit {
            pool: name.parse::<Name>().unwrap().into(),
        },
        None => ip_pool::PoolSelector::Auto { ip_version: None },
    };
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&instance::EphemeralIpCreate { pool_selector }))
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
) -> floating_ip::FloatingIp {
    let url = attach_floating_ip_url(floating_ip_name, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(Some(&floating_ip::FloatingIpAttach {
                kind: floating_ip::FloatingIpParentKind::Instance,
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
) -> floating_ip::FloatingIp {
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
