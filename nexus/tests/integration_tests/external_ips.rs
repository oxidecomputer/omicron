// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests Floating IP support in the API

use std::net::IpAddr;
use std::net::Ipv4Addr;

use crate::integration_tests::instances::fetch_instance_external_ips;
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
use nexus_types::external_api::views;
use nexus_types::external_api::views::FloatingIp;
use nexus_types::identity::Resource;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv4Range;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
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
    format!("/v1/instances/{instance_name}/external-ips/ephemeral?project={project_name}")
}

pub fn attach_floating_ip_url(
    floating_ip_name: &str,
    project_name: &str,
) -> String {
    format!("/v1/floating-ips/{floating_ip_name}/attach?project={project_name}")
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
        ip: None,
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
    let instance_name = INSTANCE_NAMES[0];
    let instance = instance_for_external_ips(
        client,
        instance_name,
        true,
        false,
        &FIP_NAMES[..1],
    )
    .await;

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

#[nexus_test]
async fn test_external_ip_live_attach_detach(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx();
    let nexus = &apictx.nexus;

    create_default_ip_pool(&client).await;
    let project = create_project(client, PROJECT_NAME).await;

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

        if *start {
            instance_simulate(nexus, &instance.identity.id).await;
            instance_simulate(nexus, &instance.identity.id).await;
        }

        // Verify that each instance has no external IPs.
        assert_eq!(
            fetch_instance_external_ips(
                client,
                INSTANCE_NAMES[i],
                PROJECT_NAME
            )
            .await
            .len(),
            0
        );

        instances.push(instance);
    }

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

        assert_eq!(eip_list.len(), 2);
        assert!(eip_list.contains(&eph_resp));
        assert!(eip_list
            .iter()
            .any(|v| matches!(v, views::ExternalIp::Floating(..))
                && v.ip() == fip_resp.ip));
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

        assert_eq!(eip_list.len(), 0);
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
}

#[nexus_test]
async fn test_external_ip_attach_detach_fail_if_in_use_by_other(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx();
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

        instance_simulate(nexus, &instance.identity.id).await;
        instance_simulate(nexus, &instance.identity.id).await;

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
