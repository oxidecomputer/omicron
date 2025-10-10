// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authorization tests for fleet-scoped multicast groups.
//!
//! Multicast groups are fleet-scoped resources (parent = "Fleet"), similar to
//! IP pools. This means:
//! - Only fleet admins can create/modify/delete multicast groups
//! - Silo users can attach their instances to any multicast group
//! - No project-level or silo-level isolation for groups themselves

use std::net::{IpAddr, Ipv4Addr};

use http::StatusCode;

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::test_params::UserPassword;
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_instance, create_local_user, create_project,
    grant_iam, link_ip_pool, object_get,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    InstanceCreate, InstanceNetworkInterfaceAttachment, MulticastGroupCreate,
    MulticastGroupMemberAdd, ProjectCreate,
};
use nexus_types::external_api::shared::SiloRole;
use nexus_types::external_api::views::{
    MulticastGroup, MulticastGroupMember, Silo,
};
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams, Instance,
    InstanceCpuCount, NameOrId,
};

use super::*;

/// Test that only fleet admins (privileged users) can create multicast groups.
/// Regular silo users should get 403 Forbidden.
#[nexus_test]
async fn test_only_fleet_admins_can_create_multicast_groups(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pool(&client).await;

    // Get current silo info
    let silo_url = format!("/v1/system/silos/{}", cptestctx.silo_name);
    let silo: Silo = object_get(client, &silo_url).await;

    // Create multicast IP pool (as fleet admin)
    create_multicast_ip_pool(&client, "mcast-pool").await;
    link_ip_pool(&client, "mcast-pool", &silo.identity.id, false).await;

    // Create a regular silo user (collaborator)
    let user = create_local_user(
        client,
        &silo,
        &"test-user".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    // Grant collaborator role to the user
    grant_iam(
        client,
        &silo_url,
        SiloRole::Collaborator,
        user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Try to create multicast group as the silo user - should FAIL with 403
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 101));
    let group_url = "/v1/multicast-groups";
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "user-group".parse().unwrap(),
            description: "Group created by silo user".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name("mcast-pool".parse().unwrap())),
    };

    let error = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &group_url)
            .body(Some(&group_params))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .expect("Expected 403 Forbidden for silo user creating multicast group")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();

    assert!(
        error.message.contains("forbidden")
            || error.message.contains("Forbidden"),
        "Expected forbidden error, got: {}",
        error.message
    );

    // Now create multicast group as fleet admin - should SUCCEED
    let group: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &group_url)
            .body(Some(&group_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(group.identity.name.as_str(), "user-group");
}

/// Test that silo users can attach their own instances to fleet-scoped
/// multicast groups, even though they can't create the groups themselves.
#[nexus_test]
async fn test_silo_users_can_attach_instances_to_multicast_groups(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pool(&client).await;

    // Get current silo info
    let silo_url = format!("/v1/system/silos/{}", cptestctx.silo_name);
    let silo: Silo = object_get(client, &silo_url).await;

    // Create multicast pool and link to silo
    create_multicast_ip_pool(&client, "mcast-pool").await;
    link_ip_pool(&client, "default", &silo.identity.id, true).await;
    link_ip_pool(&client, "mcast-pool", &silo.identity.id, false).await;

    // Create a regular silo user
    let user = create_local_user(
        client,
        &silo,
        &"test-user".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    grant_iam(
        client,
        &silo_url,
        SiloRole::Collaborator,
        user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create project as the silo user
    let project_url = "/v1/projects";
    let project_params = ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "user-project".parse().unwrap(),
            description: "Project created by silo user".to_string(),
        },
    };
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, project_url)
            .body(Some(&project_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .unwrap();

    // Fleet admin creates multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 100));
    let group_url = "/v1/multicast-groups";
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "shared-group".parse().unwrap(),
            description: "Fleet-scoped multicast group".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name("mcast-pool".parse().unwrap())),
    };
    let group: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &group_url)
            .body(Some(&group_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Silo user creates instance in their project
    let instance_url = "/v1/instances?project=user-project";
    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "user-instance".parse().unwrap(),
            description: "Instance created by silo user".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "user-instance".parse::<Hostname>().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance: Instance = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &instance_url)
            .body(Some(&instance_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Silo user can attach their instance to the fleet-scoped multicast group
    let member_add_url = format!(
        "/v1/multicast-groups/{}/members?project=user-project",
        group.identity.name
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance.identity.id),
    };

    let member: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &member_add_url)
            .body(Some(&member_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(member.instance_id, instance.identity.id);
    assert_eq!(member.multicast_group_id, group.identity.id);
}

/// Test that instances from different projects can attach to the same
/// fleet-scoped multicast group (no cross-project isolation).
#[nexus_test]
async fn test_cross_project_instance_attachment_allowed(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create pools and projects
    let (_, _project1, _project2, mcast_pool) = ops::join4(
        create_default_ip_pool(&client),
        create_project(client, "project1"),
        create_project(client, "project2"),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Fleet admin creates a multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 100));
    let group_url = "/v1/multicast-groups";
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "cross-project-group".parse().unwrap(),
            description: "Fleet-scoped group for cross-project test"
                .to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
    };
    let group: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &group_url)
            .body(Some(&group_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Create instances in both projects
    let instance1 = create_instance(client, "project1", "instance1").await;
    let instance2 = create_instance(client, "project2", "instance2").await;

    // Attach instance from project1 to the group
    let member_add_url1 = format!(
        "/v1/multicast-groups/{}/members?project=project1",
        group.identity.name
    );
    let member_params1 = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance1.identity.id),
    };
    let member1: MulticastGroupMember =
        object_create(client, &member_add_url1, &member_params1).await;

    // Attach instance from project2 to the SAME group - should succeed
    let member_add_url2 = format!(
        "/v1/multicast-groups/{}/members?project=project2",
        group.identity.name
    );
    let member_params2 = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance2.identity.id),
    };
    let member2: MulticastGroupMember =
        object_create(client, &member_add_url2, &member_params2).await;

    // Both instances should be members of the same group
    assert_eq!(member1.multicast_group_id, group.identity.id);
    assert_eq!(member2.multicast_group_id, group.identity.id);
    assert_eq!(member1.instance_id, instance1.identity.id);
    assert_eq!(member2.instance_id, instance2.identity.id);
}
