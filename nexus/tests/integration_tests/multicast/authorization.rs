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
use omicron_common::vlan::VlanID;

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
        mvlan: None,
    };

    // Try to create multicast group as silo user - should get 403 Forbidden
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &group_url)
            .body(Some(&group_params))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .expect("Expected 403 Forbidden for silo user creating multicast group");

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
        mvlan: None,
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
        "{}?project=user-project",
        mcast_group_members_url(&group.identity.name.to_string())
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

/// Test that authenticated silo users can read multicast groups without
/// requiring Fleet::Viewer role (verifies the Polar policy for read permission).
#[nexus_test]
async fn test_authenticated_users_can_read_multicast_groups(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pool(&client).await;

    // Get current silo info
    let silo_url = format!("/v1/system/silos/{}", cptestctx.silo_name);
    let silo: Silo = object_get(client, &silo_url).await;

    // Create multicast pool and link to silo
    create_multicast_ip_pool(&client, "mcast-pool").await;
    link_ip_pool(&client, "mcast-pool", &silo.identity.id, false).await;

    // Create a regular silo user with NO special roles (not even viewer)
    let user = create_local_user(
        client,
        &silo,
        &"regular-user".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    // Fleet admin creates a multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 100));
    let group_url = "/v1/multicast-groups";
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "readable-group".parse().unwrap(),
            description: "Group that should be readable by all silo users"
                .to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name("mcast-pool".parse().unwrap())),
        mvlan: Some(VlanID::new(100).unwrap()),
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

    // Wait for group to become active
    wait_for_group_active(client, "readable-group").await;

    // Regular silo user (with no Fleet roles) can GET the multicast group
    let get_group_url = mcast_group_url(&group.identity.name.to_string());
    let read_group: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, http::Method::GET, &get_group_url)
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .expect("Silo user should be able to read multicast group")
    .parsed_body()
    .unwrap();

    assert_eq!(read_group.identity.id, group.identity.id);
    assert_eq!(read_group.identity.name, group.identity.name);
    assert_eq!(read_group.multicast_ip, multicast_ip);
    assert_eq!(read_group.mvlan, Some(VlanID::new(100).unwrap()));

    // Regular silo user can also LIST multicast groups
    let list_groups: Vec<MulticastGroup> = NexusRequest::iter_collection_authn(
        client,
        "/v1/multicast-groups",
        "",
        None,
    )
    .await
    .expect("Silo user should be able to list multicast groups")
    .all_items;

    assert!(
        list_groups.iter().any(|g| g.identity.id == group.identity.id),
        "Multicast group should appear in list for silo user"
    );
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
        mvlan: None,
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
        "{}?project=project1",
        mcast_group_members_url(&group.identity.name.to_string())
    );
    let member_params1 = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance1.identity.id),
    };
    let member1: MulticastGroupMember =
        object_create(client, &member_add_url1, &member_params1).await;

    // Attach instance from project2 to the SAME group - should succeed
    let member_add_url2 = format!(
        "{}?project=project2",
        mcast_group_members_url(&group.identity.name.to_string())
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

/// Verify that unauthenticated users cannot list multicast groups without
/// proper authentication for the list endpoint.
#[nexus_test]
async fn test_unauthenticated_cannot_list_multicast_groups(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pool(&client).await;

    // Get current silo info
    let silo_url = format!("/v1/system/silos/{}", cptestctx.silo_name);
    let silo: Silo = object_get(client, &silo_url).await;

    // Create multicast pool and link to silo
    create_multicast_ip_pool(&client, "mcast-pool").await;
    link_ip_pool(&client, "mcast-pool", &silo.identity.id, false).await;

    // Fleet admin creates a multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 150));
    let group_url = "/v1/multicast-groups";
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "test-group".parse().unwrap(),
            description: "Group for auth test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name("mcast-pool".parse().unwrap())),
        mvlan: None,
    };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &group_url)
            .body(Some(&group_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Try to list multicast groups without authentication - should get 401 Unauthorized
    RequestBuilder::new(client, http::Method::GET, &group_url)
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("Expected 401 Unauthorized for unauthenticated list request");
}
