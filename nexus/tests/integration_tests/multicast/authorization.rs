// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authorization and isolation tests for multicast groups.
//!
//! Tests cross-project isolation, silo isolation, and RBAC permissions
//! following patterns from external IP tests.

use std::net::{IpAddr, Ipv4Addr};

use http::StatusCode;

use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::test_params::UserPassword;
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_local_user, create_project, create_silo,
    grant_iam, link_ip_pool, object_create, object_create_error, object_get,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    self, InstanceCreate, InstanceNetworkInterfaceAttachment, IpPoolCreate,
    MulticastGroupCreate, MulticastGroupMemberAdd, ProjectCreate,
};
use nexus_types::external_api::shared::{SiloIdentityMode, SiloRole};
use nexus_types::external_api::views::{
    self, IpPool, IpPoolRange, IpVersion, MulticastGroup, MulticastGroupMember,
    Silo,
};
use nexus_types::identity::Resource;
use omicron_common::address::{IpRange, Ipv4Range};
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams, Instance,
    InstanceCpuCount, NameOrId,
};

use super::*;

#[nexus_test]
async fn test_multicast_group_attach_fail_between_projects(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create pools and projects in parallel
    let (_, _, _, mcast_pool) = ops::join4(
        create_default_ip_pool(&client),
        create_project(client, "project1"),
        create_project(client, "project2"),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create a multicast group in project2
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 100));
    let group_url = "/v1/multicast-groups?project=project2";
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "cross-project-group".parse().unwrap(),
            description: "Group for cross-project test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
        vpc: None,
    };
    let group: MulticastGroup =
        object_create(client, &group_url, &group_params).await;

    // Create an instance in project1
    let instance_url = "/v1/instances?project=project1";
    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "cross-project-instance".parse().unwrap(),
            description: "Instance in different project".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "cross-project-instance".parse::<Hostname>().unwrap(),
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
    let instance: Instance =
        object_create(client, &instance_url, &instance_params).await;

    // Try to add the instance from project1 to the multicast group in project2
    // This should fail - instances can only join multicast groups in the same project
    let member_add_url = format!(
        "/v1/multicast-groups/{}/members?project=project2",
        group.identity.name
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance.identity.id),
    };

    let error = object_create_error(
        client,
        &member_add_url,
        &member_params,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // The error should indicate that the instance is not found in this project
    // (because it exists in a different project)
    assert!(
        error.message.contains("not found")
            || error.message.contains("instance"),
        "Expected not found error for cross-project instance, got: {}",
        error.message
    );
}

#[nexus_test]
async fn test_multicast_group_create_fails_in_other_silo_pool(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project = create_project(client, "test-project").await;

    // Create other silo and IP pool linked to that silo
    let other_silo =
        create_silo(&client, "not-my-silo", true, SiloIdentityMode::SamlJit)
            .await;

    // Create multicast pool but DON'T link it to any silo initially
    // We need to create the pool manually to avoid automatic linking

    let pool_params = IpPoolCreate::new_multicast(
        IdentityMetadataCreateParams {
            name: "external-silo-pool".parse().unwrap(),
            description: "Multicast IP pool for silo isolation testing"
                .to_string(),
        },
        IpVersion::V4,
        None,
        None,
    );

    object_create::<_, IpPool>(client, "/v1/system/ip-pools", &pool_params)
        .await;

    // Add the IP range
    let pool_range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(224, 0, 2, 1),
            std::net::Ipv4Addr::new(224, 0, 2, 255),
        )
        .unwrap(),
    );
    let range_url =
        "/v1/system/ip-pools/external-silo-pool/ranges/add".to_string();
    object_create::<_, IpPoolRange>(client, &range_url, &pool_range).await;

    // Don't link pool to current silo yet
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 2, 100));
    let group_url =
        format!("/v1/multicast-groups?project={}", project.identity.name);
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "silo-test-group".parse().unwrap(),
            description: "Group for silo isolation test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name("external-silo-pool".parse().unwrap())),
        vpc: None,
    };

    // Creating a multicast group should fail with 404 as if the pool doesn't exist
    let error = object_create_error(
        client,
        &group_url,
        &group_params,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(
        error.message,
        "not found: ip-pool with name \"external-silo-pool\""
    );

    // Error should be the same after linking the pool to the other silo
    link_ip_pool(&client, "external-silo-pool", &other_silo.identity.id, false)
        .await;
    let error = object_create_error(
        client,
        &group_url,
        &group_params,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(
        error.message,
        "not found: ip-pool with name \"external-silo-pool\""
    );

    // Only after linking the pool to the current silo should it work
    let silo_id = DEFAULT_SILO.id();
    link_ip_pool(&client, "external-silo-pool", &silo_id, false).await;

    // Now the group creation should succeed
    object_create::<_, MulticastGroup>(client, &group_url, &group_params).await;
}

#[nexus_test]
async fn test_multicast_group_rbac_permissions(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pool(&client).await;

    // Get current silo info
    let silo_url = format!("/v1/system/silos/{}", cptestctx.silo_name);
    let silo: Silo = object_get(client, &silo_url).await;

    // Link the default IP pool to the silo so silo users can create instances
    link_ip_pool(&client, "default", &silo.identity.id, true).await;

    // Create multicast IP pool and ensure it's linked to the test silo
    create_multicast_ip_pool(&client, "rbac-pool").await;
    // Also link to the test silo to ensure silo users can see it
    link_ip_pool(&client, "rbac-pool", &silo.identity.id, false).await;

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
    .unwrap()
    .parsed_body::<views::Project>()
    .unwrap();

    // Create multicast group as the silo user
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 101));
    let group_url = "/v1/multicast-groups?project=user-project";
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "user-group".parse().unwrap(),
            description: "Group created by silo user".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name("rbac-pool".parse().unwrap())),
        vpc: None,
    };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &group_url)
            .body(Some(&group_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .unwrap()
    .parsed_body::<MulticastGroup>()
    .unwrap();

    // Create instance as the silo user
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

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &instance_url)
            .body(Some(&instance_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .unwrap()
    .parsed_body::<omicron_common::api::external::Instance>()
    .unwrap();

    // Add instance to multicast group as silo user
    let member_add_url =
        "/v1/multicast-groups/user-group/members?project=user-project";
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("user-instance".parse().unwrap()),
    };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &member_add_url)
            .body(Some(&member_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .unwrap()
    .parsed_body::<MulticastGroupMember>()
    .unwrap();
}

#[nexus_test]
async fn test_multicast_group_cross_silo_isolation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pool(&client).await;

    // Create two separate silos with LocalOnly identity mode for local users
    let silo1 =
        create_silo(&client, "silo-one", true, SiloIdentityMode::LocalOnly)
            .await;

    let silo2 =
        create_silo(&client, "silo-two", true, SiloIdentityMode::LocalOnly)
            .await;

    // Create multicast pools using the shared helper
    create_multicast_ip_pool_with_range(
        &client,
        "silo1-pool",
        (224, 0, 3, 1),
        (224, 0, 3, 255),
    )
    .await;
    create_multicast_ip_pool_with_range(
        &client,
        "silo2-pool",
        (224, 0, 4, 1),
        (224, 0, 4, 255),
    )
    .await;

    // Link pools to respective silos in parallel
    ops::join2(
        link_ip_pool(&client, "silo1-pool", &silo1.identity.id, false),
        link_ip_pool(&client, "silo2-pool", &silo2.identity.id, false),
    )
    .await;

    // Create users in each silo
    let user1 = create_local_user(
        client,
        &silo1,
        &"user1".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    let user2 = create_local_user(
        client,
        &silo2,
        &"user2".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    // Grant collaborator roles
    grant_iam(
        client,
        &format!("/v1/system/silos/{}", silo1.identity.id),
        SiloRole::Collaborator,
        user1.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    grant_iam(
        client,
        &format!("/v1/system/silos/{}", silo2.identity.id),
        SiloRole::Collaborator,
        user2.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create projects in each silo
    let project1_params = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "silo1-project".parse().unwrap(),
            description: "Project in silo 1".to_string(),
        },
    };
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, "/v1/projects")
            .body(Some(&project1_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user1.id))
    .execute()
    .await
    .unwrap()
    .parsed_body::<views::Project>()
    .unwrap();

    let project2_params = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "silo2-project".parse().unwrap(),
            description: "Project in silo 2".to_string(),
        },
    };
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, "/v1/projects")
            .body(Some(&project2_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user2.id))
    .execute()
    .await
    .unwrap()
    .parsed_body::<views::Project>()
    .unwrap();

    // Create multicast group in silo1 using silo1's pool
    let group1_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "silo1-group".parse().unwrap(),
            description: "Group in silo 1".to_string(),
        },
        multicast_ip: Some(IpAddr::V4(Ipv4Addr::new(224, 0, 3, 100))),
        source_ips: None,
        pool: Some(NameOrId::Name("silo1-pool".parse().unwrap())),
        vpc: None,
    };

    NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/multicast-groups?project=silo1-project",
        )
        .body(Some(&group1_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user1.id))
    .execute()
    .await
    .unwrap()
    .parsed_body::<MulticastGroup>()
    .unwrap();

    // Try to create group in silo2 using silo1's pool - should fail
    let group2_bad_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "silo2-group-bad".parse().unwrap(),
            description: "Group in silo 2 with wrong pool".to_string(),
        },
        multicast_ip: Some(IpAddr::V4(Ipv4Addr::new(224, 0, 3, 101))),
        source_ips: None,
        pool: Some(NameOrId::Name("silo1-pool".parse().unwrap())), // Wrong pool!
        vpc: None,
    };

    let error = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/multicast-groups?project=silo2-project",
        )
        .body(Some(&group2_bad_params))
        .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(user2.id))
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();

    assert_eq!(error.message, "not found: ip-pool with name \"silo1-pool\"");

    // Create group in silo2 using silo2's pool
    let group2_good_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: "silo2-group-good".parse().unwrap(),
            description: "Group in silo 2 with correct pool".to_string(),
        },
        multicast_ip: Some(IpAddr::V4(Ipv4Addr::new(224, 0, 4, 100))),
        source_ips: None,
        pool: Some(NameOrId::Name("silo2-pool".parse().unwrap())),
        vpc: None,
    };

    NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/multicast-groups?project=silo2-project",
        )
        .body(Some(&group2_good_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user2.id))
    .execute()
    .await
    .unwrap()
    .parsed_body::<MulticastGroup>()
    .unwrap();

    // Verify silo1 user cannot see silo2's group
    let list_groups_silo1 = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::GET,
            "/v1/multicast-groups?project=silo1-project",
        )
        .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(user1.id))
    .execute()
    .await
    .unwrap()
    .parsed_body::<oxide_client::types::MulticastGroupResultsPage>()
    .unwrap();

    // Should only see silo1's group
    assert_eq!(list_groups_silo1.items.len(), 1);
    assert_eq!(list_groups_silo1.items[0].name.as_str(), "silo1-group");
}
