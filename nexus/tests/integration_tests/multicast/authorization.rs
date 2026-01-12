// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authorization tests for multicast groups.
//!
//! Groups are fleet-scoped. Any authenticated user can list and read them.
//! Member operations require modify permission on the instance being added.
//!
//! Pool linking controls access: a silo can only use pools linked to it.
//! Cross-silo multicast works by linking the same pool to multiple silos.

use http::StatusCode;

use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::test_params::UserPassword;
use nexus_test_utils::resource_helpers::{
    create_default_ip_pools, create_instance, create_local_user,
    create_project, grant_iam, link_ip_pool, object_get,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    InstanceCreate, InstanceMulticastGroupJoin,
    InstanceNetworkInterfaceAttachment, ProjectCreate, SiloCreate,
    SiloQuotasCreate,
};
use nexus_types::external_api::shared::{
    ProjectRole, SiloIdentityMode, SiloRole,
};
use nexus_types::external_api::views::{MulticastGroup, Silo};
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams, Instance,
    InstanceCpuCount,
};
use omicron_uuid_kinds::SiloUserUuid;

use super::*;

/// Create a multicast group via the instance-centric join API.
///
/// This creates a project and instance for the user, then joins the instance
/// to the specified group name. Since there's no explicit create endpoint,
/// joining the first member implicitly creates the group.
///
/// Returns the implicitly created multicast group.
async fn create_group_via_instance_join(
    client: &dropshot::test_util::ClientTestContext,
    user_id: SiloUserUuid,
    group_name: &str,
    _pool_name: &str,
) -> MulticastGroup {
    // Use unique project/instance names based on group name to avoid conflicts
    let project_name = format!("{group_name}-project");
    let instance_name = format!("{group_name}-instance");

    // Case: Create a project as the user
    let project_params = ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: project_name.parse().unwrap(),
            description: format!("Project for {group_name}"),
        },
    };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, "/v1/projects")
            .body(Some(&project_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_id))
    .execute()
    .await
    .expect("User should be able to create project");

    // Case: Create an instance as the user (stopped)
    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: format!("Instance for {group_name}"),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: vec![],
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance_url = format!("/v1/instances?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &instance_url)
            .body(Some(&instance_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_id))
    .execute()
    .await
    .expect("User should be able to create instance");

    // Case: Join the instance to the group (implicitly creates the group)
    // Uses the instance-centric API: PUT /v1/instances/{instance}/multicast-groups/{group}
    let join_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{group_name}?project={project_name}"
    );
    let join_params =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &join_url)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_id))
    .execute()
    .await
    .expect("User should be able to join instance to group (implicitly creates group)");

    // Case: Fetch and return the implicitly created group
    let group_url = mcast_group_url(group_name);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::GET, &group_url)
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(user_id))
    .execute()
    .await
    .expect("User should be able to read implicitly created group")
    .parsed_body()
    .unwrap()
}

/// Test that silo users with various permission levels can interact with
/// multicast groups appropriately.
///
/// This consolidated test verifies:
/// - Silo users can attach their own instances to fleet-scoped multicast groups
/// - Authenticated users can read multicast groups (no Fleet::Viewer required)
/// - Project-only users can access multicast groups in their project
#[nexus_test]
async fn test_silo_user_multicast_permissions(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Common setup: create pools (mcast pool auto-links to DEFAULT_SILO)
    ops::join2(
        create_default_ip_pools(&client),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Get the DEFAULT silo (same silo as PrivilegedUser)
    let silo_url = format!("/v1/system/silos/{}", DEFAULT_SILO.identity().name);
    let silo: Silo = object_get(client, &silo_url).await;

    // Create users with different permission levels:
    // - collaborator_user: Silo Collaborator (can create projects/instances)
    // - reader_user: No special roles (only authenticated)
    // - project_user: Only project-level roles (no silo-level roles)

    let collaborator_user = create_local_user(
        client,
        &silo,
        &"collaborator-user".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    grant_iam(
        client,
        &silo_url,
        SiloRole::Collaborator,
        collaborator_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    let reader_user = create_local_user(
        client,
        &silo,
        &"reader-user".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    let project_user = create_local_user(
        client,
        &silo,
        &"project-only-user".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    // Create a project for project_user (using PrivilegedUser, then grant access)
    let project_only = create_project(client, "project-only").await;
    let project_url = format!("/v1/projects/{}", project_only.identity.name);
    grant_iam(
        client,
        &project_url,
        ProjectRole::Collaborator,
        project_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Case: Silo users can attach instances to multicast groups
    // Collaborator creates group via instance join (implicitly creates group)
    let group = create_group_via_instance_join(
        client,
        collaborator_user.id,
        "shared-group",
        "mcast-pool",
    )
    .await;

    wait_for_group_active(client, "shared-group").await;

    // Collaborator creates a second project and instance to test adding to existing group
    let project_params = ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "second-project".parse().unwrap(),
            description: "Second project for testing".to_string(),
        },
    };
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, "/v1/projects")
            .body(Some(&project_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(collaborator_user.id))
    .execute()
    .await
    .unwrap();

    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "second-instance".parse().unwrap(),
            description: "Second instance for testing".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "second-instance".parse::<Hostname>().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: vec![],
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance2: Instance = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/instances?project=second-project",
        )
        .body(Some(&instance_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(collaborator_user.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // User can attach additional instance to existing multicast group
    let join_url = format!(
        "/v1/instances/{}/multicast-groups/{}?project=second-project",
        instance2.identity.name, group.identity.name
    );
    let join_params =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &join_url)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(collaborator_user.id))
    .execute()
    .await
    .expect("User should be able to attach their instance to the group");

    // Case: Authenticated users can read multicast groups
    // Regular silo user (no Fleet roles) can GET the multicast group
    let get_group_url = mcast_group_url(&group.identity.name.to_string());
    let read_group: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, http::Method::GET, &get_group_url)
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(reader_user.id))
    .execute()
    .await
    .expect("Silo user should be able to read multicast group")
    .parsed_body()
    .unwrap();

    assert_eq!(read_group.identity.id, group.identity.id);
    assert_eq!(read_group.identity.name, group.identity.name);

    // Regular silo user can also list multicast groups
    let list_response: dropshot::ResultsPage<MulticastGroup> =
        NexusRequest::new(
            RequestBuilder::new(
                client,
                http::Method::GET,
                "/v1/multicast-groups",
            )
            .expect_status(Some(StatusCode::OK)),
        )
        .authn_as(AuthnMode::SiloUser(reader_user.id))
        .execute()
        .await
        .expect("Silo user should be able to list multicast groups")
        .parsed_body()
        .unwrap();

    assert!(
        list_response.items.iter().any(|g| g.identity.id == group.identity.id),
        "Multicast group should appear in list for silo user"
    );

    // Regular silo user can also look up group by IP address
    let multicast_ip = group.multicast_ip;
    let ip_lookup_url = format!("/v1/multicast-groups/{multicast_ip}");
    let ip_lookup_group: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, http::Method::GET, &ip_lookup_url)
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(reader_user.id))
    .execute()
    .await
    .expect("Silo user should be able to look up group by IP")
    .parsed_body()
    .unwrap();

    assert_eq!(ip_lookup_group.identity.id, group.identity.id);
    assert_eq!(ip_lookup_group.multicast_ip, multicast_ip);

    // Case: Lookup group by nonexistent IP returns 404
    let nonexistent_ip = "224.99.99.99";
    let nonexistent_ip_url = format!("/v1/multicast-groups/{nonexistent_ip}");
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::GET, &nonexistent_ip_url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(reader_user.id))
    .execute()
    .await
    .expect("Lookup by nonexistent IP should return 404");

    // Case: Project-only users can access multicast groups in their project
    //
    // We sanity-check the group exists using a privileged iterator helper
    // (which always uses PrivilegedUser), then exercise the list endpoint using
    // `project_user` with a single-page GET.
    let list_response: Vec<MulticastGroup> =
        NexusRequest::iter_collection_authn(
            client,
            "/v1/multicast-groups",
            "",
            None,
        )
        .await
        .expect("Should be able to list multicast groups")
        .all_items;

    // Verify the group exists in the full list first
    assert!(
        list_response.iter().any(|g| g.identity.id == group.identity.id),
        "Multicast group should exist in the fleet-wide list"
    );

    // Now verify project_user specifically can access the list endpoint
    let project_user_list: dropshot::ResultsPage<MulticastGroup> =
        NexusRequest::object_get(client, "/v1/multicast-groups")
            .authn_as(AuthnMode::SiloUser(project_user.id))
            .execute()
            .await
            .expect("Project-only user should be able to list multicast groups")
            .parsed_body()
            .unwrap();

    assert!(
        project_user_list
            .items
            .iter()
            .any(|g| g.identity.id == group.identity.id),
        "Project-only user should see multicast groups in list"
    );

    // Project-only user can read individual multicast group
    let read_group_by_project_user: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, http::Method::GET, &get_group_url)
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(project_user.id))
    .execute()
    .await
    .expect("Project-only user should be able to read multicast group")
    .parsed_body()
    .unwrap();

    assert_eq!(read_group_by_project_user.identity.id, group.identity.id);

    // A project-only user can create a multicast group via instance join.
    // They create an instance in their project, then add it as a member.
    let project_instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "project-user-instance".parse().unwrap(),
            description: "Instance for testing project-only user".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "project-user-instance".parse::<Hostname>().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: vec![],
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let project_instance: Instance = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/instances?project=project-only",
        )
        .body(Some(&project_instance_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(project_user.id))
    .execute()
    .await
    .expect(
        "Project-only user should be able to create instance in their project",
    )
    .parsed_body()
    .unwrap();

    // Join instance to implicitly create a new group
    let project_join_url = format!(
        "/v1/instances/{}/multicast-groups/created-by-project-user?project={}",
        project_instance.identity.id, project_only.identity.id
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &project_join_url)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(project_user.id))
    .execute()
    .await
    .expect(
        "Project-only user should be able to join group (implicitly creates group)",
    );

    // Fetch the implicitly created group
    let user_created_group: MulticastGroup =
        object_get(client, &mcast_group_url("created-by-project-user")).await;

    assert_eq!(
        user_created_group.identity.name.as_str(),
        "created-by-project-user"
    );

    // A project-only user can create a second instance and attach to existing group
    let instance_name2 = "project-user-instance-2";
    let instances_url =
        format!("/v1/instances?project={}", project_only.identity.id);
    let instance_params2 = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name2.parse().unwrap(),
            description: "Second instance created by project-only user"
                .to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: instance_name2.parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
        multicast_groups: Vec::new(),
    };
    let instance2: Instance = NexusRequest::objects_post(
        client,
        &instances_url,
        &instance_params2,
    )
    .authn_as(AuthnMode::SiloUser(project_user.id))
    .execute()
    .await
    .expect(
        "Project-only user should be able to create an instance in the project",
    )
    .parsed_body()
    .expect("Should parse created instance");

    // Project-only user can attach the instance they own to a fleet-scoped group
    let join_url2 = format!(
        "/v1/instances/{}/multicast-groups/{}?project={}",
        instance2.identity.id, group.identity.name, project_only.identity.id
    );
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &join_url2)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(project_user.id))
    .execute()
    .await
    .expect(
        "Project-only user should be able to attach their instance to the group",
    );

    // Verify instance is now a member
    let members =
        list_multicast_group_members(client, &group.identity.name.to_string())
            .await;
    assert!(
        members.iter().any(|m| m.instance_id == instance2.identity.id),
        "Instance2 should be a member of the group"
    );
}

/// Verify that unauthenticated users cannot access any multicast API endpoints.
///
/// This consolidated test covers all unauthenticated access scenarios:
/// - List groups (401)
/// - Get single group (401)
/// - List members (401)
/// - Join group (401)
/// - Leave group (401)
#[nexus_test]
async fn test_unauthenticated_access_denied(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pools(&client).await;

    // Get DEFAULT_SILO info (pools are linked to DEFAULT_SILO)
    let silo_url = format!("/v1/system/silos/{}", DEFAULT_SILO.identity().name);
    let silo: Silo = object_get(client, &silo_url).await;

    // Create multicast pool (auto-links to DEFAULT_SILO)
    create_multicast_ip_pool(&client, "mcast-pool").await;

    // Create a collaborator user who can create groups
    let creator = create_local_user(
        client,
        &silo,
        &"creator-user".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    grant_iam(
        client,
        &silo_url,
        SiloRole::Collaborator,
        creator.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Creator creates a multicast group via instance join
    let group = create_group_via_instance_join(
        client,
        creator.id,
        "auth-test-group",
        "mcast-pool",
    )
    .await;

    // Create a second project and instance for testing unauthenticated operations
    create_project(client, "test-project").await;
    let instance =
        create_instance(client, "test-project", "test-instance").await;

    // List groups without authentication - should get 401
    RequestBuilder::new(client, http::Method::GET, "/v1/multicast-groups")
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect(
            "Expected 401 Unauthorized for unauthenticated list groups request",
        );

    // Get single group without authentication - should get 401
    let group_url = mcast_group_url(&group.identity.name.to_string());
    RequestBuilder::new(client, http::Method::GET, &group_url)
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect(
            "Expected 401 Unauthorized for unauthenticated get group request",
        );

    // List members without authentication - should get 401
    let members_url = mcast_group_members_url(&group.identity.name.to_string());
    RequestBuilder::new(client, http::Method::GET, &members_url)
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("Expected 401 Unauthorized for unauthenticated list members request");

    // Join without authentication - should get 401
    // Uses instance-centric API: PUT /v1/instances/{instance}/multicast-groups/{group}
    let join_url = format!(
        "/v1/instances/{}/multicast-groups/{}?project=test-project",
        instance.identity.name, group.identity.name
    );
    let join_params =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };
    RequestBuilder::new(client, http::Method::PUT, &join_url)
        .body(Some(&join_params))
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("Expected 401 Unauthorized for unauthenticated join request");

    // Leave without authentication - should get 401
    // Uses instance-centric API: DELETE /v1/instances/{instance}/multicast-groups/{group}
    RequestBuilder::new(client, http::Method::DELETE, &join_url)
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("Expected 401 Unauthorized for unauthenticated leave request");
}

/// Test the asymmetric authorization behavior: unprivileged users can list
/// group members even though they don't have access to the member instances.
///
/// This validates that listing members only requires Read permission on the
/// multicast group (fleet-scoped), not permissions on individual instances.
#[nexus_test]
async fn test_unprivileged_users_can_list_group_members(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pools(&client).await;

    // Get DEFAULT_SILO info (pools are linked to DEFAULT_SILO)
    let silo_url = format!("/v1/system/silos/{}", DEFAULT_SILO.identity().name);
    let silo: Silo = object_get(client, &silo_url).await;

    // Create multicast pool (auto-links to DEFAULT_SILO)
    create_multicast_ip_pool(&client, "mcast-pool").await;

    // Create two regular silo users
    let privileged_user = create_local_user(
        client,
        &silo,
        &"privileged-user".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    let unprivileged_user = create_local_user(
        client,
        &silo,
        &"unprivileged-user".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    // Grant Silo Collaborator only to privileged user so they can create projects
    grant_iam(
        client,
        &silo_url,
        SiloRole::Collaborator,
        privileged_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Privileged user creates group via instance join (implicitly creates group with first instance)
    let group = create_group_via_instance_join(
        client,
        privileged_user.id,
        "asymmetric-test-group",
        "mcast-pool",
    )
    .await;

    // The helper created an instance and added it as a member
    let members_url = mcast_group_members_url(&group.identity.name.to_string());

    // Unprivileged user (who does not have access to the privileged user's project
    // or instances) can list the group members - this is the asymmetric authorization
    let members_response: dropshot::ResultsPage<MulticastGroupMember> =
        NexusRequest::object_get(client, &members_url)
            .authn_as(AuthnMode::SiloUser(unprivileged_user.id))
            .execute()
            .await
            .expect(
                "Unprivileged user should be able to list group members (asymmetric authorization)",
            )
            .parsed_body()
            .unwrap();

    let members = members_response.items;

    // Verify unprivileged user can see the member that they don't own
    assert_eq!(
        members.len(),
        1,
        "Should see 1 member in the group (even though unprivileged user doesn't own it)"
    );
    assert_eq!(
        members[0].multicast_group_id, group.identity.id,
        "Member should be associated with the correct group"
    );

    // Also verify privileged user can list too (sanity check)
    let privileged_response: dropshot::ResultsPage<MulticastGroupMember> =
        NexusRequest::object_get(client, &members_url)
            .authn_as(AuthnMode::SiloUser(privileged_user.id))
            .execute()
            .await
            .expect("Privileged user should also be able to list members")
            .parsed_body()
            .unwrap();

    let privileged_members = privileged_response.items;
    assert_eq!(privileged_members.len(), 1);
    assert_eq!(privileged_members[0].multicast_group_id, group.identity.id);

    // Unprivileged user should get 404 (not 403) when trying to add/remove
    // instances from inaccessible projects
    let instance_name = "asymmetric-test-group-instance";
    let project_name = "asymmetric-test-group-project";

    // Try to JOIN the existing instance again (should get 404 because unprivileged user
    // can't see the instance, not 403 which would leak its existence)
    // Uses instance-centric API: PUT /v1/instances/{instance}/multicast-groups/{group}
    let join_url = format!(
        "/v1/instances/{}/multicast-groups/{}?project={}",
        instance_name, group.identity.name, project_name
    );
    let join_params =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &join_url)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(unprivileged_user.id))
    .execute()
    .await
    .expect(
        "Should get 404 when trying to join instance from inaccessible project",
    );

    // Try to leave the instance (should get 404, not 403)
    // Uses instance-centric API: DELETE /v1/instances/{instance}/multicast-groups/{group}
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &join_url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(unprivileged_user.id))
    .execute()
    .await
    .expect("Should get 404 when trying to leave instance from inaccessible project");

    // Verify the member still exists (unauthorized operations didn't modify anything)
    let final_members: dropshot::ResultsPage<MulticastGroupMember> =
        NexusRequest::object_get(client, &members_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    assert_eq!(
        final_members.items.len(),
        1,
        "Member should still exist after failed unauthorized operations"
    );

    // Parity: instance-centric endpoint should enforce the same behavior (404)
    // when operating on someone else's instance

    // Use the instance ID from the member list
    let instance_id = privileged_members[0].instance_id;

    // Attempt JOIN via instance-centric path (by ID) as unprivileged user
    let group_id = group.identity.id;
    let inst_join_url_id =
        format!("/v1/instances/{instance_id}/multicast-groups/{group_id}");
    let inst_join_body = serde_json::json!({});
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &inst_join_url_id)
            .body(Some(&inst_join_body))
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(unprivileged_user.id))
    .execute()
    .await
    .expect(
        "Instance-centric join should return 404 for inaccessible instance",
    );

    // Attempt leave via instance-centric path (by ID) as unprivileged user
    let inst_leave_url_id = inst_join_url_id.clone();
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &inst_leave_url_id)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(unprivileged_user.id))
    .execute()
    .await
    .expect(
        "Instance-centric leave should return 404 for inaccessible instance",
    );
}

/// Consolidated test for cross-silo multicast isolation.
///
/// This test verifies the cross-silo authorization boundaries for multicast:
/// - Silo admins can read but not modify other silos' groups
/// - Cross-silo instance attachment works when pools are linked
/// - Silos cannot use unlinked pools
///
/// Setup: Creates 2 silos with users and pools with different linking configurations.
#[nexus_test]
async fn test_cross_silo_multicast_isolation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pools(&client).await;

    // Create multicast IP pool (fleet-scoped)
    create_multicast_ip_pool(&client, "mcast-pool").await;

    // Create Silo A (not using default test silo - it has Admin->FleetAdmin mapping)
    // We explicitly create silos with no fleet role mappings to test the
    // authorization boundary correctly.
    let silo_a_params = SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: "silo-a".parse().unwrap(),
            description: "First silo for cross-silo auth testing".to_string(),
        },
        quotas: SiloQuotasCreate::empty(),
        discoverable: false,
        identity_mode: SiloIdentityMode::LocalOnly,
        admin_group_name: None,
        tls_certificates: vec![],
        mapped_fleet_roles: Default::default(),
    };

    let silo_a: Silo =
        NexusRequest::objects_post(client, "/v1/system/silos", &silo_a_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    let silo_a_url = format!("/v1/system/silos/{}", silo_a.identity.name);
    link_ip_pool(&client, "default-v4", &silo_a.identity.id, true).await;
    link_ip_pool(&client, "mcast-pool", &silo_a.identity.id, false).await;

    // Create Silo B - linked to mcast-pool (for cross-silo attachment tests)
    let silo_b_params = SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: "silo-b".parse().unwrap(),
            description: "Second silo for cross-silo auth testing".to_string(),
        },
        quotas: SiloQuotasCreate::empty(),
        discoverable: false,
        identity_mode: SiloIdentityMode::LocalOnly,
        admin_group_name: None,
        tls_certificates: vec![],
        mapped_fleet_roles: Default::default(),
    };

    let silo_b: Silo =
        NexusRequest::objects_post(client, "/v1/system/silos", &silo_b_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    let silo_b_url = format!("/v1/system/silos/{}", silo_b.identity.name);
    link_ip_pool(&client, "default-v4", &silo_b.identity.id, true).await;
    // Link mcast-pool to Silo B as well - cross-silo multicast works by
    // linking the same pool to multiple silos (pool linking = access control)
    link_ip_pool(&client, "mcast-pool", &silo_b.identity.id, false).await;

    // Create Silo C - not linked to mcast-pool (for unlinked pool tests)
    let silo_c_params = SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: "silo-c-unlinked".parse().unwrap(),
            description: "Silo without multicast pool linked".to_string(),
        },
        quotas: SiloQuotasCreate::empty(),
        discoverable: false,
        identity_mode: SiloIdentityMode::LocalOnly,
        admin_group_name: None,
        tls_certificates: vec![],
        mapped_fleet_roles: Default::default(),
    };

    let silo_c: Silo =
        NexusRequest::objects_post(client, "/v1/system/silos", &silo_c_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    let silo_c_url = format!("/v1/system/silos/{}", silo_c.identity.name);
    // Only link the default pool to Silo C (not the mcast-pool)
    link_ip_pool(&client, "default-v4", &silo_c.identity.id, true).await;

    // Create admin for Silo A
    let admin_a = create_local_user(
        client,
        &silo_a,
        &"admin-a".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    grant_iam(
        client,
        &silo_a_url,
        SiloRole::Admin,
        admin_a.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create admin for Silo B
    let admin_b = create_local_user(
        client,
        &silo_b,
        &"admin-b".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    grant_iam(
        client,
        &silo_b_url,
        SiloRole::Admin,
        admin_b.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create user for Silo C (unlinked pool tests)
    let user_c = create_local_user(
        client,
        &silo_c,
        &"user-c".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    grant_iam(
        client,
        &silo_c_url,
        SiloRole::Collaborator,
        user_c.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Case: Silo admin cannot modify other silo's groups
    //
    // Admin A creates a multicast group via instance join (owned by Silo A)
    let group_a = create_group_via_instance_join(
        client,
        admin_a.id,
        "group-owned-by-silo-a",
        "mcast-pool",
    )
    .await;

    // Admin B creates a multicast group via instance join (owned by Silo B)
    let group_b = create_group_via_instance_join(
        client,
        admin_b.id,
        "group-owned-by-silo-b",
        "mcast-pool",
    )
    .await;

    // Both silo admins can read each other's groups (fleet-scoped visibility)
    let read_b_by_a: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::GET,
            &mcast_group_url(&group_b.identity.name.to_string()),
        )
        .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(admin_a.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(read_b_by_a.identity.id, group_b.identity.id);

    let read_a_by_b: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::GET,
            &mcast_group_url(&group_a.identity.name.to_string()),
        )
        .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(admin_b.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(read_a_by_b.identity.id, group_a.identity.id);

    // Case: Cross-silo instance attachment behavior
    //
    // Admin A creates a project and instance in Silo A
    let project_a_params = ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "project-silo-a".parse().unwrap(),
            description: "Project in Silo A".to_string(),
        },
    };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, "/v1/projects")
            .body(Some(&project_a_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(admin_a.id))
    .execute()
    .await
    .unwrap();

    // Admin B creates a project in Silo B
    let project_b_params = ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "project-silo-b".parse().unwrap(),
            description: "Project in Silo B".to_string(),
        },
    };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, "/v1/projects")
            .body(Some(&project_b_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(admin_b.id))
    .execute()
    .await
    .unwrap();

    // Admin A creates instance in Silo A's project
    let instance_a_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "instance-silo-a".parse().unwrap(),
            description: "Instance in Silo A".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "instance-silo-a".parse::<Hostname>().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: vec![],
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance_a: Instance = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/instances?project=project-silo-a",
        )
        .body(Some(&instance_a_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(admin_a.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Admin B creates instance in Silo B's project
    let instance_b_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "instance-silo-b".parse().unwrap(),
            description: "Instance in Silo B".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "instance-silo-b".parse::<Hostname>().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: vec![],
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance_b: Instance = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/instances?project=project-silo-b",
        )
        .body(Some(&instance_b_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(admin_b.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Admin A joins their instance (from Silo A) to a new group (implicitly creates it)
    let group_name = "cross-silo-group";
    let join_url_a = format!(
        "/v1/instances/{}/multicast-groups/{}?project=project-silo-a",
        instance_a.identity.id, group_name
    );
    let join_params =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &join_url_a)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(admin_a.id))
    .execute()
    .await
    .expect("Admin A should be able to join instance to group");

    // Fetch the implicitly created group
    let group: MulticastGroup =
        object_get(client, &mcast_group_url(group_name)).await;

    // Admin B joins their instance (from Silo B) to the same fleet-scoped group
    // This is the key test: cross-silo instance attachment should succeed
    let join_url_b = format!(
        "/v1/instances/{}/multicast-groups/{}?project=project-silo-b",
        instance_b.identity.id, group.identity.name
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &join_url_b)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(admin_b.id))
    .execute()
    .await
    .expect("Admin B should be able to join instance to the same group");

    // Both instances should be visible in the group's member list
    let members_url = mcast_group_members_url(&group.identity.name.to_string());
    let members_response: dropshot::ResultsPage<MulticastGroupMember> =
        NexusRequest::object_get(client, &members_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    let members = members_response.items;

    assert_eq!(members.len(), 2, "Should have 2 members (one from each silo)");

    // Verify both instances are in the member list
    let instance_ids: Vec<_> = members.iter().map(|m| m.instance_id).collect();
    assert!(
        instance_ids.contains(&instance_a.identity.id),
        "Instance from Silo A should be in member list"
    );
    assert!(
        instance_ids.contains(&instance_b.identity.id),
        "Instance from Silo B should be in member list"
    );

    // Both users should be able to see the complete member list (both silos linked)
    let members_by_a: dropshot::ResultsPage<MulticastGroupMember> =
        NexusRequest::object_get(client, &members_url)
            .authn_as(AuthnMode::SiloUser(admin_a.id))
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    assert_eq!(members_by_a.items.len(), 2, "Admin A should see both members");

    let members_by_b: dropshot::ResultsPage<MulticastGroupMember> =
        NexusRequest::object_get(client, &members_url)
            .authn_as(AuthnMode::SiloUser(admin_b.id))
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    assert_eq!(members_by_b.items.len(), 2, "Admin B should see both members");

    // Admin A cannot detach Admin B's instance (404 - can't see Silo B's instance)
    let member_delete_b_by_a_url = format!(
        "/v1/instances/{}/multicast-groups/{}",
        instance_b.identity.id, group.identity.name,
    );

    NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::DELETE,
            &member_delete_b_by_a_url,
        )
        .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(admin_a.id))
    .execute()
    .await
    .expect("Admin A should get 404 when trying to detach Silo B's instance");

    // Admin B can detach their own instance from the group
    let member_delete_b_url = format!(
        "/v1/instances/{}/multicast-groups/{}",
        instance_b.identity.id, group.identity.name,
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &member_delete_b_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::SiloUser(admin_b.id))
    .execute()
    .await
    .expect("Admin B should be able to detach their own instance");

    // Verify only Admin A's instance remains
    let members_after_detach: dropshot::ResultsPage<MulticastGroupMember> =
        NexusRequest::object_get(client, &members_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    assert_eq!(
        members_after_detach.items.len(),
        1,
        "Should have 1 member after Silo B's instance detached"
    );
    assert_eq!(
        members_after_detach.items[0].instance_id, instance_a.identity.id,
        "Remaining member should be Silo A's instance"
    );

    // Case: Silo cannot use unlinked pool
    //
    // User C creates a project and instance in Silo C (which has no mcast-pool link)
    let project_c_params = ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "project-silo-c".parse().unwrap(),
            description: "Project in Silo C".to_string(),
        },
    };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, "/v1/projects")
            .body(Some(&project_c_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_c.id))
    .execute()
    .await
    .expect("User C should create project in Silo C");

    let instance_c_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "instance-silo-c".parse().unwrap(),
            description: "Instance in Silo C".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "instance-silo-c".parse::<Hostname>().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
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
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/instances?project=project-silo-c",
        )
        .body(Some(&instance_c_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_c.id))
    .execute()
    .await
    .expect("User C should create instance in Silo C");

    // User C tries to join a multicast group - should fail because mcast-pool
    // is not linked to Silo C
    let join_url_c = "/v1/instances/instance-silo-c/multicast-groups/test-group?project=project-silo-c";

    let error = NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, join_url_c)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::SiloUser(user_c.id))
    .execute()
    .await
    .expect("User C should get 400 when no pool is linked to their silo");

    // Verify the error indicates no pool was found
    let error_body: dropshot::HttpErrorResponseBody =
        error.parsed_body().unwrap();
    assert_eq!(
        error_body.error_code,
        Some("InvalidRequest".to_string()),
        "Expected InvalidRequest for no pool available, got: {:?}",
        error_body.error_code
    );
}

/// Test that both instance join endpoints have identical permission behavior
/// when project-level IAM grants are used.
///
/// This verifies that:
/// - `/v1/multicast-groups/{group}/members` (group-centric)
/// - `/v1/instances/{instance}/multicast-groups/{group}` (instance-centric)
///
/// Both enforce the same Instance::Modify permission check and return 404 (not 403)
/// when a user without permission tries to add an instance, and both succeed when
/// the user is granted project-level access.
#[nexus_test]
async fn test_both_member_endpoints_have_same_permissions(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    // create_default_ip_pools already links "default" pool to the DEFAULT_SILO
    create_default_ip_pools(&client).await;

    // Create multicast pool (already linked to DEFAULT_SILO by helper)
    create_multicast_ip_pool(&client, "mcast-pool").await;

    // Get the DEFAULT silo (same silo as PrivilegedUser)
    // This ensures that when we create a project using AuthnMode::PrivilegedUser,
    // it will be created in the same silo as our users
    let silo_url = format!("/v1/system/silos/{}", DEFAULT_SILO.identity().name);
    let silo: Silo = object_get(client, &silo_url).await;

    // Create User A (project owner) in the default silo
    let user_a = create_local_user(
        client,
        &silo,
        &"user-a".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    grant_iam(
        client,
        &silo_url,
        SiloRole::Collaborator,
        user_a.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create User B (unprivileged) in the same silo
    // User B intentionally has no silo-level roles - they're just a regular user
    let user_b = create_local_user(
        client,
        &silo,
        &"user-b".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    // Create the project as PrivilegedUser so we can grant IAM on it
    let project_a = create_project(client, "parity-test-project").await;

    // Grant User A access to the project
    let project_a_url = format!("/v1/projects/{}", project_a.identity.name);
    grant_iam(
        client,
        &project_a_url,
        ProjectRole::Collaborator,
        user_a.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // User A creates an instance in the project
    let instance_a_name = "parity-test-instance";
    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_a_name.parse().unwrap(),
            description: "Instance for parity test".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: instance_a_name.parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
        external_ips: vec![],
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: false,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };
    let instance_url =
        format!("/v1/instances?project={}", project_a.identity.name);
    let instance_a: Instance = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &instance_url)
            .body(Some(&instance_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_a.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // User A joins their instance to a group (implicitly creates the group)
    let group_name = "parity-test-group";
    let join_url_a = format!(
        "/v1/instances/{}/multicast-groups/{}?project={}",
        instance_a.identity.id, group_name, project_a.identity.name
    );
    let join_params =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &join_url_a)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_a.id))
    .execute()
    .await
    .expect("User A should be able to join group");

    // Fetch the implicitly created group
    let group_a: MulticastGroup =
        object_get(client, &mcast_group_url(group_name)).await;

    // Case: Permission enforcement without project access

    // User B should get 404 via the instance-centric endpoint (no access to instance)
    let instance_centric_url = format!(
        "/v1/instances/{}/multicast-groups/{}",
        instance_a.identity.id, group_a.identity.id
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &instance_centric_url)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("User B should get 404 without permission to access instance");

    // Case: Permission enforcement with project-level access

    // Grant User B project-level Collaborator access to User A's project
    // Use PrivilegedUser to grant IAM (requires Project::ModifyPolicy permission)
    let project_a_url = format!("/v1/projects/{}", project_a.identity.name);
    grant_iam(
        client,
        &project_a_url,
        ProjectRole::Collaborator,
        user_b.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a second instance in the project
    // (User A still owns it, but User B now has access)
    let instance_b = create_instance(
        client,
        project_a.identity.name.as_str(),
        "parity-test-instance-2",
    )
    .await;

    // User B should now succeed via the instance-centric endpoint
    // (has Instance::Modify permission)
    let join_url_b = format!(
        "/v1/instances/{}/multicast-groups/{}?project={}",
        instance_b.identity.id, group_a.identity.name, project_a.identity.name
    );
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &join_url_b)
            .body(Some(&join_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("User B should succeed with project-level access");

    // Verify the member was created
    let members = list_multicast_group_members(client, group_name).await;
    assert!(
        members.iter().any(|m| m.instance_id == instance_b.identity.id),
        "Instance B should be a member of the group"
    );

    // Create a third instance for testing the instance-centric endpoint
    let instance_c = create_instance(
        client,
        project_a.identity.name.as_str(),
        "parity-test-instance-3",
    )
    .await;

    // User B should also succeed via the instance-centric endpoint
    // (same permission check)
    let instance_centric_url_c = format!(
        "/v1/instances/{}/multicast-groups/{}",
        instance_c.identity.id, group_a.identity.id
    );
    let join_body =
        InstanceMulticastGroupJoin { source_ips: None, ip_version: None };
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &instance_centric_url_c)
            .body(Some(&join_body))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect(
        "User B should succeed via instance-centric endpoint with permission",
    );
}
