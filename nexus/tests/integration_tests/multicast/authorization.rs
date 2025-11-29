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

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::test_params::UserPassword;
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_instance, create_local_user, create_project,
    grant_iam, link_ip_pool, object_create, object_get,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    InstanceCreate, InstanceNetworkInterfaceAttachment,
    MulticastGroupMemberAdd, ProjectCreate, SiloCreate, SiloQuotasCreate,
};
use nexus_types::external_api::shared::{
    ProjectRole, SiloIdentityMode, SiloRole,
};
use nexus_types::external_api::views::{
    MulticastGroup, MulticastGroupMember, Silo,
};
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams, Instance,
    InstanceCpuCount, NameOrId,
};
use omicron_uuid_kinds::SiloUserUuid;

use super::*;

/// Create a multicast group via the member-add implicitly create pattern.
///
/// This creates a project and instance for the user, then adds the instance
/// as a member to the specified group name. Since there's no explicit create
/// endpoint, adding the first member implicitly creates the group.
///
/// Returns the implicitly created multicast group.
async fn create_group_via_member_add(
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

    // Case: Add the instance as a member (implicitly creates the group)
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &member_add_url)
            .body(Some(&member_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_id))
    .execute()
    .await
    .expect("User should be able to add member (implicitly creates group)");

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

/// Test that silo users can attach their own instances to fleet-scoped
/// multicast groups (including groups created by other users or fleet admins).
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

    // User creates group via member-add (implicitly creates the group with first instance)
    let group = create_group_via_member_add(
        client,
        user.id,
        "shared-group",
        "mcast-pool",
    )
    .await;

    wait_for_group_active(client, "shared-group").await;

    // User creates a second instance in a new project to test adding to existing group
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
    .authn_as(AuthnMode::SiloUser(user.id))
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
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/instances?project=second-project",
        )
        .body(Some(&instance_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // User can attach additional instance to existing multicast group
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance.identity.id),
        source_ips: None,
    };
    let member_add_url = mcast_group_member_add_url(
        &group.identity.name.to_string(),
        &member_params.instance,
        "second-project",
    );

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
    link_ip_pool(&client, "default", &silo.identity.id, true).await;
    link_ip_pool(&client, "mcast-pool", &silo.identity.id, false).await;

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

    // Create a regular silo user with NO special roles (not even viewer)
    let reader = create_local_user(
        client,
        &silo,
        &"regular-user".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    // Creator creates a multicast group via member-add
    let group = create_group_via_member_add(
        client,
        creator.id,
        "readable-group",
        "mcast-pool",
    )
    .await;

    // Wait for group to become active
    wait_for_group_active(client, "readable-group").await;

    // Regular silo user (with no Fleet roles) can GET the multicast group
    let get_group_url = mcast_group_url(&group.identity.name.to_string());
    let read_group: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, http::Method::GET, &get_group_url)
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(reader.id))
    .execute()
    .await
    .expect("Silo user should be able to read multicast group")
    .parsed_body()
    .unwrap();

    assert_eq!(read_group.identity.id, group.identity.id);
    assert_eq!(read_group.identity.name, group.identity.name);

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

    // Regular silo user can also lookup group by IP address
    // The main multicast-groups endpoint accepts Name, ID, or IP
    let multicast_ip = group.multicast_ip;
    let ip_lookup_url = format!("/v1/multicast-groups/{multicast_ip}");
    let ip_lookup_group: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, http::Method::GET, &ip_lookup_url)
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(reader.id))
    .execute()
    .await
    .expect("Silo user should be able to lookup group by IP")
    .parsed_body()
    .unwrap();

    assert_eq!(ip_lookup_group.identity.id, group.identity.id);
    assert_eq!(ip_lookup_group.multicast_ip, multicast_ip);
}

/// Test that instances from different projects can attach to the same
/// fleet-scoped multicast group (no cross-project isolation).
#[nexus_test]
async fn test_cross_project_instance_attachment_allowed(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create pools and projects
    let (_, _project1, _project2, _) = ops::join4(
        create_default_ip_pool(&client),
        create_project(client, "project1"),
        create_project(client, "project2"),
        create_multicast_ip_pool(&client, "mcast-pool"),
    )
    .await;

    // Create instances in both projects
    let instance1 = create_instance(client, "project1", "instance1").await;
    let instance2 = create_instance(client, "project2", "instance2").await;

    // First member-add implicitly creates the group
    let member_params1 = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance1.identity.id),
        source_ips: None,
    };
    let member_add_url1 = mcast_group_member_add_url(
        "cross-project-group",
        &member_params1.instance,
        "project1",
    );
    let member1: MulticastGroupMember =
        object_create(client, &member_add_url1, &member_params1).await;

    // Fetch the implicitly created group
    let group: MulticastGroup =
        object_get(client, &mcast_group_url("cross-project-group")).await;

    // Attach instance from project2 to the SAME group - should succeed
    let member_params2 = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance2.identity.id),
        source_ips: None,
    };
    let member_add_url2 = mcast_group_member_add_url(
        &group.identity.name.to_string(),
        &member_params2.instance,
        "project2",
    );
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
    link_ip_pool(&client, "default", &silo.identity.id, true).await;
    link_ip_pool(&client, "mcast-pool", &silo.identity.id, false).await;

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

    // Creator creates a multicast group via member-add
    create_group_via_member_add(client, creator.id, "test-group", "mcast-pool")
        .await;

    // Try to list multicast groups without authentication - should get 401 Unauthorized
    let group_url = "/v1/multicast-groups";
    RequestBuilder::new(client, http::Method::GET, group_url)
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("Expected 401 Unauthorized for unauthenticated list request");
}

/// Verify that unauthenticated users cannot access member operations.
/// This tests that member endpoints (list/add/remove) require authentication.
#[nexus_test]
async fn test_unauthenticated_cannot_access_member_operations(
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

    // Creator creates a multicast group via member-add
    let group = create_group_via_member_add(
        client,
        creator.id,
        "auth-test-group",
        "mcast-pool",
    )
    .await;

    // Create a second project and instance for testing unauthenticated add
    let project = create_project(client, "test-project").await;
    let instance =
        create_instance(client, "test-project", "test-instance").await;

    // Try to LIST members without authentication - should get 401
    let members_url = mcast_group_members_url(&group.identity.name.to_string());
    RequestBuilder::new(client, http::Method::GET, &members_url)
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("Expected 401 Unauthorized for unauthenticated list members request");

    // Try to ADD member without authentication - should get 401
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance.identity.id),
        source_ips: None,
    };
    let member_add_url = mcast_group_member_add_url(
        &group.identity.name.to_string(),
        &member_params.instance,
        project.identity.name.as_str(),
    );
    RequestBuilder::new(client, http::Method::POST, &member_add_url)
        .body(Some(&member_params))
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect(
            "Expected 401 Unauthorized for unauthenticated add member request",
        );

    // Try to REMOVE member without authentication - should get 401
    let member_delete_url = format!(
        "{}/{}?project={}",
        mcast_group_members_url(&group.identity.name.to_string()),
        instance.identity.name,
        project.identity.name.as_str()
    );
    RequestBuilder::new(client, http::Method::DELETE, &member_delete_url)
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .expect("Expected 401 Unauthorized for unauthenticated remove member request");
}

/// Test the asymmetric authorization behavior: unprivileged users CAN list
/// group members even though they don't have access to the member instances.
///
/// This validates that listing members only requires Read permission on the
/// multicast group (fleet-scoped), not permissions on individual instances.
#[nexus_test]
async fn test_unprivileged_users_can_list_group_members(
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

    // Privileged user creates group via member-add (implicitly creates group with first instance)
    let group = create_group_via_member_add(
        client,
        privileged_user.id,
        "asymmetric-test-group",
        "mcast-pool",
    )
    .await;

    // The helper created an instance and added it as a member
    let members_url = mcast_group_members_url(&group.identity.name.to_string());

    // Unprivileged user (who does not have access to the privileged user's project
    // or instances) CAN list the group members - this is the asymmetric authorization
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

    // The helper created an instance with a predictable name
    let instance_name = "asymmetric-test-group-instance";
    let project_name = "asymmetric-test-group-project";

    // Try to ADD the existing instance again (should get 404 because unprivileged user
    // can't see the instance, not 403 which would leak its existence)
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };
    let member_add_url = mcast_group_member_add_url(
        &group.identity.name.to_string(),
        &member_params.instance,
        project_name,
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &member_add_url)
            .body(Some(&member_params))
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(unprivileged_user.id))
    .execute()
    .await
    .expect(
        "Should get 404 when trying to add instance from inaccessible project",
    );

    // Try to REMOVE the instance (should get 404, not 403)
    let member_delete_url = format!(
        "{}/{}?project={}",
        mcast_group_members_url(&group.identity.name.to_string()),
        instance_name,
        project_name
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &member_delete_url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(unprivileged_user.id))
    .execute()
    .await
    .expect("Should get 404 when trying to remove instance from inaccessible project");

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

    // Attempt LEAVE via instance-centric path (by ID) as unprivileged user
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

/// Test that authenticated silo users with ONLY project-level roles (no
/// silo-level roles) can still access multicast groups. This verifies that
/// being an authenticated SiloUser is sufficient - multicast group access does
/// not depend on having any specific silo-level or project-level roles.
///
/// This verifies that project-only users can:
/// - List and read multicast groups (fleet-scoped discovery)
/// - Implicitly create groups via member-add API (group owned by their silo)
/// - Create instances and attach them to groups
#[nexus_test]
async fn test_project_only_users_can_access_multicast_groups(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    // create_default_ip_pool already links "default" pool to the DEFAULT_SILO
    create_default_ip_pool(&client).await;

    // Create multicast pool (already linked to DEFAULT_SILO by helper)
    create_multicast_ip_pool(&client, "mcast-pool").await;

    // Get the DEFAULT silo (same silo as the privileged test user)
    // This ensures that when we create a project using AuthnMode::PrivilegedUser,
    // it will be created in the same silo as our project_user
    use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
    let silo_url = format!("/v1/system/silos/{}", DEFAULT_SILO.identity().name);
    let silo: Silo = object_get(client, &silo_url).await;

    // Create a user with NO silo-level roles (only project-level roles)
    let project_user = create_local_user(
        client,
        &silo,
        &"project-only-user".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    // Create a project using AuthnMode::PrivilegedUser, which creates it in DEFAULT_SILO
    // (the same silo where we created project_user above)
    let project = create_project(client, "project-only").await;

    // Grant ONLY project-level role (Project::Collaborator), NO silo roles
    // Users with project-level roles can work within that project even without
    // silo-level roles, as long as they reference the project by ID
    let project_url = format!("/v1/projects/{}", project.identity.name);
    grant_iam(
        client,
        &project_url,
        ProjectRole::Collaborator,
        project_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a silo collaborator who can create the first group
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

    // Creator creates a multicast group via member-add
    let group = create_group_via_member_add(
        client,
        creator.id,
        "project-user-test",
        "mcast-pool",
    )
    .await;

    // Project-only user CAN LIST multicast groups (no silo roles needed)
    let list_response: dropshot::ResultsPage<MulticastGroup> =
        NexusRequest::object_get(client, "/v1/multicast-groups")
            .authn_as(AuthnMode::SiloUser(project_user.id))
            .execute()
            .await
            .expect("Project-only user should be able to list multicast groups")
            .parsed_body()
            .unwrap();

    let list_groups = list_response.items;

    assert!(
        list_groups.iter().any(|g| g.identity.id == group.identity.id),
        "Project-only user should see multicast groups in list"
    );

    // Project-only user CAN READ individual multicast group
    let get_group_url = mcast_group_url(&group.identity.name.to_string());
    let read_group: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, http::Method::GET, &get_group_url)
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(project_user.id))
    .execute()
    .await
    .expect("Project-only user should be able to read multicast group")
    .parsed_body()
    .unwrap();

    assert_eq!(read_group.identity.id, group.identity.id);

    // Project-only user CAN CREATE a multicast group via member-add
    // They create an instance in their project, then add it as a member
    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "project-user-instance".parse().unwrap(),
            description: "Instance for testing project-only user".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "project-user-instance".parse::<Hostname>().unwrap(),
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
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/instances?project=project-only",
        )
        .body(Some(&instance_params))
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

    // Add instance as member to implicitly create the group
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance.identity.id),
        source_ips: None,
    };
    let member_add_url = mcast_group_member_add_url(
        "created-by-project-user",
        &member_params.instance,
        "project-only",
    );

    NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::POST,
            &member_add_url,
        )
        .body(Some(&member_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(project_user.id))
    .execute()
    .await
    .expect("Project-only user should be able to add member (implicitly creates group)");

    // Fetch the implicitly created group
    let user_created_group: MulticastGroup =
        object_get(client, &mcast_group_url("created-by-project-user")).await;

    assert_eq!(
        user_created_group.identity.name.as_str(),
        "created-by-project-user"
    );

    // Project-only user CAN CREATE a second instance in the project (Project::Collaborator)
    // Must use project ID (not name) since user has no silo-level roles
    let instance_name2 = "project-user-instance-2";
    let instances_url =
        format!("/v1/instances?project={}", project.identity.id);
    let instance_params = InstanceCreate {
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
        network_interfaces: InstanceNetworkInterfaceAttachment::Default,
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
        &instance_params,
    )
    .authn_as(AuthnMode::SiloUser(project_user.id))
    .execute()
    .await
    .expect(
        "Project-only user should be able to create an instance in the project",
    )
    .parsed_body()
    .expect("Should parse created instance");

    // Project-only user CAN ATTACH the instance they own to a fleet-scoped group
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name2.parse().unwrap()),
        source_ips: None,
    };
    let member_add_url = mcast_group_member_add_url(
        &group.identity.name.to_string(),
        &member_params.instance,
        &project.identity.name.to_string(),
    );
    let member: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &member_add_url)
            .body(Some(&member_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(project_user.id))
    .execute()
    .await
    .expect("Project-only user should be able to attach their instance to the group")
    .parsed_body()
    .unwrap();

    // Verify the member was created successfully
    assert_eq!(member.instance_id, instance2.identity.id);
    assert_eq!(member.multicast_group_id, group.identity.id);
}

/// Test that users from different silos can both read multicast groups
/// (fleet-scoped visibility). This validates the core cross-silo multicast use case:
/// multicast groups are discoverable across silo boundaries.
///
/// This test verifies:
/// - Users in different silos can both discover and read the same multicast groups
/// - Groups created by Silo A are visible to Silo B users (and vice versa)
#[nexus_test]
async fn test_silo_admins_cannot_modify_other_silos_groups(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pool(&client).await;

    // Create multicast IP pool (fleet-scoped)
    create_multicast_ip_pool(&client, "mcast-pool").await;

    // Create Silo A (not using default test silo - it has Admin->FleetAdmin mapping)
    // We explicitly create both silos with no fleet role mappings to test the
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
    link_ip_pool(&client, "default", &silo_a.identity.id, true).await;
    link_ip_pool(&client, "mcast-pool", &silo_a.identity.id, false).await;

    // Create Silo B
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
    link_ip_pool(&client, "default", &silo_b.identity.id, true).await;
    link_ip_pool(&client, "mcast-pool", &silo_b.identity.id, false).await;

    // Create silo admin for Silo A
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

    // Create silo admin for Silo B
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

    // Admin A creates a multicast group via member-add (owned by Silo A)
    let group_a = create_group_via_member_add(
        client,
        admin_a.id,
        "group-owned-by-silo-a",
        "mcast-pool",
    )
    .await;

    // Admin B creates a multicast group via member-add (owned by Silo B)
    let group_b = create_group_via_member_add(
        client,
        admin_b.id,
        "group-owned-by-silo-b",
        "mcast-pool",
    )
    .await;

    // Both silo admins CAN READ each other's groups (fleet-scoped visibility)
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
}

/// Test that instances from different silos can attach to the same multicast
/// group when both silos have the multicast pool linked.
///
/// Cross-silo multicast works by linking the same pool to multiple silos.
/// Pool linking is the mechanism of access control: a silo can only use
/// pools that are linked to it.
///
/// This test verifies:
/// - Users in different silos (both linked to the pool) can join the same group
/// - Instances from Silo A can attach to a group
/// - Instances from Silo B can attach to the SAME group
/// - Both members can be listed together in the group membership
#[nexus_test]
async fn test_cross_silo_instance_attachment(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pool(&client).await;

    // Create multicast IP pool (fleet-scoped)
    create_multicast_ip_pool(&client, "mcast-pool").await;

    // Get Silo A (default test silo)
    let silo_a_url = format!("/v1/system/silos/{}", cptestctx.silo_name);
    let silo_a: Silo = object_get(client, &silo_a_url).await;
    link_ip_pool(&client, "default", &silo_a.identity.id, true).await;
    link_ip_pool(&client, "mcast-pool", &silo_a.identity.id, false).await;

    // Create Silo B
    let silo_b_params = SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: "silo-b-cross".parse().unwrap(),
            description: "Second silo for cross-silo instance attachment"
                .to_string(),
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
    link_ip_pool(&client, "default", &silo_b.identity.id, true).await;
    // Link mcast-pool to Silo B as well - cross-silo multicast works by
    // linking the same pool to multiple silos (pool linking = access control)
    link_ip_pool(&client, "mcast-pool", &silo_b.identity.id, false).await;

    // Create user in Silo A
    let user_a = create_local_user(
        client,
        &silo_a,
        &"user-a".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    grant_iam(
        client,
        &silo_a_url,
        SiloRole::Collaborator,
        user_a.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create user in Silo B
    let user_b = create_local_user(
        client,
        &silo_b,
        &"user-b".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    grant_iam(
        client,
        &silo_b_url,
        SiloRole::Collaborator,
        user_b.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // User A creates a project in Silo A
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
    .authn_as(AuthnMode::SiloUser(user_a.id))
    .execute()
    .await
    .unwrap();

    // User B creates a project in Silo B
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
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .unwrap();

    // User A creates instance in Silo A's project
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

    let instance_a: Instance = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/instances?project=project-silo-a",
        )
        .body(Some(&instance_a_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_a.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // User B creates instance in Silo B's project
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

    let instance_b: Instance = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/instances?project=project-silo-b",
        )
        .body(Some(&instance_b_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // User A attaches their instance (from Silo A) to a new group (implicitly creates it)
    let group_name = "cross-silo-group";
    let member_a_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance_a.identity.id),
        source_ips: None,
    };
    let member_add_a_url = mcast_group_member_add_url(
        group_name,
        &member_a_params.instance,
        "project-silo-a",
    );

    let member_a: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &member_add_a_url)
            .body(Some(&member_a_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_a.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Fetch the implicitly created group
    let group: MulticastGroup =
        object_get(client, &mcast_group_url(group_name)).await;

    assert_eq!(member_a.instance_id, instance_a.identity.id);
    assert_eq!(member_a.multicast_group_id, group.identity.id);

    // User B attaches their instance (from Silo B) to the SAME fleet-scoped group
    // This is the key test: cross-silo instance attachment should succeed
    let member_b_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance_b.identity.id),
        source_ips: None,
    };
    let member_add_b_url = mcast_group_member_add_url(
        &group.identity.name.to_string(),
        &member_b_params.instance,
        "project-silo-b",
    );

    let member_b: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &member_add_b_url)
            .body(Some(&member_b_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(member_b.instance_id, instance_b.identity.id);
    assert_eq!(member_b.multicast_group_id, group.identity.id);

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
            .authn_as(AuthnMode::SiloUser(user_a.id))
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    assert_eq!(members_by_a.items.len(), 2, "User A should see both members");

    let members_by_b: dropshot::ResultsPage<MulticastGroupMember> =
        NexusRequest::object_get(client, &members_url)
            .authn_as(AuthnMode::SiloUser(user_b.id))
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    assert_eq!(members_by_b.items.len(), 2, "User B should see both members");

    // Case: Cross-silo IP lookup
    // User B (from Silo B, which has mcast-pool linked) can lookup the group
    // by its IP address. Cross-silo access works because both silos are
    // linked to the same pool.
    let multicast_ip = group.multicast_ip;
    let ip_lookup_url = format!("/v1/multicast-groups/{multicast_ip}");
    let ip_lookup_result: MulticastGroup = NexusRequest::new(
        RequestBuilder::new(client, http::Method::GET, &ip_lookup_url)
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("User B should be able to lookup group by IP (pool linked to silo)")
    .parsed_body()
    .unwrap();

    assert_eq!(
        ip_lookup_result.identity.id, group.identity.id,
        "IP lookup should return the correct group"
    );
    assert_eq!(
        ip_lookup_result.multicast_ip, multicast_ip,
        "IP lookup result should have matching multicast_ip"
    );

    // Case: Cross-silo new group creation
    // User B (from Silo B, which has mcast-pool linked) can create a new
    // multicast group. Pool linking is the mechanism of access control.
    let new_group_name = "user-b-created-group";
    let member_add_new_group_url = format!(
        "{}/members?project=project-silo-b",
        mcast_group_url(new_group_name)
    );
    let new_group_member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance_b.identity.id),
        source_ips: None,
    };

    // This should succeed because mcast-pool is linked to Silo B
    let new_group_member: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::POST,
            &member_add_new_group_url,
        )
        .body(Some(&new_group_member_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("User B should create new group (pool linked to silo)")
    .parsed_body()
    .unwrap();

    assert_eq!(
        new_group_member.instance_id, instance_b.identity.id,
        "New group member should reference User B's instance"
    );

    // Verify the new group was created and is accessible
    let new_group: MulticastGroup =
        object_get(client, &mcast_group_url(new_group_name)).await;
    assert_eq!(
        new_group.identity.name.as_str(),
        new_group_name,
        "New group should have correct name"
    );

    // Remove member from new group (triggers implicit deletion)
    let new_group_member_delete_url = format!(
        "{}/members/{}",
        mcast_group_url(new_group_name),
        instance_b.identity.id
    );
    NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::DELETE,
            &new_group_member_delete_url,
        )
        .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("Should clean up new group member");

    // Re-add User B's instance to the original group for subsequent tests
    let rejoin_member: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &member_add_b_url)
            .body(Some(&member_b_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("User B should rejoin original group")
    .parsed_body()
    .unwrap();

    assert_eq!(rejoin_member.instance_id, instance_b.identity.id);

    // Case: Cross-silo detach

    // User A CANNOT detach User B's instance (404 - can't see Silo B's instance)
    // Using instance ID since we're crossing silo boundaries
    let member_delete_b_by_a_url = format!(
        "{}/{}",
        mcast_group_members_url(&group.identity.name.to_string()),
        instance_b.identity.id,
    );

    NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::DELETE,
            &member_delete_b_by_a_url,
        )
        .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(user_a.id))
    .execute()
    .await
    .expect("User A should get 404 when trying to detach Silo B's instance");

    // User B CAN detach their own instance from the group (even though owned by different silo)
    let member_delete_b_url = format!(
        "{}/{}",
        mcast_group_members_url(&group.identity.name.to_string()),
        instance_b.identity.id,
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &member_delete_b_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("User B should be able to detach their own instance");

    // Verify only User A's instance remains
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
}

/// Test that both member-add endpoints have identical permission behavior
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
    // create_default_ip_pool already links "default" pool to the DEFAULT_SILO
    create_default_ip_pool(&client).await;

    // Create multicast pool (already linked to DEFAULT_SILO by helper)
    create_multicast_ip_pool(&client, "mcast-pool").await;

    // Get the DEFAULT silo (same silo as PrivilegedUser)
    // This ensures that when we create a project using AuthnMode::PrivilegedUser,
    // it will be created in the same silo as our users
    use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
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
    // User B intentionally has NO silo-level roles - they're just a regular user
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

    // User A adds the instance as a member (implicitly creates the group)
    let group_name = "parity-test-group";
    let member_params_create = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance_a.identity.id),
        source_ips: None,
    };
    // When using instance ID, do not provide ?project= parameter (causes 400 Bad Request)
    let member_add_url = mcast_group_member_add_url(
        group_name,
        &member_params_create.instance,
        project_a.identity.name.as_str(),
    );
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &member_add_url)
            .body(Some(&member_params_create))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_a.id))
    .execute()
    .await
    .unwrap();

    // Fetch the implicitly created group
    let group_a: MulticastGroup =
        object_get(client, &mcast_group_url(group_name)).await;

    // Case: Permission enforcement without project access

    // Build URLs for both endpoints
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance_a.identity.id),
        source_ips: None,
    };
    let group_centric_url = mcast_group_member_add_url(
        &group_a.identity.name.to_string(),
        &member_params.instance,
        project_a.identity.name.as_str(),
    );

    let instance_centric_url = format!(
        "/v1/instances/{}/multicast-groups/{}",
        instance_a.identity.id, group_a.identity.id
    );
    let inst_body = serde_json::json!({});

    // User B should get 404 via the group-centric endpoint (no access to instance)
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &group_centric_url)
            .body(Some(&member_params))
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect(
        "User B should get 404 via group-centric endpoint without permission",
    );

    // User B should ALSO get 404 via the instance-centric endpoint (same permission check)
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &instance_centric_url)
            .body(Some(&inst_body))
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("User B should get 404 via instance-centric endpoint without permission");

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

    // Create a second instance in the project (User A still owns it, but User B now has access)
    let instance_b = create_instance(
        client,
        project_a.identity.name.as_str(),
        "parity-test-instance-2",
    )
    .await;

    // User B should now succeed via the group-centric endpoint (has Instance::Modify permission)
    let member_b_params = MulticastGroupMemberAdd {
        instance: NameOrId::Id(instance_b.identity.id),
        source_ips: None,
    };
    let group_centric_url_b = mcast_group_member_add_url(
        &group_a.identity.name.to_string(),
        &member_b_params.instance,
        project_a.identity.name.as_str(),
    );
    let member: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &group_centric_url_b)
            .body(Some(&member_b_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("User B should succeed via group-centric endpoint with permission")
    .parsed_body()
    .unwrap();

    assert_eq!(member.instance_id, instance_b.identity.id);
    assert_eq!(member.multicast_group_id, group_a.identity.id);

    // Create a third instance for testing the instance-centric endpoint
    let instance_c = create_instance(
        client,
        project_a.identity.name.as_str(),
        "parity-test-instance-3",
    )
    .await;

    // User B should ALSO succeed via the instance-centric endpoint (same permission check)
    let instance_centric_url_c = format!(
        "/v1/instances/{}/multicast-groups/{}",
        instance_c.identity.id, group_a.identity.id
    );
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &instance_centric_url_c)
            .body(Some(&inst_body))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect(
        "User B should succeed via instance-centric endpoint with permission",
    );

    // This verifies both endpoints have identical permission behavior:
    // - Without permission: both return 404
    // - With project-level access granted: both succeed with 201 Created
}

/// Test that a silo cannot use a multicast pool that is not linked to it.
///
/// Pool linking is the access control mechanism for multicast. A silo can only
/// use multicast pools that are explicitly linked to it. This test verifies
/// that a user in Silo B cannot join a multicast group when the pool is only
/// linked to Silo A.
#[nexus_test]
async fn test_silo_cannot_use_unlinked_pool(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pool(&client).await;

    // Create multicast IP pool (fleet-scoped)
    create_multicast_ip_pool(&client, "mcast-pool").await;

    // Get Silo A (default test silo) and link pools to it
    let silo_a_url = format!("/v1/system/silos/{}", cptestctx.silo_name);
    let silo_a: Silo = object_get(client, &silo_a_url).await;
    link_ip_pool(&client, "default", &silo_a.identity.id, true).await;
    link_ip_pool(&client, "mcast-pool", &silo_a.identity.id, false).await;

    // Create Silo B (but do not link mcast-pool to it)
    let silo_b_params = SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: "silo-b-unlinked".parse().unwrap(),
            description: "Silo without multicast pool linked".to_string(),
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

    // Link only the default pool to Silo B (not the mcast-pool)
    link_ip_pool(&client, "default", &silo_b.identity.id, true).await;

    // Create user in Silo B
    let user_b = create_local_user(
        client,
        &silo_b,
        &"user-b-unlinked".parse().unwrap(),
        UserPassword::LoginDisallowed,
    )
    .await;

    grant_iam(
        client,
        &silo_b_url,
        SiloRole::Collaborator,
        user_b.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // User B creates a project and instance in Silo B
    let project_params = ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: "project-silo-b".parse().unwrap(),
            description: "Project in Silo B".to_string(),
        },
    };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, "/v1/projects")
            .body(Some(&project_params))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("User B should create project in Silo B");

    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "instance-silo-b".parse().unwrap(),
            description: "Instance in Silo B".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: "instance-silo-b".parse::<Hostname>().unwrap(),
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
        RequestBuilder::new(
            client,
            http::Method::POST,
            "/v1/instances?project=project-silo-b",
        )
        .body(Some(&instance_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("User B should create instance in Silo B");

    // User B tries to join a multicast group - should fail because mcast-pool
    // is not linked to Silo B
    let member_add_url =
        "/v1/multicast-groups/test-group/members?project=project-silo-b";
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name("instance-silo-b".parse().unwrap()),
        source_ips: None,
    };

    let error = NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, member_add_url)
            .body(Some(&member_params))
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::SiloUser(user_b.id))
    .execute()
    .await
    .expect("User B should get 404 when pool is not linked to their silo");

    // Verify the error indicates no pool was found
    let error_body: dropshot::HttpErrorResponseBody =
        error.parsed_body().unwrap();
    assert!(
        error_body.message.contains("pool")
            || error_body.message.contains("multicast"),
        "Error should indicate pool not found, got: {}",
        error_body.message
    );
}
