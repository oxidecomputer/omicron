// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests Affinity Groups

use dropshot::test_util::ClientTestContext;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_instance_with;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::object_get;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views::AffinityGroup;
use nexus_types::external_api::views::Sled;
use nexus_types::external_api::views::SledInstance;
use omicron_common::api::external;
use std::collections::BTreeSet;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

async fn sleds_list(client: &ClientTestContext, sleds_url: &str) -> Vec<Sled> {
    objects_list_page_authz::<Sled>(client, sleds_url).await.items
}

async fn sled_instance_list(
    client: &ClientTestContext,
    url: &str,
) -> Vec<SledInstance> {
    objects_list_page_authz::<SledInstance>(client, url).await.items
}

async fn create_stopped_instance(
    client: &ClientTestContext,
    project_name: &str,
    instance_name: &str,
) -> external::Instance {
    create_instance_with(
        client,
        project_name,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::None,
        // Disks=
        Vec::<params::InstanceDiskAttachment>::new(),
        // External IPs=
        Vec::<params::ExternalIpCreate>::new(),
        // Start=
        false,
        // Auto-restart policy=
        None,
    )
    .await
}

async fn start_instance(
    client: &ClientTestContext,
    instance: &external::Instance,
) -> external::Instance {
    let uri = format!("/v1/instances/{}/start", instance.identity.id);

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &uri)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|e| panic!("failed to make \"POST\" request to {uri}: {e}"))
    .parsed_body()
    .unwrap()
}

fn affinity_groups_url(project_name: &str) -> String {
    format!("/v1/affinity-groups?project={project_name}")
}

fn affinity_group_url(project_name: &str, group_name: &str) -> String {
    format!("/v1/affinity-groups/{group_name}?project={project_name}")
}

fn affinity_group_members_url(project_name: &str, group_name: &str) -> String {
    format!("/v1/affinity-groups/{group_name}/members?project={project_name}")
}

fn affinity_group_member_instance_url(
    project_name: &str,
    group_name: &str,
    instance_name: &str,
) -> String {
    format!("/v1/affinity-groups/{group_name}/members/instance/{instance_name}?project={project_name}")
}

fn affinity_group_create_params(
    group_name: &'static str,
) -> params::AffinityGroupCreate {
    params::AffinityGroupCreate {
        identity: external::IdentityMetadataCreateParams {
            name: group_name.parse().unwrap(),
            description: String::from("This is a description"),
        },
        policy: external::AffinityPolicy::Fail,
        failure_domain: external::FailureDomain::Sled,
    }
}

// Affinity group helper API helper methods

async fn affinity_groups_list(
    client: &ClientTestContext,
    url: &str,
) -> Vec<AffinityGroup> {
    objects_list_page_authz::<AffinityGroup>(client, url).await.items
}

async fn affinity_group_create(
    client: &ClientTestContext,
    url: &str,
    params: &params::AffinityGroupCreate,
) -> AffinityGroup {
    object_create::<_, AffinityGroup>(client, url, params).await
}

async fn affinity_group_get(
    client: &ClientTestContext,
    url: &str,
) -> AffinityGroup {
    object_get::<AffinityGroup>(client, url).await
}

async fn affinity_group_members_list(
    client: &ClientTestContext,
    url: &str,
) -> Vec<external::AffinityGroupMember> {
    objects_list_page_authz::<external::AffinityGroupMember>(client, url)
        .await
        .items
}

async fn affinity_group_member_add(
    client: &ClientTestContext,
    project: &str,
    group: &str,
    instance: &str,
) {
    let uri = affinity_group_member_instance_url(project, group, instance);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &uri)
            .expect_status(Some(http::StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|e| {
        panic!("failed to make \"POST\" request to {uri}: {e}")
    });
}

#[nexus_test(extra_sled_agents = 2)]
async fn test_affinity_group(cptestctx: &ControlPlaneTestContext) {
    let external_client = &cptestctx.external_client;

    const PROJECT_NAME: &'static str = "test-project";
    const GROUP_NAME: &'static str = "group";

    // Verify that there are three sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    let sleds = sleds_list(&external_client, &sleds_url).await;
    assert_eq!(sleds.len(), 3);
    const INSTANCE_COUNT: usize = 3;

    // Verify that there are no instances on the sleds.
    for sled in &sleds {
        let sled_id = sled.identity.id;
        let instances_url =
            format!("/v1/system/hardware/sleds/{sled_id}/instances");
        assert!(sled_instance_list(&external_client, &instances_url)
            .await
            .is_empty());
    }

    // Create an IP pool and project that we'll use for testing.
    create_default_ip_pool(&external_client).await;
    let project = create_project(&external_client, PROJECT_NAME).await;

    let mut instances = Vec::new();
    for i in 0..INSTANCE_COUNT {
        instances.push(
            create_stopped_instance(
                &external_client,
                PROJECT_NAME,
                &format!("test-instance-{i}"),
            )
            .await,
        );
    }

    // When we start, we observe no affinity groups
    let groups = affinity_groups_list(
        &external_client,
        &affinity_groups_url(PROJECT_NAME),
    )
    .await;
    assert!(groups.is_empty());

    // We can now create a group and observe it
    let group = affinity_group_create(
        &external_client,
        &affinity_groups_url(PROJECT_NAME),
        &affinity_group_create_params(GROUP_NAME),
    )
    .await;

    let groups = affinity_groups_list(
        &external_client,
        &affinity_groups_url(PROJECT_NAME),
    )
    .await;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].identity.id, group.identity.id);

    let observed_group = affinity_group_get(
        &external_client,
        &affinity_group_url(PROJECT_NAME, GROUP_NAME),
    )
    .await;
    assert_eq!(observed_group.identity.id, group.identity.id);

    // List all members of the affinity group (expect nothing)

    let members = affinity_group_members_list(
        &external_client,
        &affinity_group_members_url(PROJECT_NAME, GROUP_NAME),
    )
    .await;
    assert!(members.is_empty());

    // Add these instances to an affinity group
    for instance in &instances {
        affinity_group_member_add(
            &external_client,
            PROJECT_NAME,
            GROUP_NAME,
            &instance.identity.name.to_string(),
        )
        .await;
    }

    // List members again (expect all instances)
    let members = affinity_group_members_list(
        &external_client,
        &affinity_group_members_url(PROJECT_NAME, GROUP_NAME),
    )
    .await;
    assert_eq!(members.len(), instances.len());

    // Start the instances we created earlier.
    //
    // We don't actually care that they're "running" from the perspective of the
    // simulated sled agent, we just want placement to be triggered from Nexus.
    for instance in &instances {
        start_instance(&external_client, &instance).await;
    }

    // Use a BTreeSet so we can ignore ordering when comparing instance
    // placement.
    let expected_instances = instances
        .iter()
        .map(|instance| instance.identity.id)
        .collect::<BTreeSet<_>>();

    // We expect that all sleds will be empty, except for one, which will have
    // all the instances in our affinity group.
    let mut empty_sleds = 0;
    let mut populated_sleds = 0;
    for sled in &sleds {
        let sled_id = sled.identity.id;
        let instances_url =
            format!("/v1/system/hardware/sleds/{sled_id}/instances");

        let observed_instances =
            sled_instance_list(&external_client, &instances_url)
                .await
                .into_iter()
                .map(|sled_instance| sled_instance.identity.id)
                .collect::<BTreeSet<_>>();

        if !observed_instances.is_empty() {
            assert_eq!(observed_instances, expected_instances);
            populated_sleds += 1;
        } else {
            empty_sleds += 1;
        }
    }
    assert_eq!(populated_sleds, 1);
    assert_eq!(empty_sleds, 2);

    // TODO:
    // - Add members
    // - List members
    // - View members
    // - Remove members
    //
    // - Update groups
    // - Create more groups, remove more groups
    // - Anywhere project is "optional", validate that
    //
    // TODO:
    // - All of htat, but for anti-affinity groups

    // TODO: Start the instance to trigger placement??
}
