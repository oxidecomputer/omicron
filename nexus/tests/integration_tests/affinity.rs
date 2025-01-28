// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests Affinity Groups

use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_instance_with;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::object_create_error;
use nexus_test_utils::resource_helpers::object_delete;
use nexus_test_utils::resource_helpers::object_delete_error;
use nexus_test_utils::resource_helpers::object_get;
use nexus_test_utils::resource_helpers::object_get_error;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views::AffinityGroup;
use nexus_types::external_api::views::Sled;
use nexus_types::external_api::views::SledInstance;
use omicron_common::api::external;
use omicron_common::api::external::AffinityGroupMember;
use std::collections::BTreeSet;
use std::fmt;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Simplifying mechanism for making calls to Nexus' external API
struct ApiHelper<'a> {
    client: &'a ClientTestContext,
}

struct ProjectScopedApiHelper<'a> {
    client: &'a ClientTestContext,
    project: Option<&'a str>,
}

impl<'a> ProjectScopedApiHelper<'a> {
    async fn create_stopped_instance(
        &self,
        instance_name: &str,
    ) -> external::Instance {
        create_instance_with(
            &self.client,
            &self.project.as_ref().expect("Need to specify project name"),
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

    async fn affinity_groups_list(&self) -> Vec<AffinityGroup> {
        let url = groups_url(GroupType::Affinity, self.project);
        objects_list_page_authz(&self.client, &url).await.items
    }

    async fn affinity_group_create(&self, group: &str) -> AffinityGroup {
        let url = groups_url(GroupType::Affinity, self.project);
        let params = affinity_group_create_params(group);
        object_create(&self.client, &url, &params).await
    }

    async fn affinity_group_create_expect_error(
        &self,
        group: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = groups_url(GroupType::Affinity, self.project);
        let params = affinity_group_create_params(group);
        object_create_error(&self.client, &url, &params, status).await
    }

    async fn affinity_group_get(&self, group: &str) -> AffinityGroup {
        let url = group_url(GroupType::Affinity, self.project, group);
        object_get(&self.client, &url).await
    }

    async fn affinity_group_get_expect_error(
        &self,
        group: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = group_url(GroupType::Affinity, self.project, group);
        object_get_error(&self.client, &url, status).await
    }

    async fn affinity_group_members_list(
        &self,
        group: &str,
    ) -> Vec<AffinityGroupMember> {
        let url = group_members_url(GroupType::Affinity, self.project, group);
        objects_list_page_authz(&self.client, &url).await.items
    }

    async fn affinity_group_member_add(
        &self,
        group: &str,
        instance: &str,
    ) -> AffinityGroupMember {
        let url = group_member_instance_url(
            GroupType::Affinity,
            self.project,
            group,
            instance,
        );
        object_create(&self.client, &url, &()).await
    }

    async fn affinity_group_member_add_expect_error(
        &self,
        group: &str,
        instance: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = group_member_instance_url(
            GroupType::Affinity,
            self.project,
            group,
            instance,
        );
        object_create_error(&self.client, &url, &(), status).await
    }

    async fn affinity_group_member_get(
        &self,
        group: &str,
        instance: &str,
    ) -> AffinityGroupMember {
        let url = group_member_instance_url(
            GroupType::Affinity,
            self.project,
            group,
            instance,
        );
        object_get(&self.client, &url).await
    }

    async fn affinity_group_member_get_expect_error(
        &self,
        group: &str,
        instance: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = group_member_instance_url(
            GroupType::Affinity,
            self.project,
            group,
            instance,
        );
        object_get_error(&self.client, &url, status).await
    }

    async fn affinity_group_member_delete(&self, group: &str, instance: &str) {
        let url = group_member_instance_url(
            GroupType::Affinity,
            self.project,
            group,
            instance,
        );
        object_delete(&self.client, &url).await
    }

    async fn affinity_group_member_delete_expect_error(
        &self,
        group: &str,
        instance: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = group_member_instance_url(
            GroupType::Affinity,
            self.project,
            group,
            instance,
        );
        object_delete_error(&self.client, &url, status).await
    }
}

impl<'a> ApiHelper<'a> {
    fn new(client: &'a ClientTestContext) -> Self {
        Self { client }
    }

    fn use_project(&'a self, project: &'a str) -> ProjectScopedApiHelper<'a> {
        ProjectScopedApiHelper { client: self.client, project: Some(project) }
    }

    fn no_project(&'a self) -> ProjectScopedApiHelper<'a> {
        ProjectScopedApiHelper { client: self.client, project: None }
    }

    async fn create_project(&self, name: &str) {
        create_project(&self.client, name).await;
    }

    async fn sleds_list(&self) -> Vec<Sled> {
        let url = "/v1/system/hardware/sleds";
        objects_list_page_authz(&self.client, url).await.items
    }

    async fn sled_instance_list(&self, sled: &str) -> Vec<SledInstance> {
        let url = format!("/v1/system/hardware/sleds/{sled}/instances");
        objects_list_page_authz(&self.client, &url).await.items
    }

    async fn start_instance(
        &self,
        instance: &external::Instance,
    ) -> external::Instance {
        let uri = format!("/v1/instances/{}/start", instance.identity.id);

        NexusRequest::new(
            RequestBuilder::new(&self.client, http::Method::POST, &uri)
                .expect_status(Some(http::StatusCode::ACCEPTED)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap_or_else(|e| {
            panic!("failed to make \"POST\" request to {uri}: {e}")
        })
        .parsed_body()
        .unwrap()
    }
}

fn project_query_param_suffix(project: Option<&str>) -> String {
    if let Some(project) = project {
        format!("?project={project}")
    } else {
        String::new()
    }
}

enum GroupType {
    Affinity,
    AntiAffinity,
}

impl fmt::Display for GroupType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            GroupType::Affinity => "affinity-groups",
            GroupType::AntiAffinity => "anti-affinity-groups",
        };
        write!(f, "{s}")
    }
}

fn groups_url(ty: GroupType, project: Option<&str>) -> String {
    let query_params = project_query_param_suffix(project);
    format!("/v1/{ty}{query_params}")
}

fn group_url(ty: GroupType, project: Option<&str>, group: &str) -> String {
    let query_params = project_query_param_suffix(project);
    format!("/v1/{ty}/{group}{query_params}")
}

fn group_members_url(
    ty: GroupType,
    project: Option<&str>,
    group: &str,
) -> String {
    let query_params = project_query_param_suffix(project);
    format!("/v1/{ty}/{group}/members{query_params}")
}

fn group_member_instance_url(
    ty: GroupType,
    project: Option<&str>,
    group: &str,
    instance: &str,
) -> String {
    let query_params = project_query_param_suffix(project);
    format!("/v1/{ty}/{group}/members/instance/{instance}{query_params}")
}

fn affinity_group_create_params(
    group_name: &str,
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

#[nexus_test(extra_sled_agents = 2)]
async fn test_affinity_group(cptestctx: &ControlPlaneTestContext) {
    let external_client = &cptestctx.external_client;

    const PROJECT_NAME: &'static str = "test-project";
    const GROUP_NAME: &'static str = "group";
    const EXPECTED_SLEDS: usize = 3;
    const INSTANCE_COUNT: usize = EXPECTED_SLEDS;

    let api = ApiHelper::new(external_client);

    // Verify the expected sleds to begin with.
    let sleds = api.sleds_list().await;
    assert_eq!(sleds.len(), EXPECTED_SLEDS);

    // Verify that there are no instances on the sleds.
    for sled in &sleds {
        let sled_id = sled.identity.id.to_string();
        assert!(api.sled_instance_list(&sled_id).await.is_empty());
    }

    // Create an IP pool and project that we'll use for testing.
    create_default_ip_pool(&external_client).await;
    api.create_project(PROJECT_NAME).await;

    let project_api = api.use_project(PROJECT_NAME);

    let mut instances = Vec::new();
    for i in 0..INSTANCE_COUNT {
        instances.push(
            project_api
                .create_stopped_instance(&format!("test-instance-{i}"))
                .await,
        );
    }

    // When we start, we observe no affinity groups
    let groups = project_api.affinity_groups_list().await;
    assert!(groups.is_empty());

    // We can now create a group and observe it
    let group = project_api.affinity_group_create(GROUP_NAME).await;

    // We can list it and also GET the group specifically
    let groups = project_api.affinity_groups_list().await;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].identity.id, group.identity.id);

    let observed_group = project_api.affinity_group_get(GROUP_NAME).await;
    assert_eq!(observed_group.identity.id, group.identity.id);

    // List all members of the affinity group (expect nothing)
    let members = project_api.affinity_group_members_list(GROUP_NAME).await;
    assert!(members.is_empty());

    // Add these instances to an affinity group
    for instance in &instances {
        project_api
            .affinity_group_member_add(
                GROUP_NAME,
                &instance.identity.name.to_string(),
            )
            .await;
    }

    // List members again (expect all instances)
    let members = project_api.affinity_group_members_list(GROUP_NAME).await;
    assert_eq!(members.len(), instances.len());

    // We can also list each member
    for instance in &instances {
        project_api
            .affinity_group_member_get(
                GROUP_NAME,
                instance.identity.name.as_str(),
            )
            .await;
    }

    // Start the instances we created earlier.
    //
    // We don't actually care that they're "running" from the perspective of the
    // simulated sled agent, we just want placement to be triggered from Nexus.
    for instance in &instances {
        api.start_instance(&instance).await;
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
        let observed_instances = api
            .sled_instance_list(&sled.identity.id.to_string())
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
}

#[nexus_test]
async fn test_affinity_group_membership(cptestctx: &ControlPlaneTestContext) {
    let external_client = &cptestctx.external_client;

    const PROJECT_NAME: &'static str = "test-project";
    const GROUP_NAME: &'static str = "group";

    let api = ApiHelper::new(external_client);

    // Create an IP pool and project that we'll use for testing.
    create_default_ip_pool(&external_client).await;
    api.create_project(PROJECT_NAME).await;

    let project_api = api.use_project(PROJECT_NAME);

    let instance =
        project_api.create_stopped_instance(&format!("test-instance")).await;

    // When we start, we observe no affinity groups
    let groups = project_api.affinity_groups_list().await;
    assert!(groups.is_empty());

    // We can now create a group and observe it
    let _group = project_api.affinity_group_create(GROUP_NAME).await;

    // List all members of the affinity group (expect nothing)
    let members = project_api.affinity_group_members_list(GROUP_NAME).await;
    assert!(members.is_empty());

    // Add the instance to the affinity group
    let instance_name = &instance.identity.name.to_string();
    project_api.affinity_group_member_add(GROUP_NAME, &instance_name).await;

    // List members again (expect the instance)
    let members = project_api.affinity_group_members_list(GROUP_NAME).await;
    assert_eq!(members.len(), 1);
    project_api
        .affinity_group_member_get(GROUP_NAME, instance.identity.name.as_str())
        .await;

    // Delete the member, observe that it is gone
    project_api.affinity_group_member_delete(GROUP_NAME, &instance_name).await;
    let members = project_api.affinity_group_members_list(GROUP_NAME).await;
    assert_eq!(members.len(), 0);

    // If we try to get the member, it will 404
    project_api
        .affinity_group_member_get_expect_error(
            GROUP_NAME,
            &instance_name,
            StatusCode::NOT_FOUND,
        )
        .await;
}
