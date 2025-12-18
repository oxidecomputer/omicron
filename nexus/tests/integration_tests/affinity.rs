// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests Affinity (and Anti-Affinity) Groups

use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
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
use nexus_test_utils::resource_helpers::object_put;
use nexus_test_utils::resource_helpers::object_put_error;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views::AffinityGroup;
use nexus_types::external_api::views::AntiAffinityGroup;
use nexus_types::external_api::views::Sled;
use nexus_types::external_api::views::SledInstance;
use omicron_common::api::external;
use omicron_common::api::external::AffinityGroupMember;
use omicron_common::api::external::AntiAffinityGroupMember;
use omicron_common::api::external::ObjectIdentity;
use std::collections::BTreeSet;
use std::marker::PhantomData;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Simplifying mechanism for making calls to Nexus' external API
struct ApiHelper<'a> {
    client: &'a ClientTestContext,
}

// This is an extention of the "ApiHelper", with an opinion about:
//
// - What project (if any) is selected
// - Whether or not we're accessing affinity/anti-affinity groups
struct ProjectScopedApiHelper<'a, T> {
    client: &'a ClientTestContext,
    project: Option<&'a str>,
    affinity_type: PhantomData<T>,
}

impl<T: AffinityGroupish> ProjectScopedApiHelper<'_, T> {
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
            // Instance CPU platform=
            None,
            // Multicast groups=
            Vec::new(),
        )
        .await
    }

    async fn instance_groups_list(&self, instance: &str) -> Vec<T::Group> {
        let url = instance_groups_url(T::URL_COMPONENT, instance, self.project);
        objects_list_page_authz(&self.client, &url).await.items
    }

    async fn groups_list(&self) -> Vec<T::Group> {
        let url = groups_url(T::URL_COMPONENT, self.project);
        objects_list_page_authz(&self.client, &url).await.items
    }

    async fn groups_list_expect_error(
        &self,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = groups_url(T::URL_COMPONENT, self.project);
        object_get_error(&self.client, &url, status).await
    }

    async fn group_create(&self, group: &str) -> T::Group {
        let url = groups_url(T::URL_COMPONENT, self.project);
        let params = T::make_create_params(group);
        object_create(&self.client, &url, &params).await
    }

    async fn group_create_expect_error(
        &self,
        group: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = groups_url(T::URL_COMPONENT, self.project);
        let params = T::make_create_params(group);
        object_create_error(&self.client, &url, &params, status).await
    }

    async fn group_get(&self, group: &str) -> T::Group {
        let url = group_url(T::URL_COMPONENT, self.project, group);
        object_get(&self.client, &url).await
    }

    async fn group_get_expect_error(
        &self,
        group: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = group_url(T::URL_COMPONENT, self.project, group);
        object_get_error(&self.client, &url, status).await
    }

    async fn group_update(&self, group: &str) -> T::Group {
        let url = group_url(T::URL_COMPONENT, self.project, group);
        let params = T::make_update_params();
        object_put(&self.client, &url, &params).await
    }

    async fn group_update_expect_error(
        &self,
        group: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = group_url(T::URL_COMPONENT, self.project, group);
        let params = T::make_update_params();
        object_put_error(&self.client, &url, &params, status).await
    }

    async fn group_delete(&self, group: &str) {
        let url = group_url(T::URL_COMPONENT, self.project, group);
        object_delete(&self.client, &url).await
    }

    async fn group_delete_expect_error(
        &self,
        group: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = group_url(T::URL_COMPONENT, self.project, group);
        object_delete_error(&self.client, &url, status).await
    }

    async fn group_members_list(&self, group: &str) -> Vec<T::Member> {
        let url = group_members_url(T::URL_COMPONENT, self.project, group);
        objects_list_page_authz(&self.client, &url).await.items
    }

    async fn group_members_list_expect_error(
        &self,
        group: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = group_members_url(T::URL_COMPONENT, self.project, group);
        object_get_error(&self.client, &url, status).await
    }

    async fn group_member_add(&self, group: &str, instance: &str) -> T::Member {
        let url = group_member_instance_url(
            T::URL_COMPONENT,
            self.project,
            group,
            instance,
        );
        object_create(&self.client, &url, &()).await
    }

    async fn group_member_add_expect_error(
        &self,
        group: &str,
        instance: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = group_member_instance_url(
            T::URL_COMPONENT,
            self.project,
            group,
            instance,
        );
        object_create_error(&self.client, &url, &(), status).await
    }

    async fn group_member_get(&self, group: &str, instance: &str) -> T::Member {
        let url = group_member_instance_url(
            T::URL_COMPONENT,
            self.project,
            group,
            instance,
        );
        object_get(&self.client, &url).await
    }

    async fn group_member_get_expect_error(
        &self,
        group: &str,
        instance: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = group_member_instance_url(
            T::URL_COMPONENT,
            self.project,
            group,
            instance,
        );
        object_get_error(&self.client, &url, status).await
    }

    async fn group_member_delete(&self, group: &str, instance: &str) {
        let url = group_member_instance_url(
            T::URL_COMPONENT,
            self.project,
            group,
            instance,
        );
        object_delete(&self.client, &url).await
    }

    async fn group_member_delete_expect_error(
        &self,
        group: &str,
        instance: &str,
        status: StatusCode,
    ) -> HttpErrorResponseBody {
        let url = group_member_instance_url(
            T::URL_COMPONENT,
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

    fn use_project<T>(
        &'a self,
        project: &'a str,
    ) -> ProjectScopedApiHelper<'a, T> {
        ProjectScopedApiHelper {
            client: self.client,
            project: Some(project),
            affinity_type: PhantomData,
        }
    }

    fn no_project<T>(&'a self) -> ProjectScopedApiHelper<'a, T> {
        ProjectScopedApiHelper {
            client: self.client,
            project: None,
            affinity_type: PhantomData,
        }
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

/// Types and traits used by both affinity and anti-affinity groups.
///
/// Use this trait if you're trying to test something which appilies to both
/// group types. Conversely, if you're trying to test behavior specific to one
/// type or the other, I recommend that you avoid making your tests generic.
trait AffinityGroupish {
    /// The struct used to represent this group.
    ///
    /// Should be the result of GET-ing this group.
    type Group: serde::de::DeserializeOwned + ObjectIdentity;

    /// The struct representing a single member within this group.
    type Member: serde::de::DeserializeOwned;

    /// Parameters that can be used to construct this group as a part of a POST
    /// request.
    type CreateParams: serde::Serialize;

    /// Parameters that can be used to update this group as a part of a PUT
    /// request.
    type UpdateParams: serde::Serialize;

    const URL_COMPONENT: &'static str;
    const RESOURCE_NAME: &'static str;

    fn make_create_params(group_name: &str) -> Self::CreateParams;
    fn make_update_params() -> Self::UpdateParams;
}

// Arbitrary text used to validate PUT calls to groups
const NEW_DESCRIPTION: &'static str = "Updated description";

struct AffinityType;

impl AffinityGroupish for AffinityType {
    type Group = AffinityGroup;
    type Member = AffinityGroupMember;
    type CreateParams = params::AffinityGroupCreate;
    type UpdateParams = params::AffinityGroupUpdate;

    const URL_COMPONENT: &'static str = "affinity-groups";
    const RESOURCE_NAME: &'static str = "affinity-group";

    fn make_create_params(group_name: &str) -> Self::CreateParams {
        params::AffinityGroupCreate {
            identity: external::IdentityMetadataCreateParams {
                name: group_name.parse().unwrap(),
                description: String::from("This is a description"),
            },
            policy: external::AffinityPolicy::Fail,
            failure_domain: external::FailureDomain::Sled,
        }
    }

    fn make_update_params() -> Self::UpdateParams {
        params::AffinityGroupUpdate {
            identity: external::IdentityMetadataUpdateParams {
                name: None,
                description: Some(NEW_DESCRIPTION.to_string()),
            },
        }
    }
}

struct AntiAffinityType;

impl AffinityGroupish for AntiAffinityType {
    type Group = AntiAffinityGroup;
    type Member = AntiAffinityGroupMember;
    type CreateParams = params::AntiAffinityGroupCreate;
    type UpdateParams = params::AntiAffinityGroupUpdate;

    const URL_COMPONENT: &'static str = "anti-affinity-groups";
    const RESOURCE_NAME: &'static str = "anti-affinity-group";

    fn make_create_params(group_name: &str) -> Self::CreateParams {
        params::AntiAffinityGroupCreate {
            identity: external::IdentityMetadataCreateParams {
                name: group_name.parse().unwrap(),
                description: String::from("This is a description"),
            },
            policy: external::AffinityPolicy::Fail,
            failure_domain: external::FailureDomain::Sled,
        }
    }

    fn make_update_params() -> Self::UpdateParams {
        params::AntiAffinityGroupUpdate {
            identity: external::IdentityMetadataUpdateParams {
                name: None,
                description: Some(NEW_DESCRIPTION.to_string()),
            },
        }
    }
}

fn instance_groups_url(
    ty: &str,
    instance: &str,
    project: Option<&str>,
) -> String {
    let query_params = project_query_param_suffix(project);
    format!("/v1/instances/{instance}/{ty}{query_params}")
}

fn groups_url(ty: &str, project: Option<&str>) -> String {
    let query_params = project_query_param_suffix(project);
    format!("/v1/{ty}{query_params}")
}

fn group_url(ty: &str, project: Option<&str>, group: &str) -> String {
    let query_params = project_query_param_suffix(project);
    format!("/v1/{ty}/{group}{query_params}")
}

fn group_members_url(ty: &str, project: Option<&str>, group: &str) -> String {
    let query_params = project_query_param_suffix(project);
    format!("/v1/{ty}/{group}/members{query_params}")
}

fn group_member_instance_url(
    ty: &str,
    project: Option<&str>,
    group: &str,
    instance: &str,
) -> String {
    let query_params = project_query_param_suffix(project);
    format!("/v1/{ty}/{group}/members/instance/{instance}{query_params}")
}

#[nexus_test(extra_sled_agents = 2)]
async fn test_affinity_group_usage(cptestctx: &ControlPlaneTestContext) {
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

    let project_api = api.use_project::<AffinityType>(PROJECT_NAME);

    let mut instances = Vec::new();
    for i in 0..INSTANCE_COUNT {
        instances.push(
            project_api
                .create_stopped_instance(&format!("test-instance-{i}"))
                .await,
        );
    }

    // When we start, we observe no affinity groups
    let groups = project_api.groups_list().await;
    assert!(groups.is_empty());

    // We can now create a group and observe it
    let group = project_api.group_create(GROUP_NAME).await;

    // We can list it and also GET the group specifically
    let groups = project_api.groups_list().await;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].identity.id, group.identity.id);

    let observed_group = project_api.group_get(GROUP_NAME).await;
    assert_eq!(observed_group.identity.id, group.identity.id);

    // List all members of the affinity group (expect nothing)
    let members = project_api.group_members_list(GROUP_NAME).await;
    assert!(members.is_empty());

    // Add these instances to an affinity group
    for instance in &instances {
        project_api
            .group_member_add(GROUP_NAME, &instance.identity.name.to_string())
            .await;
    }

    // List members again (expect all instances)
    let members = project_api.group_members_list(GROUP_NAME).await;
    assert_eq!(members.len(), instances.len());

    // We can also list each member
    for instance in &instances {
        project_api
            .group_member_get(GROUP_NAME, instance.identity.name.as_str())
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

#[nexus_test(extra_sled_agents = 2)]
async fn test_anti_affinity_group_usage(cptestctx: &ControlPlaneTestContext) {
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

    let project_api = api.use_project::<AntiAffinityType>(PROJECT_NAME);

    let mut instances = Vec::new();
    for i in 0..INSTANCE_COUNT {
        instances.push(
            project_api
                .create_stopped_instance(&format!("test-instance-{i}"))
                .await,
        );
    }

    // When we start, we observe no anti-affinity groups
    let groups = project_api.groups_list().await;
    assert!(groups.is_empty());

    // We can now create a group and observe it
    let group = project_api.group_create(GROUP_NAME).await;

    // We can list it and also GET the group specifically
    let groups = project_api.groups_list().await;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].identity.id, group.identity.id);

    let observed_group = project_api.group_get(GROUP_NAME).await;
    assert_eq!(observed_group.identity.id, group.identity.id);

    // List all members of the anti-affinity group (expect nothing)
    let members = project_api.group_members_list(GROUP_NAME).await;
    assert!(members.is_empty());

    // Add these instances to the anti-affinity group
    for instance in &instances {
        project_api
            .group_member_add(GROUP_NAME, &instance.identity.name.to_string())
            .await;
    }

    // List members again (expect all instances)
    let members = project_api.group_members_list(GROUP_NAME).await;
    assert_eq!(members.len(), instances.len());

    // We can also list each member
    for instance in &instances {
        project_api
            .group_member_get(GROUP_NAME, instance.identity.name.as_str())
            .await;
    }

    // Start the instances we created earlier.
    //
    // We don't actually care that they're "running" from the perspective of the
    // simulated sled agent, we just want placement to be triggered from Nexus.
    for instance in &instances {
        api.start_instance(&instance).await;
    }

    let mut expected_instances = instances
        .iter()
        .map(|instance| instance.identity.id)
        .collect::<BTreeSet<_>>();

    // We expect that each sled will have a single instance, as all of the
    // instances will want to be anti-located from each other.
    for sled in &sleds {
        let observed_instances = api
            .sled_instance_list(&sled.identity.id.to_string())
            .await
            .into_iter()
            .map(|sled_instance| sled_instance.identity.id)
            .collect::<Vec<_>>();

        assert_eq!(
            observed_instances.len(),
            1,
            "All instances should be placed on distinct sleds"
        );

        assert!(
            expected_instances.remove(&observed_instances[0]),
            "The instance {} was observed on multiple sleds",
            observed_instances[0]
        );
    }

    assert!(
        expected_instances.is_empty(),
        "Did not find allocations for some instances: {expected_instances:?}"
    );
}

#[nexus_test]
async fn test_affinity_group_crud(cptestctx: &ControlPlaneTestContext) {
    let external_client = &cptestctx.external_client;
    test_group_crud::<AffinityType>(external_client).await;
}

#[nexus_test]
async fn test_anti_affinity_group_crud(cptestctx: &ControlPlaneTestContext) {
    let external_client = &cptestctx.external_client;
    test_group_crud::<AntiAffinityType>(external_client).await;
}

async fn test_group_crud<T: AffinityGroupish>(client: &ClientTestContext) {
    const PROJECT_NAME: &'static str = "test-project";
    const GROUP_NAME: &'static str = "group";

    let api = ApiHelper::new(client);

    // Create an IP pool and project that we'll use for testing.
    create_default_ip_pool(&client).await;
    api.create_project(PROJECT_NAME).await;

    let project_api = api.use_project::<T>(PROJECT_NAME);

    let instance = project_api.create_stopped_instance("test-instance").await;

    // When we start, we observe no affinity groups
    let groups = project_api.groups_list().await;
    assert!(groups.is_empty());

    // We can now create a group and observe it
    project_api.group_create(GROUP_NAME).await;
    let response = project_api
        .group_create_expect_error(GROUP_NAME, StatusCode::BAD_REQUEST)
        .await;
    assert_eq!(
        response.message,
        format!("already exists: {} \"{GROUP_NAME}\"", T::RESOURCE_NAME),
    );

    // We can modify the group itself
    let group = project_api.group_update(GROUP_NAME).await;
    assert_eq!(group.identity().description, NEW_DESCRIPTION);

    // List all members of the affinity group (expect nothing)
    let members = project_api.group_members_list(GROUP_NAME).await;
    assert!(members.is_empty());

    // Add the instance to the affinity group
    let instance_name = &instance.identity.name.to_string();
    project_api.group_member_add(GROUP_NAME, &instance_name).await;
    let response = project_api
        .group_member_add_expect_error(
            GROUP_NAME,
            &instance_name,
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(
        response.message,
        format!(
            "already exists: {}-member \"{}\"",
            T::RESOURCE_NAME,
            instance.identity.id
        ),
    );

    // List members again (expect the instance)
    let members = project_api.group_members_list(GROUP_NAME).await;
    assert_eq!(members.len(), 1);
    project_api
        .group_member_get(GROUP_NAME, instance.identity.name.as_str())
        .await;

    // Delete the member, observe that it is gone
    project_api.group_member_delete(GROUP_NAME, &instance_name).await;
    project_api
        .group_member_delete_expect_error(
            GROUP_NAME,
            &instance_name,
            StatusCode::NOT_FOUND,
        )
        .await;
    let members = project_api.group_members_list(GROUP_NAME).await;
    assert_eq!(members.len(), 0);
    project_api
        .group_member_get_expect_error(
            GROUP_NAME,
            &instance_name,
            StatusCode::NOT_FOUND,
        )
        .await;

    // Delete the group, observe that it is gone
    project_api.group_delete(GROUP_NAME).await;
    project_api
        .group_delete_expect_error(GROUP_NAME, StatusCode::NOT_FOUND)
        .await;
    project_api.group_get_expect_error(GROUP_NAME, StatusCode::NOT_FOUND).await;
    let groups = project_api.groups_list().await;
    assert!(groups.is_empty());
}

#[nexus_test]
async fn test_affinity_instance_group_list(
    cptestctx: &ControlPlaneTestContext,
) {
    let external_client = &cptestctx.external_client;
    test_instance_group_list::<AffinityType>(external_client).await;
}

#[nexus_test]
async fn test_anti_affinity_instance_group_list(
    cptestctx: &ControlPlaneTestContext,
) {
    let external_client = &cptestctx.external_client;
    test_instance_group_list::<AntiAffinityType>(external_client).await;
}

async fn test_instance_group_list<T: AffinityGroupish>(
    client: &ClientTestContext,
) {
    const PROJECT_NAME: &'static str = "test-project";

    let api = ApiHelper::new(client);

    // Create an IP pool and project that we'll use for testing.
    create_default_ip_pool(&client).await;
    api.create_project(PROJECT_NAME).await;

    let project_api = api.use_project::<T>(PROJECT_NAME);

    project_api.create_stopped_instance("test-instance").await;
    let groups = project_api.instance_groups_list("test-instance").await;
    assert!(groups.is_empty(), "New instance should not belong to any groups");

    project_api.group_create("yes-group").await;
    project_api.group_create("no-group").await;

    project_api.group_member_add("yes-group", "test-instance").await;

    let groups = project_api.instance_groups_list("test-instance").await;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].identity().name, "yes-group");
}

#[nexus_test]
async fn test_affinity_group_project_selector(
    cptestctx: &ControlPlaneTestContext,
) {
    let external_client = &cptestctx.external_client;
    test_group_project_selector::<AffinityType>(external_client).await;
}

#[nexus_test]
async fn test_anti_affinity_group_project_selector(
    cptestctx: &ControlPlaneTestContext,
) {
    let external_client = &cptestctx.external_client;
    test_group_project_selector::<AntiAffinityType>(external_client).await;
}

async fn test_group_project_selector<T: AffinityGroupish>(
    client: &ClientTestContext,
) {
    const PROJECT_NAME: &'static str = "test-project";
    const GROUP_NAME: &'static str = "group";

    let api = ApiHelper::new(client);

    // Create an IP pool and project that we'll use for testing.
    create_default_ip_pool(&client).await;
    api.create_project(PROJECT_NAME).await;

    // All requests use the "?project={PROJECT_NAME}" query parameter
    let project_api = api.use_project::<T>(PROJECT_NAME);
    // All requests omit the project query parameter
    let no_project_api = api.no_project::<T>();

    let instance = project_api.create_stopped_instance("test-instance").await;

    // We can only list groups within a project
    no_project_api.groups_list_expect_error(StatusCode::BAD_REQUEST).await;
    let _groups = project_api.groups_list().await;

    // We can only create a group within a project
    no_project_api
        .group_create_expect_error(GROUP_NAME, StatusCode::BAD_REQUEST)
        .await;
    let group = project_api.group_create(GROUP_NAME).await;

    // Once we've created a group, we can access it by:
    //
    // - Project + Group Name, or
    // - No Project + Group ID
    //
    // Other combinations are considered bad requests.
    let group_id = group.identity().id.to_string();

    project_api.group_get(GROUP_NAME).await;
    no_project_api.group_get(&group_id).await;
    project_api
        .group_get_expect_error(&group_id, StatusCode::BAD_REQUEST)
        .await;
    no_project_api
        .group_get_expect_error(GROUP_NAME, StatusCode::BAD_REQUEST)
        .await;

    // Same for listing members
    project_api.group_members_list(GROUP_NAME).await;
    no_project_api.group_members_list(&group_id).await;
    project_api
        .group_members_list_expect_error(&group_id, StatusCode::BAD_REQUEST)
        .await;
    no_project_api
        .group_members_list_expect_error(GROUP_NAME, StatusCode::BAD_REQUEST)
        .await;

    // Same for updating the group
    project_api.group_update(GROUP_NAME).await;
    no_project_api.group_update(&group_id).await;
    project_api
        .group_update_expect_error(&group_id, StatusCode::BAD_REQUEST)
        .await;
    no_project_api
        .group_update_expect_error(GROUP_NAME, StatusCode::BAD_REQUEST)
        .await;

    // Group Members can be added by name or UUID
    let instance_name = instance.identity.name.as_str();
    let instance_id = instance.identity.id.to_string();
    project_api.group_member_add(GROUP_NAME, instance_name).await;
    project_api.group_member_delete(GROUP_NAME, instance_name).await;
    no_project_api.group_member_add(&group_id, &instance_id).await;
    no_project_api.group_member_delete(&group_id, &instance_id).await;

    // Trying to use UUIDs with the project selector is invalid
    project_api
        .group_member_add_expect_error(
            GROUP_NAME,
            &instance_id,
            StatusCode::BAD_REQUEST,
        )
        .await;
    project_api
        .group_member_add_expect_error(
            &group_id,
            instance_name,
            StatusCode::BAD_REQUEST,
        )
        .await;

    // Using any names without the project selector is invalid
    no_project_api
        .group_member_add_expect_error(
            GROUP_NAME,
            &instance_id,
            StatusCode::BAD_REQUEST,
        )
        .await;
    no_project_api
        .group_member_add_expect_error(
            &group_id,
            instance_name,
            StatusCode::BAD_REQUEST,
        )
        .await;
    no_project_api
        .group_member_add_expect_error(
            GROUP_NAME,
            instance_name,
            StatusCode::BAD_REQUEST,
        )
        .await;

    // Group deletion also prevents mixing {project, ID} and {no-project, name}.
    project_api
        .group_delete_expect_error(&group_id, StatusCode::BAD_REQUEST)
        .await;
    no_project_api
        .group_delete_expect_error(GROUP_NAME, StatusCode::BAD_REQUEST)
        .await;
    no_project_api.group_delete(&group_id).await;
}
