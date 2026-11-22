// TODO-RAINCLAUDE: Generated stub impl of the API trait. It exists so that
// this crate, rather than omicron-nexus, instantiates the context-independent
// extractor and serde code for every endpoint.
#![allow(unused_variables)]
use super::*;

pub struct AnchorStub;

impl NexusExternalApi for AnchorStub {
    type Context = ();

    async fn system_policy_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<latest::policy::Policy<latest::policy::FleetRole>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_policy_update(
        rqctx: RequestContext<Self::Context>,
        new_policy: TypedBody<
            latest::policy::Policy<latest::policy::FleetRole>,
        >,
    ) -> Result<
        HttpResponseOk<latest::policy::Policy<latest::policy::FleetRole>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_networking_settings_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<latest::system_networking::SystemNetworkingSettings>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_networking_settings_update(
        rqctx: RequestContext<Self::Context>,
        new_settings: TypedBody<
            latest::system_networking::SystemNetworkingSettingsUpdate,
        >,
    ) -> Result<
        HttpResponseOk<latest::system_networking::SystemNetworkingSettings>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn policy_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<latest::policy::Policy<latest::policy::SiloRole>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn policy_update(
        rqctx: RequestContext<Self::Context>,
        new_policy: TypedBody<latest::policy::Policy<latest::policy::SiloRole>>,
    ) -> Result<
        HttpResponseOk<latest::policy::Policy<latest::policy::SiloRole>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn auth_settings_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::silo::SiloAuthSettings>, HttpError> {
        unimplemented!()
    }

    async fn auth_settings_update(
        rqctx: RequestContext<Self::Context>,
        new_settings: TypedBody<latest::silo::SiloAuthSettingsUpdate>,
    ) -> Result<HttpResponseOk<latest::silo::SiloAuthSettings>, HttpError> {
        unimplemented!()
    }

    async fn utilization_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::silo::Utilization>, HttpError> {
        unimplemented!()
    }

    async fn silo_utilization_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SiloPath>,
    ) -> Result<HttpResponseOk<latest::silo::SiloUtilization>, HttpError> {
        unimplemented!()
    }

    async fn silo_utilization_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::silo::SiloUtilization>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_quotas_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::silo::SiloQuotas>>, HttpError> {
        unimplemented!()
    }

    async fn silo_quotas_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SiloPath>,
    ) -> Result<HttpResponseOk<latest::silo::SiloQuotas>, HttpError> {
        unimplemented!()
    }

    async fn silo_quotas_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SiloPath>,
        new_quota: TypedBody<latest::silo::SiloQuotasUpdate>,
    ) -> Result<HttpResponseOk<latest::silo::SiloQuotas>, HttpError> {
        unimplemented!()
    }

    async fn silo_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::silo::Silo>>, HttpError> {
        unimplemented!()
    }

    async fn silo_create(
        rqctx: RequestContext<Self::Context>,
        new_silo_params: TypedBody<latest::silo::SiloCreate>,
    ) -> Result<HttpResponseCreated<latest::silo::Silo>, HttpError> {
        unimplemented!()
    }

    async fn silo_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SiloPath>,
    ) -> Result<HttpResponseOk<latest::silo::Silo>, HttpError> {
        unimplemented!()
    }

    async fn silo_ip_pool_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SiloPath>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::ip_pool::SiloIpPool>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn silo_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SiloPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn silo_policy_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SiloPath>,
    ) -> Result<
        HttpResponseOk<latest::policy::Policy<latest::policy::SiloRole>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn silo_policy_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SiloPath>,
        new_policy: TypedBody<latest::policy::Policy<latest::policy::SiloRole>>,
    ) -> Result<
        HttpResponseOk<latest::policy::Policy<latest::policy::SiloRole>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn silo_user_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById<latest::silo::SiloSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::user::User>>, HttpError> {
        unimplemented!()
    }

    async fn silo_user_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::user::UserParam>,
        query_params: Query<latest::silo::SiloSelector>,
    ) -> Result<HttpResponseOk<latest::user::User>, HttpError> {
        unimplemented!()
    }

    async fn silo_identity_provider_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<latest::silo::SiloSelector>>,
    ) -> Result<
        HttpResponseOk<
            ResultsPage<latest::identity_provider::IdentityProvider>,
        >,
        HttpError,
    > {
        unimplemented!()
    }

    async fn saml_identity_provider_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::silo::SiloSelector>,
        new_provider: TypedBody<
            latest::identity_provider::SamlIdentityProviderCreate,
        >,
    ) -> Result<
        HttpResponseCreated<latest::identity_provider::SamlIdentityProvider>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn saml_identity_provider_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::ProviderPath>,
        query_params: Query<latest::silo::OptionalSiloSelector>,
    ) -> Result<
        HttpResponseOk<latest::identity_provider::SamlIdentityProvider>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn local_idp_user_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::silo::SiloSelector>,
        new_user_params: TypedBody<latest::user::UserCreate>,
    ) -> Result<HttpResponseCreated<latest::user::User>, HttpError> {
        unimplemented!()
    }

    async fn local_idp_user_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::user::UserParam>,
        query_params: Query<latest::silo::SiloSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn local_idp_user_set_password(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::user::UserParam>,
        query_params: Query<latest::path_params::SiloPath>,
        update: TypedBody<latest::user::UserPassword>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn scim_token_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::silo::SiloSelector>,
    ) -> Result<
        HttpResponseOk<Vec<latest::scim::ScimClientBearerToken>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn scim_token_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::silo::SiloSelector>,
    ) -> Result<
        HttpResponseCreated<latest::scim::ScimClientBearerTokenValue>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn scim_token_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::scim::ScimV2TokenPathParam>,
        query_params: Query<latest::silo::SiloSelector>,
    ) -> Result<HttpResponseOk<latest::scim::ScimClientBearerToken>, HttpError> {
        unimplemented!()
    }

    async fn scim_token_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::scim::ScimV2TokenPathParam>,
        query_params: Query<latest::silo::SiloSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_list_users(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<scim2_rs::QueryParams>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_get_user(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::scim::ScimV2UserPathParam>,
        query_params: Query<scim2_rs::QueryParams>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_create_user(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<scim2_rs::CreateUserRequest>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_put_user(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::scim::ScimV2UserPathParam>,
        body: TypedBody<scim2_rs::CreateUserRequest>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_patch_user(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::scim::ScimV2UserPathParam>,
        body: TypedBody<scim2_rs::PatchRequest>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_delete_user(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::scim::ScimV2UserPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_list_groups(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<scim2_rs::QueryParams>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_get_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::scim::ScimV2GroupPathParam>,
        query_params: Query<scim2_rs::QueryParams>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_create_group(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<scim2_rs::CreateGroupRequest>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_put_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::scim::ScimV2GroupPathParam>,
        body: TypedBody<scim2_rs::CreateGroupRequest>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_patch_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::scim::ScimV2GroupPathParam>,
        body: TypedBody<scim2_rs::PatchRequest>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn scim_v2_delete_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::scim::ScimV2GroupPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn project_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::project::Project>>, HttpError> {
        unimplemented!()
    }

    async fn project_create(
        rqctx: RequestContext<Self::Context>,
        new_project: TypedBody<latest::project::ProjectCreate>,
    ) -> Result<HttpResponseCreated<latest::project::Project>, HttpError> {
        unimplemented!()
    }

    async fn project_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::ProjectPath>,
    ) -> Result<HttpResponseOk<latest::project::Project>, HttpError> {
        unimplemented!()
    }

    async fn project_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::ProjectPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn project_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::ProjectPath>,
        updated_project: TypedBody<latest::project::ProjectUpdate>,
    ) -> Result<HttpResponseOk<latest::project::Project>, HttpError> {
        unimplemented!()
    }

    async fn project_policy_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::ProjectPath>,
    ) -> Result<
        HttpResponseOk<latest::policy::Policy<latest::policy::ProjectRole>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn project_policy_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::ProjectPath>,
        new_policy: TypedBody<
            latest::policy::Policy<latest::policy::ProjectRole>,
        >,
    ) -> Result<
        HttpResponseOk<latest::policy::Policy<latest::policy::ProjectRole>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn ip_pool_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<latest::ip_pool::IpPoolFilter>>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::ip_pool::SiloIpPool>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn project_ip_pool_list_v2026_01_01_00(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<v2026_01_01_00::ip_pool::SiloIpPool>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn ip_pool_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
    ) -> Result<HttpResponseOk<latest::ip_pool::SiloIpPool>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::ip_pool::SystemIpPoolFilter>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<latest::ip_pool::IpPool>>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_list_v2026_02_09_00(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<v2025_11_20_00::ip_pool::IpPool>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_ip_pool_create(
        rqctx: RequestContext<Self::Context>,
        pool_params: TypedBody<latest::ip_pool::IpPoolCreate>,
    ) -> Result<HttpResponseCreated<latest::ip_pool::IpPool>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
    ) -> Result<HttpResponseOk<latest::ip_pool::IpPool>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
        updates: TypedBody<latest::ip_pool::IpPoolUpdate>,
    ) -> Result<HttpResponseOk<latest::ip_pool::IpPool>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_utilization_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
    ) -> Result<HttpResponseOk<latest::ip_pool::IpPoolUtilization>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_silo_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
        // paginating by resource_id because they're unique per pool. most robust
        // option would be to paginate by a composite key representing the (pool,
        // resource_type, resource)
        query_params: Query<PaginatedById>,
        // TODO: this could just list latest::silo::Silo -- it's not like knowing silo_id
        // and nothing else is particularly useful -- except we also want to say
        // whether the pool is marked default on each silo. So one option would
        // be  to do the same as we did with SiloIpPool -- include is_default on
        // whatever the thing is. Still... all we'd have to do to make this usable
        // in both places would be to make it { ...IpPool, silo_id, silo_name,
        // is_default }
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::ip_pool::IpPoolSiloLink>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_ip_pool_silo_link(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
        resource_assoc: TypedBody<latest::ip_pool::IpPoolLinkSilo>,
    ) -> Result<HttpResponseCreated<latest::ip_pool::IpPoolSiloLink>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_silo_unlink(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::ip_pool::IpPoolSiloPath>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_silo_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::ip_pool::IpPoolSiloPath>,
        update: TypedBody<latest::ip_pool::IpPoolSiloUpdate>,
    ) -> Result<HttpResponseOk<latest::ip_pool::IpPoolSiloLink>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_assign(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
        assign_params: TypedBody<latest::ip_pool::IpPoolAssignParam>,
    ) -> Result<HttpResponseOk<latest::ip_pool::IpPool>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_service_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v2025_11_20_00::ip_pool::IpPool>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_range_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
        query_params: Query<IpPoolRangePaginationParams>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::ip_pool::IpPoolRange>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_ip_pool_range_add(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
        range_params: TypedBody<IpRange>,
    ) -> Result<HttpResponseCreated<latest::ip_pool::IpPoolRange>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_range_remove(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
        range_params: TypedBody<IpRange>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_service_range_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<IpPoolRangePaginationParams>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::ip_pool::IpPoolRange>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_ip_pool_service_range_add(
        rqctx: RequestContext<Self::Context>,
        range_params: TypedBody<IpRange>,
    ) -> Result<HttpResponseCreated<latest::ip_pool::IpPoolRange>, HttpError> {
        unimplemented!()
    }

    async fn system_ip_pool_service_range_remove(
        rqctx: RequestContext<Self::Context>,
        range_params: TypedBody<IpRange>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn system_subnet_pool_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::subnet_pool::SubnetPool>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_subnet_pool_create(
        rqctx: RequestContext<Self::Context>,
        pool_params: TypedBody<latest::subnet_pool::SubnetPoolCreate>,
    ) -> Result<HttpResponseCreated<latest::subnet_pool::SubnetPool>, HttpError> {
        unimplemented!()
    }

    async fn system_subnet_pool_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolPath>,
    ) -> Result<HttpResponseOk<latest::subnet_pool::SubnetPool>, HttpError> {
        unimplemented!()
    }

    async fn system_subnet_pool_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolPath>,
        updates: TypedBody<latest::subnet_pool::SubnetPoolUpdate>,
    ) -> Result<HttpResponseOk<latest::subnet_pool::SubnetPool>, HttpError> {
        unimplemented!()
    }

    async fn system_subnet_pool_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn system_subnet_pool_member_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolPath>,
        query_params: Query<SubnetPoolMemberPaginationParams>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::subnet_pool::SubnetPoolMember>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_subnet_pool_member_add(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolPath>,
        subnet_params: TypedBody<latest::subnet_pool::SubnetPoolMemberAdd>,
    ) -> Result<
        HttpResponseCreated<latest::subnet_pool::SubnetPoolMember>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_subnet_pool_member_remove(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolPath>,
        subnet_params: TypedBody<latest::subnet_pool::SubnetPoolMemberRemove>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn system_subnet_pool_silo_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::subnet_pool::SubnetPoolSiloLink>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn silo_subnet_pool_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SiloPath>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::subnet_pool::SiloSubnetPool>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn subnet_pool_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::subnet_pool::SiloSubnetPool>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn subnet_pool_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolPath>,
    ) -> Result<HttpResponseOk<latest::subnet_pool::SiloSubnetPool>, HttpError> {
        unimplemented!()
    }

    async fn system_subnet_pool_silo_link(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolPath>,
        silo_link: TypedBody<latest::subnet_pool::SubnetPoolLinkSilo>,
    ) -> Result<
        HttpResponseCreated<latest::subnet_pool::SubnetPoolSiloLink>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_subnet_pool_silo_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolSiloPath>,
        update: TypedBody<latest::subnet_pool::SubnetPoolSiloUpdate>,
    ) -> Result<
        HttpResponseOk<latest::subnet_pool::SubnetPoolSiloLink>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_subnet_pool_silo_unlink(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolSiloPath>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn system_subnet_pool_utilization_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::subnet_pool::SubnetPoolPath>,
    ) -> Result<
        HttpResponseOk<latest::subnet_pool::SubnetPoolUtilization>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn external_subnet_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::ProjectSelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::external_subnet::ExternalSubnet>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn external_subnet_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::ProjectSelector>,
        subnet_params: TypedBody<latest::external_subnet::ExternalSubnetCreate>,
    ) -> Result<
        HttpResponseCreated<latest::external_subnet::ExternalSubnet>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn external_subnet_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::external_subnet::ExternalSubnetPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<
        HttpResponseOk<latest::external_subnet::ExternalSubnet>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn external_subnet_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::external_subnet::ExternalSubnetPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        subnet_params: TypedBody<latest::external_subnet::ExternalSubnetUpdate>,
    ) -> Result<
        HttpResponseOk<latest::external_subnet::ExternalSubnet>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn external_subnet_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::external_subnet::ExternalSubnetPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn external_subnet_attach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::external_subnet::ExternalSubnetPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        attach_params: TypedBody<latest::external_subnet::ExternalSubnetAttach>,
    ) -> Result<
        HttpResponseAccepted<latest::external_subnet::ExternalSubnet>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn external_subnet_detach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::external_subnet::ExternalSubnetPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<
        HttpResponseAccepted<latest::external_subnet::ExternalSubnet>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn floating_ip_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::ProjectSelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::floating_ip::FloatingIp>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn floating_ip_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::ProjectSelector>,
        floating_params: TypedBody<latest::floating_ip::FloatingIpCreate>,
    ) -> Result<HttpResponseCreated<latest::floating_ip::FloatingIp>, HttpError> {
        unimplemented!()
    }

    async fn floating_ip_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::FloatingIpPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        updated_floating_ip: TypedBody<latest::floating_ip::FloatingIpUpdate>,
    ) -> Result<HttpResponseOk<latest::floating_ip::FloatingIp>, HttpError> {
        unimplemented!()
    }

    async fn floating_ip_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::FloatingIpPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn floating_ip_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::FloatingIpPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<latest::floating_ip::FloatingIp>, HttpError> {
        unimplemented!()
    }

    async fn floating_ip_attach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::FloatingIpPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        target: TypedBody<latest::floating_ip::FloatingIpAttach>,
    ) -> Result<HttpResponseAccepted<latest::floating_ip::FloatingIp>, HttpError> {
        unimplemented!()
    }

    async fn floating_ip_detach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::FloatingIpPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseAccepted<latest::floating_ip::FloatingIp>, HttpError> {
        unimplemented!()
    }

    async fn multicast_group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::multicast::MulticastGroup>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn multicast_group_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::multicast::MulticastGroupPath>,
    ) -> Result<HttpResponseOk<latest::multicast::MulticastGroup>, HttpError> {
        unimplemented!()
    }

    async fn multicast_group_member_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::multicast::MulticastGroupPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::multicast::MulticastGroupMember>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn disk_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::ProjectSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError> {
        unimplemented!()
    }

    async fn disk_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::ProjectSelector>,
        new_disk: TypedBody<latest::disk::DiskCreate>,
    ) -> Result<HttpResponseCreated<Disk>, HttpError> {
        unimplemented!()
    }

    async fn disk_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::DiskPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<Disk>, HttpError> {
        unimplemented!()
    }

    async fn disk_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::DiskPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn disk_bulk_write_import_start(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::DiskPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn disk_bulk_write_import(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::DiskPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        import_params: TypedBody<latest::disk::ImportBlocksBulkWrite>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn disk_bulk_write_import_stop(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::DiskPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn disk_finalize_import(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::DiskPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        finalize_params: TypedBody<latest::disk::FinalizeDisk>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn instance_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::ProjectSelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::instance::Instance>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn instance_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::ProjectSelector>,
        new_instance: TypedBody<latest::instance::InstanceCreate>,
    ) -> Result<HttpResponseCreated<latest::instance::Instance>, HttpError> {
        unimplemented!()
    }

    async fn instance_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::InstancePath>,
    ) -> Result<HttpResponseOk<latest::instance::Instance>, HttpError> {
        unimplemented!()
    }

    async fn instance_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::InstancePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn instance_update(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::InstancePath>,
        instance_config: TypedBody<latest::instance::InstanceUpdate>,
    ) -> Result<HttpResponseOk<latest::instance::Instance>, HttpError> {
        unimplemented!()
    }

    async fn instance_reboot(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::instance::InstanceStopOrRebootParams>,
        path_params: Path<latest::path_params::InstancePath>,
    ) -> Result<HttpResponseAccepted<latest::instance::Instance>, HttpError> {
        unimplemented!()
    }

    async fn instance_start(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::InstancePath>,
    ) -> Result<HttpResponseAccepted<latest::instance::Instance>, HttpError> {
        unimplemented!()
    }

    async fn instance_stop(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::instance::InstanceStopOrRebootParams>,
        path_params: Path<latest::path_params::InstancePath>,
    ) -> Result<HttpResponseAccepted<latest::instance::Instance>, HttpError> {
        unimplemented!()
    }

    async fn instance_serial_console(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::InstancePath>,
        query_params: Query<latest::instance::InstanceSerialConsoleRequest>,
    ) -> Result<
        HttpResponseOk<latest::instance::InstanceSerialConsoleData>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn instance_serial_console_stream(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::InstancePath>,
        query_params: Query<
            latest::instance::InstanceSerialConsoleStreamRequest,
        >,
        conn: WebsocketConnection,
    ) -> WebsocketChannelResult {
        unimplemented!()
    }

    async fn instance_ssh_public_key_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::InstancePath>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::OptionalProjectSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<latest::ssh_key::SshKey>>, HttpError> {
        unimplemented!()
    }

    async fn instance_disk_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::OptionalProjectSelector>,
        >,
        path_params: Path<latest::path_params::InstancePath>,
    ) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError> {
        unimplemented!()
    }

    async fn instance_disk_attach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::InstancePath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        disk_to_attach: TypedBody<latest::path_params::DiskPath>,
    ) -> Result<HttpResponseAccepted<Disk>, HttpError> {
        unimplemented!()
    }

    async fn instance_disk_detach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::InstancePath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        disk_to_detach: TypedBody<latest::path_params::DiskPath>,
    ) -> Result<HttpResponseAccepted<Disk>, HttpError> {
        unimplemented!()
    }

    async fn instance_affinity_group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::OptionalProjectSelector>,
        >,
        path_params: Path<latest::path_params::InstancePath>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::affinity::AffinityGroup>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn instance_anti_affinity_group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::OptionalProjectSelector>,
        >,
        path_params: Path<latest::path_params::InstancePath>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::affinity::AntiAffinityGroup>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn affinity_group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::ProjectSelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::affinity::AffinityGroup>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn affinity_group_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::AffinityGroupPath>,
    ) -> Result<HttpResponseOk<latest::affinity::AffinityGroup>, HttpError> {
        unimplemented!()
    }

    async fn affinity_group_member_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::OptionalProjectSelector>,
        >,
        path_params: Path<latest::path_params::AffinityGroupPath>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::affinity::AffinityGroupMember>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn affinity_group_member_instance_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::affinity::AffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseOk<latest::affinity::AffinityGroupMember>, HttpError> {
        unimplemented!()
    }

    async fn affinity_group_member_instance_add(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::affinity::AffinityInstanceGroupMemberPath>,
    ) -> Result<
        HttpResponseCreated<latest::affinity::AffinityGroupMember>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn affinity_group_member_instance_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::affinity::AffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn affinity_group_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::ProjectSelector>,
        new_affinity_group_params: TypedBody<
            latest::affinity::AffinityGroupCreate,
        >,
    ) -> Result<HttpResponseCreated<latest::affinity::AffinityGroup>, HttpError> {
        unimplemented!()
    }

    async fn affinity_group_update(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::AffinityGroupPath>,
        updated_group: TypedBody<latest::affinity::AffinityGroupUpdate>,
    ) -> Result<HttpResponseOk<latest::affinity::AffinityGroup>, HttpError> {
        unimplemented!()
    }

    async fn affinity_group_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::AffinityGroupPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn anti_affinity_group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::ProjectSelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::affinity::AntiAffinityGroup>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn anti_affinity_group_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::AntiAffinityGroupPath>,
    ) -> Result<HttpResponseOk<latest::affinity::AntiAffinityGroup>, HttpError> {
        unimplemented!()
    }

    async fn anti_affinity_group_member_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::OptionalProjectSelector>,
        >,
        path_params: Path<latest::path_params::AntiAffinityGroupPath>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::affinity::AntiAffinityGroupMember>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn anti_affinity_group_member_instance_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<
            latest::affinity::AntiAffinityInstanceGroupMemberPath,
        >,
    ) -> Result<
        HttpResponseOk<latest::affinity::AntiAffinityGroupMember>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn anti_affinity_group_member_instance_add(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<
            latest::affinity::AntiAffinityInstanceGroupMemberPath,
        >,
    ) -> Result<
        HttpResponseCreated<latest::affinity::AntiAffinityGroupMember>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn anti_affinity_group_member_instance_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<
            latest::affinity::AntiAffinityInstanceGroupMemberPath,
        >,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn anti_affinity_group_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::ProjectSelector>,
        new_affinity_group_params: TypedBody<
            latest::affinity::AntiAffinityGroupCreate,
        >,
    ) -> Result<
        HttpResponseCreated<latest::affinity::AntiAffinityGroup>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn anti_affinity_group_update(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::AntiAffinityGroupPath>,
        updated_group: TypedBody<latest::affinity::AntiAffinityGroupUpdate>,
    ) -> Result<HttpResponseOk<latest::affinity::AntiAffinityGroup>, HttpError> {
        unimplemented!()
    }

    async fn anti_affinity_group_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::AntiAffinityGroupPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn certificate_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::certificate::Certificate>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn certificate_create(
        rqctx: RequestContext<Self::Context>,
        new_cert: TypedBody<latest::certificate::CertificateCreate>,
    ) -> Result<HttpResponseCreated<latest::certificate::Certificate>, HttpError> {
        unimplemented!()
    }

    async fn certificate_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::CertificatePath>,
    ) -> Result<HttpResponseOk<latest::certificate::Certificate>, HttpError> {
        unimplemented!()
    }

    async fn certificate_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::CertificatePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn silo_certificate_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<latest::silo::SiloSelector>>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::certificate::Certificate>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn silo_certificate_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::silo::SiloSelector>,
        new_cert: TypedBody<latest::certificate::CertificateCreate>,
    ) -> Result<HttpResponseCreated<latest::certificate::Certificate>, HttpError> {
        unimplemented!()
    }

    async fn silo_certificate_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::CertificatePath>,
        query_params: Query<latest::silo::OptionalSiloSelector>,
    ) -> Result<HttpResponseOk<latest::certificate::Certificate>, HttpError> {
        unimplemented!()
    }

    async fn silo_certificate_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::CertificatePath>,
        query_params: Query<latest::silo::OptionalSiloSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn networking_address_lot_create(
        rqctx: RequestContext<Self::Context>,
        new_address_lot: TypedBody<latest::networking::AddressLotCreate>,
    ) -> Result<
        HttpResponseCreated<latest::networking::AddressLotCreateResponse>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_address_lot_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::AddressLotPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn networking_address_lot_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::networking::AddressLot>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_address_lot_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::AddressLotPath>,
    ) -> Result<
        HttpResponseOk<latest::networking::AddressLotViewResponse>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_address_lot_block_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::AddressLotPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::networking::AddressLotBlock>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_loopback_address_create(
        rqctx: RequestContext<Self::Context>,
        new_loopback_address: TypedBody<
            latest::networking::LoopbackAddressCreate,
        >,
    ) -> Result<
        HttpResponseCreated<latest::networking::LoopbackAddress>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_loopback_address_delete(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::networking::LoopbackAddressPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn networking_loopback_address_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::networking::LoopbackAddress>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_switch_port_settings_create(
        rqctx: RequestContext<Self::Context>,
        new_settings: TypedBody<latest::networking::SwitchPortSettingsCreate>,
    ) -> Result<
        HttpResponseCreated<latest::networking::SwitchPortSettings>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_switch_port_settings_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::networking::SwitchPortSettingsSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn networking_switch_port_settings_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::networking::SwitchPortSettingsSelector>,
        >,
    ) -> Result<
        HttpResponseOk<
            ResultsPage<latest::networking::SwitchPortSettingsIdentity>,
        >,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_switch_port_settings_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::networking::SwitchPortSettingsInfoSelector>,
    ) -> Result<HttpResponseOk<latest::networking::SwitchPortSettings>, HttpError> {
        unimplemented!()
    }

    async fn networking_switch_port_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedById<latest::networking::SwitchPortPageSelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::networking::SwitchPort>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_switch_port_status(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::networking::SwitchPortPathSelector>,
        query_params: Query<latest::networking::SwitchPortSelector>,
    ) -> Result<HttpResponseOk<latest::switch::SwitchLinkState>, HttpError> {
        unimplemented!()
    }

    async fn networking_switch_port_apply_settings(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::networking::SwitchPortPathSelector>,
        query_params: Query<latest::networking::SwitchPortSelector>,
        settings_body: TypedBody<latest::networking::SwitchPortApplySettings>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn networking_switch_port_clear_settings(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::networking::SwitchPortPathSelector>,
        query_params: Query<latest::networking::SwitchPortSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn networking_switch_port_lldp_config_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::networking::SwitchPortPathSelector>,
        query_params: Query<latest::networking::SwitchPortSelector>,
    ) -> Result<HttpResponseOk<latest::networking::LldpLinkConfig>, HttpError> {
        unimplemented!()
    }

    async fn networking_switch_port_lldp_config_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::networking::SwitchPortPathSelector>,
        query_params: Query<latest::networking::SwitchPortSelector>,
        config: TypedBody<latest::networking::LldpLinkConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn networking_switch_port_lldp_neighbors(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::networking::LldpPortPathSelector>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::networking::LldpNeighbor>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_bgp_config_create(
        rqctx: RequestContext<Self::Context>,
        config: TypedBody<latest::networking::BgpConfigCreate>,
    ) -> Result<HttpResponseCreated<latest::networking::BgpConfig>, HttpError> {
        unimplemented!()
    }

    async fn networking_bgp_config_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::networking::BgpConfig>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_bgp_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<latest::networking::BgpPeerStatus>>, HttpError> {
        unimplemented!()
    }

    async fn networking_bgp_exported(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<latest::networking::BgpExported>>, HttpError> {
        unimplemented!()
    }

    async fn networking_bgp_message_history(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::networking::BgpRouteSelector>,
    ) -> Result<
        HttpResponseOk<latest::networking::AggregateBgpMessageHistory>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_bgp_imported_routes_ipv4(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::networking::BgpRouteSelector>,
    ) -> Result<
        HttpResponseOk<Vec<v2025_11_20_00::networking::BgpImportedRouteIpv4>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_bgp_imported(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::networking::BgpRouteSelector>,
    ) -> Result<HttpResponseOk<Vec<latest::networking::BgpImported>>, HttpError> {
        unimplemented!()
    }

    async fn networking_bgp_config_delete(
        rqctx: RequestContext<Self::Context>,
        sel: Query<latest::networking::BgpConfigSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn networking_bgp_config_update(
        rqctx: RequestContext<Self::Context>,
        sel: Query<latest::networking::BgpConfigSelector>,
        update: TypedBody<latest::networking::BgpConfigUpdate>,
    ) -> Result<HttpResponseOk<latest::networking::BgpConfig>, HttpError> {
        unimplemented!()
    }

    async fn networking_bgp_announce_set_update(
        rqctx: RequestContext<Self::Context>,
        config: TypedBody<latest::networking::BgpAnnounceSetCreate>,
    ) -> Result<HttpResponseOk<latest::networking::BgpAnnounceSet>, HttpError> {
        unimplemented!()
    }

    async fn networking_bgp_announce_set_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<Vec<latest::networking::BgpAnnounceSet>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_bgp_announce_set_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::networking::BgpAnnounceSetSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn networking_bgp_announcement_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::networking::BgpAnnounceSetSelector>,
    ) -> Result<
        HttpResponseOk<Vec<latest::networking::BgpAnnouncement>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn networking_bfd_enable(
        rqctx: RequestContext<Self::Context>,
        session: TypedBody<latest::networking::BfdSessionEnable>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn networking_bfd_disable(
        rqctx: RequestContext<Self::Context>,
        session: TypedBody<latest::networking::BfdSessionDisable>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn networking_bfd_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<latest::bfd::BfdStatus>>, HttpError> {
        unimplemented!()
    }

    async fn networking_allow_list_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::system::AllowList>, HttpError> {
        unimplemented!()
    }

    async fn networking_allow_list_update(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::system::AllowListUpdate>,
    ) -> Result<HttpResponseOk<latest::system::AllowList>, HttpError> {
        unimplemented!()
    }

    async fn networking_inbound_icmp_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ServiceIcmpConfig>, HttpError> {
        unimplemented!()
    }

    async fn networking_inbound_icmp_update(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<ServiceIcmpConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn image_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::OptionalProjectSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<latest::image::Image>>, HttpError> {
        unimplemented!()
    }

    async fn image_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        new_image: TypedBody<latest::image::ImageCreate>,
    ) -> Result<HttpResponseCreated<latest::image::Image>, HttpError> {
        unimplemented!()
    }

    async fn image_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::ImagePath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<latest::image::Image>, HttpError> {
        unimplemented!()
    }

    async fn image_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::ImagePath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn image_promote(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::ImagePath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseAccepted<latest::image::Image>, HttpError> {
        unimplemented!()
    }

    async fn image_demote(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::ImagePath>,
        query_params: Query<latest::project::ProjectSelector>,
    ) -> Result<HttpResponseAccepted<latest::image::Image>, HttpError> {
        unimplemented!()
    }

    async fn instance_network_interface_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::instance::InstanceSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<InstanceNetworkInterface>>, HttpError> {
        unimplemented!()
    }

    async fn instance_network_interface_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::instance::InstanceSelector>,
        interface_params: TypedBody<
            latest::instance::InstanceNetworkInterfaceCreate,
        >,
    ) -> Result<HttpResponseCreated<InstanceNetworkInterface>, HttpError> {
        unimplemented!()
    }

    async fn instance_network_interface_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::NetworkInterfacePath>,
        query_params: Query<latest::instance::OptionalInstanceSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn instance_network_interface_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::NetworkInterfacePath>,
        query_params: Query<latest::instance::OptionalInstanceSelector>,
    ) -> Result<HttpResponseOk<InstanceNetworkInterface>, HttpError> {
        unimplemented!()
    }

    async fn instance_network_interface_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::NetworkInterfacePath>,
        query_params: Query<latest::instance::OptionalInstanceSelector>,
        updated_iface: TypedBody<
            latest::instance::InstanceNetworkInterfaceUpdate,
        >,
    ) -> Result<HttpResponseOk<InstanceNetworkInterface>, HttpError> {
        unimplemented!()
    }

    async fn instance_external_ip_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::InstancePath>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::external_ip::ExternalIp>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn instance_ephemeral_ip_attach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::InstancePath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        ip_to_create: TypedBody<latest::instance::EphemeralIpCreate>,
    ) -> Result<HttpResponseAccepted<latest::external_ip::ExternalIp>, HttpError> {
        unimplemented!()
    }

    async fn instance_ephemeral_ip_detach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::InstancePath>,
        query_params: Query<latest::instance::EphemeralIpDetachSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn instance_external_subnet_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        path_params: Path<latest::path_params::InstancePath>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::external_subnet::ExternalSubnet>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn instance_multicast_group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedById<latest::project::OptionalProjectSelector>,
        >,
        path_params: Path<latest::path_params::InstancePath>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::multicast::MulticastGroupMember>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn instance_multicast_group_join(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::multicast::InstanceMulticastGroupPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        body_params: TypedBody<latest::multicast::InstanceMulticastGroupJoin>,
    ) -> Result<
        HttpResponseCreated<latest::multicast::MulticastGroupMember>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn instance_multicast_group_join_v2025_11_20_00(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<
            v2025_11_20_00::multicast::InstanceMulticastGroupPath,
        >,
        query_params: Query<v2025_11_20_00::project::OptionalProjectSelector>,
    ) -> Result<
        HttpResponseCreated<v2025_11_20_00::multicast::MulticastGroupMember>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn instance_multicast_group_leave(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::multicast::InstanceMulticastGroupPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn snapshot_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::ProjectSelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::snapshot::Snapshot>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn snapshot_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::ProjectSelector>,
        new_snapshot: TypedBody<latest::snapshot::SnapshotCreate>,
    ) -> Result<HttpResponseCreated<latest::snapshot::Snapshot>, HttpError> {
        unimplemented!()
    }

    async fn snapshot_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SnapshotPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<latest::snapshot::Snapshot>, HttpError> {
        unimplemented!()
    }

    async fn snapshot_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SnapshotPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn vpc_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::ProjectSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<latest::vpc::Vpc>>, HttpError> {
        unimplemented!()
    }

    async fn vpc_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::ProjectSelector>,
        body: TypedBody<latest::vpc::VpcCreate>,
    ) -> Result<HttpResponseCreated<latest::vpc::Vpc>, HttpError> {
        unimplemented!()
    }

    async fn vpc_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::VpcPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<latest::vpc::Vpc>, HttpError> {
        unimplemented!()
    }

    async fn vpc_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::VpcPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
        updated_vpc: TypedBody<latest::vpc::VpcUpdate>,
    ) -> Result<HttpResponseOk<latest::vpc::Vpc>, HttpError> {
        unimplemented!()
    }

    async fn vpc_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::VpcPath>,
        query_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn vpc_subnet_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<latest::vpc::VpcSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::vpc::VpcSubnet>>, HttpError> {
        unimplemented!()
    }

    async fn vpc_subnet_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::vpc::VpcSelector>,
        create_params: TypedBody<latest::vpc::VpcSubnetCreate>,
    ) -> Result<HttpResponseCreated<latest::vpc::VpcSubnet>, HttpError> {
        unimplemented!()
    }

    async fn vpc_subnet_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SubnetPath>,
        query_params: Query<latest::vpc::OptionalVpcSelector>,
    ) -> Result<HttpResponseOk<latest::vpc::VpcSubnet>, HttpError> {
        unimplemented!()
    }

    async fn vpc_subnet_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SubnetPath>,
        query_params: Query<latest::vpc::OptionalVpcSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn vpc_subnet_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SubnetPath>,
        query_params: Query<latest::vpc::OptionalVpcSelector>,
        subnet_params: TypedBody<latest::vpc::VpcSubnetUpdate>,
    ) -> Result<HttpResponseOk<latest::vpc::VpcSubnet>, HttpError> {
        unimplemented!()
    }

    async fn vpc_subnet_list_network_interfaces(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SubnetPath>,
        query_params: Query<
            PaginatedByNameOrId<latest::vpc::OptionalVpcSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<InstanceNetworkInterface>>, HttpError> {
        unimplemented!()
    }

    async fn vpc_firewall_rules_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::vpc::VpcSelector>,
    ) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError> {
        unimplemented!()
    }

    async fn vpc_firewall_rules_update(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::vpc::VpcSelector>,
        update: TypedBody<VpcFirewallRuleUpdateParams>,
    ) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError> {
        unimplemented!()
    }

    async fn vpc_router_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<latest::vpc::VpcSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::vpc::VpcRouter>>, HttpError> {
        unimplemented!()
    }

    async fn vpc_router_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::RouterPath>,
        query_params: Query<latest::vpc::OptionalVpcSelector>,
    ) -> Result<HttpResponseOk<latest::vpc::VpcRouter>, HttpError> {
        unimplemented!()
    }

    async fn vpc_router_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::vpc::VpcSelector>,
        create_params: TypedBody<latest::vpc::VpcRouterCreate>,
    ) -> Result<HttpResponseCreated<latest::vpc::VpcRouter>, HttpError> {
        unimplemented!()
    }

    async fn vpc_router_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::RouterPath>,
        query_params: Query<latest::vpc::OptionalVpcSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn vpc_router_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::RouterPath>,
        query_params: Query<latest::vpc::OptionalVpcSelector>,
        router_params: TypedBody<latest::vpc::VpcRouterUpdate>,
    ) -> Result<HttpResponseOk<latest::vpc::VpcRouter>, HttpError> {
        unimplemented!()
    }

    async fn vpc_router_route_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<latest::vpc::RouterSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<RouterRoute>>, HttpError> {
        unimplemented!()
    }

    async fn vpc_router_route_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::RoutePath>,
        query_params: Query<latest::vpc::OptionalRouterSelector>,
    ) -> Result<HttpResponseOk<RouterRoute>, HttpError> {
        unimplemented!()
    }

    async fn vpc_router_route_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::vpc::RouterSelector>,
        create_params: TypedBody<latest::vpc::RouterRouteCreate>,
    ) -> Result<HttpResponseCreated<RouterRoute>, HttpError> {
        unimplemented!()
    }

    async fn vpc_router_route_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::RoutePath>,
        query_params: Query<latest::vpc::OptionalRouterSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn vpc_router_route_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::RoutePath>,
        query_params: Query<latest::vpc::OptionalRouterSelector>,
        router_params: TypedBody<latest::vpc::RouterRouteUpdate>,
    ) -> Result<HttpResponseOk<RouterRoute>, HttpError> {
        unimplemented!()
    }

    async fn internet_gateway_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<latest::vpc::VpcSelector>>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::internet_gateway::InternetGateway>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn internet_gateway_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::InternetGatewayPath>,
        query_params: Query<latest::vpc::OptionalVpcSelector>,
    ) -> Result<
        HttpResponseOk<latest::internet_gateway::InternetGateway>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn internet_gateway_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::vpc::VpcSelector>,
        create_params: TypedBody<
            latest::internet_gateway::InternetGatewayCreate,
        >,
    ) -> Result<
        HttpResponseCreated<latest::internet_gateway::InternetGateway>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn internet_gateway_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::InternetGatewayPath>,
        query_params: Query<
            latest::internet_gateway::InternetGatewayDeleteSelector,
        >,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn internet_gateway_ip_pool_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<
                latest::internet_gateway::InternetGatewaySelector,
            >,
        >,
    ) -> Result<
        HttpResponseOk<
            ResultsPage<latest::internet_gateway::InternetGatewayIpPool>,
        >,
        HttpError,
    > {
        unimplemented!()
    }

    async fn internet_gateway_ip_pool_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::internet_gateway::InternetGatewaySelector>,
        create_params: TypedBody<
            latest::internet_gateway::InternetGatewayIpPoolCreate,
        >,
    ) -> Result<
        HttpResponseCreated<latest::internet_gateway::InternetGatewayIpPool>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn internet_gateway_ip_pool_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpPoolPath>,
        query_params: Query<
            latest::internet_gateway::DeleteInternetGatewayElementSelector,
        >,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn internet_gateway_ip_address_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<
                latest::internet_gateway::InternetGatewaySelector,
            >,
        >,
    ) -> Result<
        HttpResponseOk<
            ResultsPage<latest::internet_gateway::InternetGatewayIpAddress>,
        >,
        HttpError,
    > {
        unimplemented!()
    }

    async fn internet_gateway_ip_address_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::internet_gateway::InternetGatewaySelector>,
        create_params: TypedBody<
            latest::internet_gateway::InternetGatewayIpAddressCreate,
        >,
    ) -> Result<
        HttpResponseCreated<latest::internet_gateway::InternetGatewayIpAddress>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn internet_gateway_ip_address_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::IpAddressPath>,
        query_params: Query<
            latest::internet_gateway::DeleteInternetGatewayElementSelector,
        >,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn rack_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::rack::Rack>>, HttpError> {
        unimplemented!()
    }

    async fn rack_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::RackPath>,
    ) -> Result<HttpResponseOk<latest::rack::Rack>, HttpError> {
        unimplemented!()
    }

    async fn rack_support_shell_tunnel(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::RackPath>,
        upgrade: WebsocketUpgrade,
    ) -> WebsocketEndpointResult {
        unimplemented!()
    }

    async fn sled_list_uninitialized(
        rqctx: RequestContext<Self::Context>,
        query: Query<PaginationParams<EmptyScanParams, String>>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::hardware::UninitializedSled>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn sled_add(
        rqctx: RequestContext<Self::Context>,
        sled: TypedBody<latest::hardware::UninitializedSledId>,
    ) -> Result<HttpResponseCreated<latest::sled::SledId>, HttpError> {
        unimplemented!()
    }

    async fn rack_membership_add_sleds(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::RackPath>,
        req: TypedBody<latest::rack::RackMembershipAddSledsRequest>,
    ) -> Result<HttpResponseOk<latest::rack::RackMembershipStatus>, HttpError> {
        unimplemented!()
    }

    async fn rack_membership_abort(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::RackPath>,
        query_params: Query<latest::rack::RackMembershipVersionParam>,
    ) -> Result<HttpResponseOk<latest::rack::RackMembershipStatus>, HttpError> {
        unimplemented!()
    }

    async fn rack_membership_status(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::rack::RackMembershipConfigPathParams>,
        query_params: Query<latest::rack::RackMembershipVersionParam>,
    ) -> Result<HttpResponseOk<latest::rack::RackMembershipStatus>, HttpError> {
        unimplemented!()
    }

    async fn sled_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::sled::Sled>>, HttpError> {
        unimplemented!()
    }

    async fn sled_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SledPath>,
    ) -> Result<HttpResponseOk<latest::sled::Sled>, HttpError> {
        unimplemented!()
    }

    async fn sled_set_provision_policy(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SledPath>,
        new_provision_state: TypedBody<latest::sled::SledProvisionPolicyParams>,
    ) -> Result<
        HttpResponseOk<latest::sled::SledProvisionPolicyResponse>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn sled_instance_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SledPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::sled::SledInstance>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn physical_disk_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::physical_disk::PhysicalDisk>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn physical_disk_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::PhysicalDiskPath>,
    ) -> Result<HttpResponseOk<latest::physical_disk::PhysicalDisk>, HttpError> {
        unimplemented!()
    }

    async fn physical_disk_list_unadopted(
        rqctx: RequestContext<Self::Context>,
        query: Query<PaginationParams<EmptyScanParams, String>>,
    ) -> Result<
        HttpResponseOk<
            ResultsPage<latest::physical_disk::UnadoptedPhysicalDisk>,
        >,
        HttpError,
    > {
        unimplemented!()
    }

    async fn physical_disk_list_adoption_requests(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<
            ResultsPage<latest::physical_disk::PhysicalDiskAdoptionRequest>,
        >,
        HttpError,
    > {
        unimplemented!()
    }

    async fn physical_disk_enable_adoption(
        rqctx: RequestContext<Self::Context>,
        req: TypedBody<latest::physical_disk::PhysicalDiskManufacturerIdentity>,
    ) -> Result<
        HttpResponseCreated<latest::physical_disk::PhysicalDiskAdoptionRequest>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn physical_disk_disable_adoption(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<
            latest::physical_disk::PhysicalDiskAdoptionRequestPath,
        >,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn switch_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::switch::Switch>>, HttpError> {
        unimplemented!()
    }

    async fn switch_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SwitchPath>,
    ) -> Result<HttpResponseOk<latest::switch::Switch>, HttpError> {
        unimplemented!()
    }

    async fn sled_physical_disk_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SledPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::physical_disk::PhysicalDisk>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn rack_list_unadopted(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::multirack::UnadoptedRack>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_metric(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::metrics::SystemMetricsPathParam>,
        pag_params: Query<
            PaginationParams<
                latest::metrics::ResourceMetrics,
                latest::metrics::ResourceMetrics,
            >,
        >,
        other_params: Query<latest::silo::OptionalSiloSelector>,
    ) -> Result<
        HttpResponseOk<
            ResultsPage<oximeter_types_versions::latest::types::Measurement>,
        >,
        HttpError,
    > {
        unimplemented!()
    }

    async fn silo_metric(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::metrics::SystemMetricsPathParam>,
        pag_params: Query<
            PaginationParams<
                latest::metrics::ResourceMetrics,
                latest::metrics::ResourceMetrics,
            >,
        >,
        other_params: Query<latest::project::OptionalProjectSelector>,
    ) -> Result<
        HttpResponseOk<
            ResultsPage<oximeter_types_versions::latest::types::Measurement>,
        >,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_timeseries_schema_list(
        rqctx: RequestContext<Self::Context>,
        pag_params: Query<TimeseriesSchemaPaginationParams>,
    ) -> Result<
        HttpResponseOk<
            ResultsPage<
                oximeter_types_versions::latest::schema::TimeseriesSchema,
            >,
        >,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_timeseries_query(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::timeseries::TimeseriesQuery>,
    ) -> Result<HttpResponseOk<latest::oxql::OxqlQueryResult>, HttpError> {
        unimplemented!()
    }

    async fn timeseries_query(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::ProjectSelector>,
        body: TypedBody<latest::timeseries::TimeseriesQuery>,
    ) -> Result<HttpResponseOk<latest::oxql::OxqlQueryResult>, HttpError> {
        unimplemented!()
    }

    async fn system_update_repository_upload(
        rqctx: RequestContext<Self::Context>,
        query: Query<latest::update::UpdatesPutRepositoryParams>,
        body: StreamingBody,
    ) -> Result<HttpResponseOk<latest::update::TufRepoUpload>, HttpError> {
        unimplemented!()
    }

    async fn system_update_repository_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::update::UpdatesGetRepositoryParams>,
    ) -> Result<HttpResponseOk<latest::update::TufRepo>, HttpError> {
        unimplemented!()
    }

    async fn system_update_repository_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByVersion>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::update::TufRepo>>, HttpError> {
        unimplemented!()
    }

    async fn system_update_trust_root_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::update::UpdatesTrustRoot>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn system_update_trust_root_create(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::update::TufSignedRootRole>,
    ) -> Result<HttpResponseCreated<latest::update::UpdatesTrustRoot>, HttpError> {
        unimplemented!()
    }

    async fn system_update_trust_root_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::TufTrustRootPath>,
    ) -> Result<HttpResponseOk<latest::update::UpdatesTrustRoot>, HttpError> {
        unimplemented!()
    }

    async fn system_update_trust_root_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::TufTrustRootPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn target_release_update(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::update::SetTargetReleaseParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn system_update_recovery_finish(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::update::SetTargetReleaseParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn system_update_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::update::UpdateStatus>, HttpError> {
        unimplemented!()
    }

    async fn user_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById<latest::user::OptionalGroupSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::user::User>>, HttpError> {
        unimplemented!()
    }

    async fn user_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::UserPath>,
    ) -> Result<HttpResponseOk<latest::user::User>, HttpError> {
        unimplemented!()
    }

    async fn user_token_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::UserPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::device::DeviceAccessToken>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn user_session_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::UserPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::device::ConsoleSession>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn user_logout(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::UserPath>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::user::Group>>, HttpError> {
        unimplemented!()
    }

    async fn group_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::GroupPath>,
    ) -> Result<HttpResponseOk<latest::user::Group>, HttpError> {
        unimplemented!()
    }

    async fn user_builtin_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByName>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::user::UserBuiltin>>, HttpError> {
        unimplemented!()
    }

    async fn user_builtin_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::user::UserBuiltinSelector>,
    ) -> Result<HttpResponseOk<latest::user::UserBuiltin>, HttpError> {
        unimplemented!()
    }

    async fn current_user_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::user::CurrentUser>, HttpError> {
        unimplemented!()
    }

    async fn current_user_groups(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::user::Group>>, HttpError> {
        unimplemented!()
    }

    async fn current_user_ssh_key_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::ssh_key::SshKey>>, HttpError> {
        unimplemented!()
    }

    async fn current_user_ssh_key_create(
        rqctx: RequestContext<Self::Context>,
        new_key: TypedBody<latest::ssh_key::SshKeyCreate>,
    ) -> Result<HttpResponseCreated<latest::ssh_key::SshKey>, HttpError> {
        unimplemented!()
    }

    async fn current_user_ssh_key_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SshKeyPath>,
    ) -> Result<HttpResponseOk<latest::ssh_key::SshKey>, HttpError> {
        unimplemented!()
    }

    async fn current_user_ssh_key_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::SshKeyPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn current_user_access_token_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::device::DeviceAccessToken>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn current_user_access_token_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::TokenPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByTimeAndId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::support_bundle::SupportBundleInfo>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn support_bundle_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::support_bundle::SupportBundlePath>,
    ) -> Result<
        HttpResponseOk<latest::support_bundle::SupportBundleInfo>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn support_bundle_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<headers::RangeRequest>,
        path_params: Path<latest::support_bundle::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_download(
        rqctx: RequestContext<Self::Context>,
        headers: Header<headers::RangeRequest>,
        path_params: Path<latest::support_bundle::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_download_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<headers::RangeRequest>,
        path_params: Path<latest::support_bundle::SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_head(
        rqctx: RequestContext<Self::Context>,
        headers: Header<headers::RangeRequest>,
        path_params: Path<latest::support_bundle::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_head_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<headers::RangeRequest>,
        path_params: Path<latest::support_bundle::SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_create(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::support_bundle::SupportBundleCreate>,
    ) -> Result<
        HttpResponseCreated<latest::support_bundle::SupportBundleInfo>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn support_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::support_bundle::SupportBundlePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::support_bundle::SupportBundlePath>,
        body: TypedBody<latest::support_bundle::SupportBundleUpdate>,
    ) -> Result<
        HttpResponseOk<latest::support_bundle::SupportBundleInfo>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn probe_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<latest::project::ProjectSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<latest::probe::ProbeInfo>>, HttpError> {
        unimplemented!()
    }

    async fn probe_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::path_params::ProbePath>,
        query_params: Query<latest::project::ProjectSelector>,
    ) -> Result<HttpResponseOk<latest::probe::ProbeInfo>, HttpError> {
        unimplemented!()
    }

    async fn probe_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::ProjectSelector>,
        new_probe: TypedBody<latest::probe::ProbeCreate>,
    ) -> Result<HttpResponseCreated<Probe>, HttpError> {
        unimplemented!()
    }

    async fn probe_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::project::ProjectSelector>,
        path_params: Path<latest::path_params::ProbePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn audit_log_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByTimeAndId<latest::audit::AuditLogParams>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::audit::AuditLogEntry>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn login_saml_begin(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::LoginToProviderPathParam>,
        query_params: Query<latest::console::LoginUrlQuery>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn login_saml_redirect(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::LoginToProviderPathParam>,
        query_params: Query<latest::console::LoginUrlQuery>,
    ) -> Result<HttpResponseFound, HttpError> {
        unimplemented!()
    }

    async fn login_saml(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::LoginToProviderPathParam>,
        body_bytes: dropshot::UntypedBody,
    ) -> Result<HttpResponseSeeOther, HttpError> {
        unimplemented!()
    }

    async fn login_local_begin(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::LoginPath>,
        query_params: Query<latest::console::LoginUrlQuery>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn login_local(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::LoginPath>,
        credentials: TypedBody<latest::user::UsernamePasswordCredentials>,
    ) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError> {
        unimplemented!()
    }

    async fn logout(
        rqctx: RequestContext<Self::Context>,
        cookies: Cookies,
    ) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError> {
        unimplemented!()
    }

    async fn login_begin(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::console::LoginUrlQuery>,
    ) -> Result<HttpResponseFound, HttpError> {
        unimplemented!()
    }

    async fn console_projects(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn console_settings_page(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn console_system_page(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn console_lookup(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn console_root(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn console_projects_new(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn console_silo_images(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn console_silo_utilization(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn console_silo_access(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn asset(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn device_auth_request(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::device_params::DeviceAuthRequest>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn device_auth_verify(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn device_auth_success(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn device_auth_confirm(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::device_params::DeviceAuthVerify>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn device_access_token(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::device_params::DeviceAccessTokenRequest>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn problem_list(
        rqctx: RequestContext<Self::Context>,
        pagination: Query<
            PaginatedByTimeAndId<latest::problem::ProblemListParams>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<latest::problem::Problem>>, HttpError> {
        unimplemented!()
    }

    async fn problem_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::problem::ProblemSelector>,
    ) -> Result<HttpResponseOk<latest::problem::Problem>, HttpError> {
        unimplemented!()
    }

    async fn problem_causes_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::problem::ProblemSelector>,
        pagination: Query<PaginatedById<latest::problem::ProblemListParams>>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::problem::RelatedProblem>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn problem_consequences_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::problem::ProblemSelector>,
        pagination: Query<PaginatedById<latest::problem::ProblemListParams>>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::problem::RelatedProblem>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn problem_impact_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::problem::ProblemSelector>,
        pagination: Query<PaginatedById<latest::problem::ImpactListParams>>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::problem::ProblemImpact>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn problem_support_bundles_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::problem::ProblemSelector>,
        query_params: Query<PaginatedByTimeAndId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::support_bundle::SupportBundleInfo>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn problem_alert_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::problem::ProblemSelector>,
        pagination: Query<PaginatedByTimeAndId<latest::alert::AlertListParams>>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::alert::Alert>>, HttpError> {
        unimplemented!()
    }

    async fn alert_list(
        rqctx: RequestContext<Self::Context>,
        pagination: Query<PaginatedByTimeAndId<latest::alert::AlertListParams>>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::alert::Alert>>, HttpError> {
        unimplemented!()
    }

    async fn alert_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::alert::AlertSelector>,
    ) -> Result<HttpResponseOk<latest::alert::Alert>, HttpError> {
        unimplemented!()
    }

    async fn alert_class_list(
        rqctx: RequestContext<Self::Context>,
        pag_params: Query<
            PaginationParams<EmptyScanParams, latest::alert::AlertClassPage>,
        >,
        filter: Query<latest::alert::AlertClassFilter>,
    ) -> Result<HttpResponseOk<ResultsPage<latest::alert::AlertClass>>, HttpError> {
        unimplemented!()
    }

    async fn alert_receiver_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::alert::AlertReceiver>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn alert_receiver_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::alert::AlertReceiverSelector>,
    ) -> Result<HttpResponseOk<latest::alert::AlertReceiver>, HttpError> {
        unimplemented!()
    }

    async fn alert_receiver_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::alert::AlertReceiverSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn alert_receiver_subscription_add(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::alert::AlertReceiverSelector>,
        params: TypedBody<latest::alert::AlertSubscriptionCreate>,
    ) -> Result<
        HttpResponseCreated<latest::alert::AlertSubscriptionCreated>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn alert_receiver_subscription_remove(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::alert::AlertSubscriptionSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn alert_delivery_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::alert::AlertReceiverSelector>,
        state_filter: Query<latest::alert::AlertDeliveryStateFilter>,
        pagination: Query<PaginatedByTimeAndId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<latest::alert::AlertDelivery>>,
        HttpError,
    > {
        unimplemented!()
    }

    async fn alert_receiver_probe(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::alert::AlertReceiverSelector>,
        query_params: Query<latest::alert::AlertReceiverProbe>,
    ) -> Result<HttpResponseOk<latest::alert::AlertProbeResult>, HttpError> {
        unimplemented!()
    }

    async fn alert_delivery_resend(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::alert::AlertSelector>,
        receiver: Query<latest::alert::AlertReceiverSelector>,
    ) -> Result<HttpResponseCreated<latest::alert::AlertDeliveryId>, HttpError> {
        unimplemented!()
    }

    async fn webhook_receiver_create(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::alert::WebhookCreate>,
    ) -> Result<HttpResponseCreated<latest::alert::WebhookReceiver>, HttpError> {
        unimplemented!()
    }

    async fn webhook_receiver_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::alert::AlertReceiverSelector>,
        params: TypedBody<latest::alert::WebhookReceiverUpdate>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn webhook_secrets_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::alert::AlertReceiverSelector>,
    ) -> Result<HttpResponseOk<latest::alert::WebhookSecrets>, HttpError> {
        unimplemented!()
    }

    async fn webhook_secrets_add(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<latest::alert::AlertReceiverSelector>,
        params: TypedBody<latest::alert::WebhookSecretCreate>,
    ) -> Result<HttpResponseCreated<latest::alert::WebhookSecret>, HttpError> {
        unimplemented!()
    }

    async fn webhook_secrets_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::alert::WebhookSecretSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

}

#[inline(never)]
pub fn anchor() {
    let _ = std::hint::black_box(nexus_external_api_mod::api_description::<AnchorStub>());
}
