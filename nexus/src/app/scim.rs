// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! SCIM endpoints

use dropshot::Body;
use dropshot::HttpError;
use http::Response;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::views;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use uuid::Uuid;

// XXX temporary for stub PR
use crate::app::Unimpl;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;

impl super::Nexus {
    // SCIM tokens

    pub(crate) async fn scim_idp_get_tokens(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> ListResultVec<views::ScimClientBearerToken> {
        let (.., _authz_silo, _) =
            silo_lookup.fetch_for(authz::Action::ListChildren).await?;

        let resource_type = ResourceType::ScimClientBearerToken;
        let lookup_type =
            LookupType::ByOther(String::from("scim_idp_get_tokens"));
        let not_found_error = lookup_type.into_not_found(resource_type);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    pub(crate) async fn scim_idp_create_token(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> CreateResult<views::ScimClientBearerTokenValue> {
        let (.., _authz_silo, _) =
            silo_lookup.fetch_for(authz::Action::Modify).await?;

        let resource_type = ResourceType::ScimClientBearerToken;
        let lookup_type =
            LookupType::ByOther(String::from("scim_idp_create_token"));
        let not_found_error = lookup_type.into_not_found(resource_type);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    pub(crate) async fn scim_idp_get_token_by_id(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        token_id: Uuid,
    ) -> LookupResult<views::ScimClientBearerToken> {
        let (.., _authz_silo, _) =
            silo_lookup.fetch_for(authz::Action::Read).await?;

        let resource_type = ResourceType::ScimClientBearerToken;
        let lookup_type = LookupType::by_id(token_id);
        let not_found_error = lookup_type.into_not_found(resource_type);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    pub(crate) async fn scim_idp_delete_token_by_id(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        token_id: Uuid,
    ) -> DeleteResult {
        let (.., _authz_silo, _) =
            silo_lookup.fetch_for(authz::Action::Delete).await?;

        let resource_type = ResourceType::ScimClientBearerToken;
        let lookup_type = LookupType::by_id(token_id);
        let not_found_error = lookup_type.into_not_found(resource_type);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    pub(crate) async fn scim_idp_delete_tokens_for_silo(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> DeleteResult {
        let (.., _authz_silo, _) =
            silo_lookup.fetch_for(authz::Action::ListChildren).await?;

        let resource_type = ResourceType::ScimClientBearerToken;
        let lookup_type = LookupType::ByOther(String::from(
            "scim_idp_delete_tokens_for_silo",
        ));
        let not_found_error = lookup_type.into_not_found(resource_type);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    // SCIM implementation
    // XXX cannot use [`unimplemented_todo`] here, there's no opctx

    pub async fn scim_v2_list_users(
        &self,
        _request: &dropshot::RequestInfo,
        _query: scim2_rs::QueryParams,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }

    pub async fn scim_v2_get_user_by_id(
        &self,
        _request: &dropshot::RequestInfo,
        _query: scim2_rs::QueryParams,
        _user_id: String,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }

    pub async fn scim_v2_create_user(
        &self,
        _request: &dropshot::RequestInfo,
        _body: scim2_rs::CreateUserRequest,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }

    pub async fn scim_v2_replace_user(
        &self,
        _request: &dropshot::RequestInfo,
        _user_id: String,
        _body: scim2_rs::CreateUserRequest,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }

    pub async fn scim_v2_patch_user(
        &self,
        _request: &dropshot::RequestInfo,
        _user_id: String,
        _body: scim2_rs::PatchRequest,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }

    pub async fn scim_v2_delete_user(
        &self,
        _request: &dropshot::RequestInfo,
        _user_id: String,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }

    pub async fn scim_v2_list_groups(
        &self,
        _request: &dropshot::RequestInfo,
        _query: scim2_rs::QueryParams,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }

    pub async fn scim_v2_get_group_by_id(
        &self,
        _request: &dropshot::RequestInfo,
        _query: scim2_rs::QueryParams,
        _group_id: String,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }

    pub async fn scim_v2_create_group(
        &self,
        _request: &dropshot::RequestInfo,
        _body: scim2_rs::CreateGroupRequest,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }

    pub async fn scim_v2_replace_group(
        &self,
        _request: &dropshot::RequestInfo,
        _group_id: String,
        _body: scim2_rs::CreateGroupRequest,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }

    pub async fn scim_v2_patch_group(
        &self,
        _request: &dropshot::RequestInfo,
        _group_id: String,
        _body: scim2_rs::PatchRequest,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }

    pub async fn scim_v2_delete_group(
        &self,
        _request: &dropshot::RequestInfo,
        _group_id: String,
    ) -> Result<Response<Body>, HttpError> {
        Err(Error::internal_error("endpoint is not implemented").into())
    }
}
