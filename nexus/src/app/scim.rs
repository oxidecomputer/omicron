// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! SCIM endpoints

use crate::db::model::UserProvisionType;

use anyhow::anyhow;
use chrono::Utc;
use dropshot::Body;
use dropshot::HttpError;
use http::Response;
use http::StatusCode;
use nexus_db_lookup::lookup;
use nexus_db_queries::authn::{Actor, Reason};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::CrdbScimProviderStore;
use nexus_types::external_api::views;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use uuid::Uuid;

impl super::Nexus {
    // SCIM tokens

    pub(crate) async fn scim_idp_get_tokens(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> ListResultVec<views::ScimClientBearerToken> {
        let (.., authz_silo, _) = silo_lookup.fetch().await?;

        let tokens =
            self.datastore().scim_idp_get_tokens(opctx, &authz_silo).await?;

        Ok(tokens.into_iter().map(|t| t.into()).collect())
    }

    pub(crate) async fn scim_idp_create_token(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> CreateResult<views::ScimClientBearerTokenValue> {
        let (.., authz_silo, _) = silo_lookup.fetch().await?;

        let token =
            self.datastore().scim_idp_create_token(opctx, &authz_silo).await?;

        Ok(token.into())
    }

    pub(crate) async fn scim_idp_get_token_by_id(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        token_id: Uuid,
    ) -> LookupResult<views::ScimClientBearerToken> {
        let (.., authz_silo, _) = silo_lookup.fetch().await?;

        let token = self
            .datastore()
            .scim_idp_get_token_by_id(opctx, &authz_silo, token_id)
            .await?;

        Ok(token.into())
    }

    pub(crate) async fn scim_idp_delete_token_by_id(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        token_id: Uuid,
    ) -> DeleteResult {
        let (.., authz_silo, _) = silo_lookup.fetch().await?;

        self.datastore()
            .scim_idp_delete_token_by_id(opctx, &authz_silo, token_id)
            .await?;

        Ok(())
    }

    // SCIM client authentication

    pub(crate) async fn scim_token_actor(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> Result<Actor, Reason> {
        let Some(bearer_token) = self
            .datastore()
            .scim_lookup_token_by_bearer(opctx, token.clone())
            .await
            .map_err(|e| Reason::UnknownError { source: e })?
        else {
            return Err(Reason::UnknownActor {
                actor: "scim bearer token".to_string(),
            });
        };

        if let Some(time_expires) = &bearer_token.time_expires {
            let now = Utc::now();
            if now > *time_expires {
                return Err(Reason::BadCredentials {
                    actor: Actor::Scim { silo_id: bearer_token.silo_id },
                    source: anyhow!(
                        "token expired at {time_expires} (current time: {now})"
                    ),
                });
            }
        }

        // Validate that silo has the SCIM user provision type
        let (_, db_silo) = {
            self.silo_lookup(opctx, bearer_token.silo_id.into())
                .map_err(|e| Reason::UnknownError { source: e })?
                .fetch()
                .await
                .map_err(|e| Reason::UnknownError { source: e })?
        };

        if db_silo.user_provision_type != UserProvisionType::Scim {
            // This should basically be impossible if the bearer token lookup
            // returned something, but double check anyway.
            return Err(Reason::BadCredentials {
                actor: Actor::Scim { silo_id: bearer_token.silo_id },
                source: anyhow!(
                    "silo {} not a SCIM silo!",
                    bearer_token.silo_id,
                ),
            });
        }

        Ok(Actor::Scim { silo_id: bearer_token.silo_id })
    }

    /// For an authenticataed Actor::Scim, return a scim2_rs::Provider
    pub(crate) async fn scim_get_provider_from_opctx(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<scim2_rs::Provider<CrdbScimProviderStore>> {
        match opctx.authn.actor() {
            Some(Actor::Scim { silo_id }) => Ok(scim2_rs::Provider::new(
                self.log.new(slog::o!(
                    "component" => "scim2_rs::Provider",
                    "silo" => silo_id.to_string(),
                )),
                CrdbScimProviderStore::new(*silo_id, self.datastore().clone()),
            )),

            _ => Err(Error::Unauthenticated {
                internal_message: "not an Actor::Scim".to_string(),
            }),
        }
    }

    // SCIM implementation

    pub async fn scim_v2_list_users(
        &self,
        opctx: &OpContext,
        query: scim2_rs::QueryParams,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.list_users(query).await {
            Ok(response) => response.to_http_response(),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }

    pub async fn scim_v2_get_user_by_id(
        &self,
        opctx: &OpContext,
        query: scim2_rs::QueryParams,
        user_id: String,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.get_user_by_id(query, &user_id).await {
            Ok(response) => response.to_http_response(StatusCode::OK),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }

    pub async fn scim_v2_create_user(
        &self,
        opctx: &OpContext,
        body: scim2_rs::CreateUserRequest,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.create_user(body).await {
            Ok(response) => response.to_http_response(StatusCode::CREATED),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }

    pub async fn scim_v2_replace_user(
        &self,
        opctx: &OpContext,
        user_id: String,
        body: scim2_rs::CreateUserRequest,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.replace_user(&user_id, body).await {
            Ok(response) => response.to_http_response(StatusCode::OK),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }

    pub async fn scim_v2_patch_user(
        &self,
        opctx: &OpContext,
        user_id: String,
        body: scim2_rs::PatchRequest,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.patch_user(&user_id, body).await {
            Ok(response) => response.to_http_response(StatusCode::OK),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }

    pub async fn scim_v2_delete_user(
        &self,
        opctx: &OpContext,
        user_id: String,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.delete_user(&user_id).await {
            Ok(response) => Ok(response),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }

    pub async fn scim_v2_list_groups(
        &self,
        opctx: &OpContext,
        query: scim2_rs::QueryParams,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.list_groups(query).await {
            Ok(response) => response.to_http_response(),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }

    pub async fn scim_v2_get_group_by_id(
        &self,
        opctx: &OpContext,
        query: scim2_rs::QueryParams,
        group_id: String,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.get_group_by_id(query, &group_id).await {
            Ok(response) => response.to_http_response(StatusCode::OK),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }

    pub async fn scim_v2_create_group(
        &self,
        opctx: &OpContext,
        body: scim2_rs::CreateGroupRequest,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.create_group(body).await {
            Ok(response) => response.to_http_response(StatusCode::CREATED),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }

    pub async fn scim_v2_replace_group(
        &self,
        opctx: &OpContext,
        group_id: String,
        body: scim2_rs::CreateGroupRequest,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.replace_group(&group_id, body).await {
            Ok(response) => response.to_http_response(StatusCode::OK),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }

    pub async fn scim_v2_patch_group(
        &self,
        opctx: &OpContext,
        group_id: String,
        body: scim2_rs::PatchRequest,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.patch_group(&group_id, body).await {
            Ok(response) => response.to_http_response(StatusCode::OK),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }

    pub async fn scim_v2_delete_group(
        &self,
        opctx: &OpContext,
        group_id: String,
    ) -> Result<Response<Body>, HttpError> {
        let provider = self.scim_get_provider_from_opctx(opctx).await?;

        let result = match provider.delete_group(&group_id).await {
            Ok(response) => Ok(response),
            Err(error) => error.to_http_response(),
        };

        result.map_err(HttpError::from)
    }
}
