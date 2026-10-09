// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use super::service_account::{
    Parent, account_parent, fetch_account, parent_exists,
};
use crate::authz;
use crate::context::OpContext;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
use nexus_db_errors::{ErrorHandler, OptionalError, public_error_from_diesel};
use nexus_db_model::{
    FederationIdentityProvider, ServiceAccount, ServiceAccountToken,
};
use nexus_db_schema::schema::{
    federation_identity_provider, service_account, service_account_token, silo,
};
use nexus_types::external_api::service_account as api;
use nexus_types::identity::Resource;
use omicron_common::api::external::{Error, LookupType};
use serde_json::Value;
use std::num::NonZeroU32;
use uuid::Uuid;

#[derive(Clone)]
pub struct ServiceAccountFederationConfig {
    pub service_account: ServiceAccount,
    pub identity_provider: FederationIdentityProvider,
    parent: Parent,
    silo_id: Uuid,
}

fn federation_lookup_error(error: Error) -> Error {
    match error {
        Error::ObjectNotFound { .. } => Error::Forbidden,
        error => error,
    }
}

impl DataStore {
    pub async fn service_account_federation_config(
        &self,
        opctx: &OpContext,
        silo_id: Uuid,
        path: api::ServiceAccountPath,
        selector: api::ServiceAccountParentSelector,
    ) -> Result<ServiceAccountFederationConfig, Error> {
        opctx
            .authorize(
                authz::Action::CreateChild,
                &authz::SERVICE_ACCOUNT_TOKEN_LIST,
            )
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let authz_silo =
            authz::Silo::new(authz::FLEET, silo_id, LookupType::ById(silo_id));
        let parent = account_parent(
            &conn,
            &authz_silo,
            path.scope,
            &selector,
            &path.service_account,
        )
        .await
        .map_err(federation_lookup_error)?;
        let err = OptionalError::new();
        self.transaction_retry_wrapper("service_account_federation_config")
            .transaction(&conn, |conn| {
                let parent = parent.clone();
                let selector = path.service_account.clone();
                let err = err.clone();
                async move {
                    let live_silo = diesel::select(diesel::dsl::exists(
                        silo::table
                            .filter(silo::id.eq(silo_id))
                            .filter(silo::time_deleted.is_null()),
                    ))
                    .get_result_async::<bool>(&conn)
                    .await?;
                    if !live_silo || !parent_exists(&conn, &parent).await? {
                        return Err(err.bail(Error::Forbidden));
                    }
                    let account = fetch_account(&conn, &parent, &selector)
                        .await
                        .optional()?
                        .ok_or_else(|| err.bail(Error::Forbidden))?;
                    let idp_id = account
                        .identity_provider_id
                        .filter(|_| account.trust_policy.is_some())
                        .ok_or_else(|| err.bail(Error::Forbidden))?;
                    let provider = federation_identity_provider::table
                        .filter(federation_identity_provider::id.eq(idp_id))
                        .filter(
                            federation_identity_provider::silo_id.eq(silo_id),
                        )
                        .filter(
                            federation_identity_provider::time_deleted
                                .is_null(),
                        )
                        .select(FederationIdentityProvider::as_select())
                        .first_async(&conn)
                        .await
                        .optional()?
                        .ok_or_else(|| err.bail(Error::Forbidden))?;
                    Ok(ServiceAccountFederationConfig {
                        service_account: account,
                        identity_provider: provider,
                        parent,
                        silo_id,
                    })
                }
            })
            .await
            .map_err(|e| {
                err.take().unwrap_or_else(|| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })
            })
    }

    pub async fn service_account_federation_token_create(
        &self,
        opctx: &OpContext,
        verified: &ServiceAccountFederationConfig,
        jwt_claims: Value,
        ttl_seconds: Option<NonZeroU32>,
    ) -> Result<ServiceAccountToken, Error> {
        opctx
            .authorize(
                authz::Action::CreateChild,
                &authz::SERVICE_ACCOUNT_TOKEN_LIST,
            )
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        self.transaction_retry_wrapper("service_account_federation_token_create")
            .transaction(&conn, |conn| {
                let verified = verified.clone();
                let jwt_claims = jwt_claims.clone();
                let err = err.clone();
                async move {
                    let account = service_account::table
                        .filter(service_account::id.eq(verified.service_account.id()))
                        .filter(service_account::federation_generation.eq(
                            verified.service_account.federation_generation,
                        ))
                        .filter(service_account::time_deleted.is_null())
                        .select(ServiceAccount::as_select())
                        .first_async(&conn)
                        .await
                        .optional()?
                        .ok_or_else(|| err.bail(Error::Forbidden))?;
                    let live_silo = diesel::select(diesel::dsl::exists(
                        silo::table
                            .filter(silo::id.eq(verified.silo_id))
                            .filter(silo::time_deleted.is_null()),
                    ))
                    .get_result_async::<bool>(&conn)
                    .await?;
                    let now = Utc::now();
                    let jwt_expiration = jwt_claims
                        .get("exp")
                        .and_then(Value::as_i64)
                        .and_then(|exp| DateTime::from_timestamp(exp, 0))
                        .filter(|exp| *exp > now)
                        .ok_or_else(|| err.bail(Error::Forbidden))?;
                    if !live_silo || !parent_exists(&conn, &verified.parent).await? {
                        return Err(err.bail(Error::Forbidden));
                    }
                    let maximum = (now + Duration::seconds(
                        account.federation_token_max_ttl_seconds,
                    )).min(jwt_expiration);
                    let time_expires = match ttl_seconds {
                        Some(ttl) => {
                            let expires = now + Duration::seconds(i64::from(ttl.get()));
                            if expires > maximum {
                                return Err(err.bail(Error::invalid_request(
                                    "Requested token TTL exceeds the federation maximum \
                                     or the OIDC token's remaining lifetime",
                                )));
                            }
                            expires
                        }
                        None => maximum,
                    };
                    let mut token = ServiceAccountToken::new(
                        account.id(), Some(time_expires),
                    );
                    token.idp_id = Some(verified.identity_provider.id());
                    token.federation_generation = Some(i64::from(
                        &account.federation_generation.0,
                    ));
                    token.federation_jwt_claims = Some(jwt_claims);
                    diesel::insert_into(service_account_token::table)
                        .values(token)
                        .returning(ServiceAccountToken::as_returning())
                        .get_result_async(&conn)
                        .await
                }
            })
            .await
            .map_err(|e| {
                err.take().unwrap_or_else(|| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })
            })
    }
}
