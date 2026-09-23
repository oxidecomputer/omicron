// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::{ErrorHandler, OptionalError, public_error_from_diesel};
use nexus_db_model::{
    FederationIdentityProvider, FederationSession, FederationTrustPolicy,
};
use nexus_db_schema::schema::{
    federation_identity_provider, federation_session, federation_trust_policy,
    silo,
};
use nexus_types::identity::Resource;
use omicron_common::api::external::{Error, NameOrId};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone)]
pub struct FederationTrustPolicyConfig {
    pub trust_policy: FederationTrustPolicy,
    pub identity_provider: FederationIdentityProvider,
}

impl DataStore {
    pub async fn federation_trust_policy_config(
        &self,
        opctx: &OpContext,
        silo_id: Uuid,
        selector: &NameOrId,
    ) -> Result<FederationTrustPolicyConfig, Error> {
        opctx
            .authorize(
                authz::Action::CreateChild,
                &authz::FEDERATION_SESSION_LIST,
            )
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        self.transaction_retry_wrapper("federation_trust_policy_config")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let selector = selector.clone();
                async move {
                    let mut query = federation_trust_policy::table
                        .filter(federation_trust_policy::silo_id.eq(silo_id))
                        .filter(federation_trust_policy::time_deleted.is_null())
                        .into_boxed();
                    query = match selector {
                        NameOrId::Id(id) => {
                            query.filter(federation_trust_policy::id.eq(id))
                        }
                        NameOrId::Name(name) => query.filter(
                            federation_trust_policy::name.eq(name.to_string()),
                        ),
                    };
                    let policy = query
                        .select(FederationTrustPolicy::as_select())
                        .first_async(&conn)
                        .await
                        .optional()?
                        .ok_or_else(|| err.bail(Error::Forbidden))?;
                    let provider = federation_identity_provider::table
                        .filter(
                            federation_identity_provider::id.eq(policy.idp_id),
                        )
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
                    Ok(FederationTrustPolicyConfig {
                        trust_policy: policy,
                        identity_provider: provider,
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

    pub async fn federation_session_create(
        &self,
        opctx: &OpContext,
        verified: &FederationTrustPolicyConfig,
        jwt_claims: Value,
        audit_log_id: Uuid,
    ) -> Result<FederationSession, Error> {
        opctx
            .authorize(
                authz::Action::CreateChild,
                &authz::FEDERATION_SESSION_LIST,
            )
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        self.transaction_retry_wrapper("federation_session_create")
            .transaction(&conn, |conn| {
                let verified = verified.clone();
                let jwt_claims = jwt_claims.clone();
                let err = err.clone();
                async move {
                    let policy = federation_trust_policy::table
                        .filter(
                            federation_trust_policy::id
                                .eq(verified.trust_policy.id()),
                        )
                        .filter(
                            federation_trust_policy::silo_id
                                .eq(verified.trust_policy.silo_id),
                        )
                        .filter(
                            federation_trust_policy::revision
                                .eq(verified.trust_policy.revision),
                        )
                        .filter(federation_trust_policy::time_deleted.is_null())
                        .select(FederationTrustPolicy::as_select())
                        .first_async(&conn)
                        .await
                        .optional()?
                        .ok_or_else(|| err.bail(Error::Forbidden))?;
                    let live_silo = diesel::select(diesel::dsl::exists(
                        silo::table
                            .filter(silo::id.eq(policy.silo_id))
                            .filter(silo::time_deleted.is_null()),
                    ))
                    .get_result_async::<bool>(&conn)
                    .await?;
                    if !live_silo
                        || jwt_claims
                            .get("exp")
                            .and_then(Value::as_i64)
                            .is_none_or(|exp| exp <= Utc::now().timestamp())
                    {
                        return Err(err.bail(Error::Forbidden));
                    }
                    let session = FederationSession::new(
                        policy.id(),
                        policy.revision,
                        jwt_claims,
                        audit_log_id,
                    );
                    diesel::insert_into(federation_session::table)
                        .values(session)
                        .returning(FederationSession::as_returning())
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
