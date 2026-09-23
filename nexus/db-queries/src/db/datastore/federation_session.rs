// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{DataStore, RunnableQuery};
use crate::authz;
use crate::context::OpContext;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use nexus_db_errors::{ErrorHandler, OptionalError, public_error_from_diesel};
use nexus_db_model::{
    FederationIdentityProvider, FederationRoleGrant, FederationSession,
    FederationTrustPolicy,
};
use nexus_db_schema::schema::{
    federation_identity_provider, federation_role_grant, federation_session,
    federation_trust_policy, silo,
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
    pub async fn federation_session_fetch_for_authn(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> Result<
        Option<(FederationSession, Uuid, Vec<FederationRoleGrant>)>,
        Error,
    > {
        opctx
            .authorize(
                authz::Action::CreateChild,
                &authz::FEDERATION_SESSION_LIST,
            )
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("federation_session_fetch_for_authn")
            .transaction(&conn, |conn| {
                let token = token.clone();
                async move {
                    let now = Utc::now();
                    let found = Self::federation_session_fetch_for_authn_query(
                        token, now,
                    )
                    .get_result_async::<(FederationSession, Uuid)>(&conn)
                    .await
                    .optional()?;
                    let Some((mut session, silo_id)) = found else {
                        return Ok(None);
                    };
                    let grants = federation_role_grant::table
                        .filter(
                            federation_role_grant::trust_policy_id
                                .eq(session.trust_policy_id),
                        )
                        .select(FederationRoleGrant::as_select())
                        .load_async(&conn)
                        .await?;
                    diesel::update(
                        federation_session::table
                            .filter(federation_session::id.eq(session.id)),
                    )
                    .set(federation_session::time_last_used.eq(now))
                    .execute_async(&conn)
                    .await?;
                    session.time_last_used = now;
                    Ok(Some((session, silo_id, grants)))
                }
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn federation_session_fetch_for_authn_query(
        token: String,
        now: DateTime<Utc>,
    ) -> impl RunnableQuery<(FederationSession, Uuid)> + Send + use<> {
        federation_session::table
            .inner_join(
                federation_trust_policy::table.on(federation_trust_policy::id
                    .eq(federation_session::trust_policy_id)),
            )
            .inner_join(
                federation_identity_provider::table.on(
                    federation_identity_provider::id
                        .eq(federation_trust_policy::idp_id)
                        .and(
                            federation_identity_provider::silo_id
                                .eq(federation_trust_policy::silo_id),
                        ),
                ),
            )
            .inner_join(
                silo::table.on(silo::id.eq(federation_trust_policy::silo_id)),
            )
            .filter(federation_session::token.eq(token))
            .filter(federation_session::time_expires.gt(now))
            .filter(
                federation_session::trust_policy_revision
                    .eq(federation_trust_policy::revision),
            )
            .filter(federation_trust_policy::time_deleted.is_null())
            .filter(federation_identity_provider::time_deleted.is_null())
            .filter(silo::time_deleted.is_null())
            .select((
                FederationSession::as_select(),
                federation_trust_policy::silo_id,
            ))
            .limit(1)
    }

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

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::raw_query_builder::expectorate_query_contents;

    #[tokio::test]
    async fn expectorate_federation_session_fetch_for_authn() {
        let query = DataStore::federation_session_fetch_for_authn_query(
            "test-token".to_owned(),
            DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        );
        expectorate_query_contents(
            query,
            "tests/output/federation_session_fetch_for_authn.sql",
        )
        .await;
    }
}
