// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_queries::context::OpContext;
use nexus_types::external_api::{
    service_account as account, service_account_token as api,
};
use omicron_common::api::external::{DataPageParams, Error};
use uuid::Uuid;

impl super::Nexus {
    pub(crate) async fn authenticate_service_account_token(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> Result<nexus_auth::authn::Details, nexus_auth::authn::Reason> {
        use nexus_auth::{authn, authz};
        use nexus_db_model::DatabaseString;
        use nexus_types::external_api::service_account::ServiceAccountGrant;
        use omicron_common::api::external::ResourceType;

        let (session, silo_id, grants) = self
            .datastore()
            .service_account_token_fetch_for_authn(opctx, token)
            .await
            .map_err(|source| authn::Reason::UnknownError { source })?
            .ok_or_else(|| authn::Reason::UnknownActor {
                actor: "service account token".to_owned(),
            })?;
        let mut roles = authz::RoleSet::new();
        for grant in grants {
            match ServiceAccountGrant::try_from(grant)
                .map_err(|source| authn::Reason::UnknownError { source })?
            {
                ServiceAccountGrant::Silo { resource_id, role_name } => {
                    roles.insert(
                        ResourceType::Silo,
                        resource_id,
                        &role_name.to_database_string(),
                    );
                }
                ServiceAccountGrant::Project { resource_id, role_name } => {
                    roles.insert(
                        ResourceType::Project,
                        resource_id,
                        &role_name.to_database_string(),
                    );
                }
            }
        }
        let federation_identity = match session.idp_id {
            None => None,
            Some(idp_id) => {
                let claim = |name| {
                    session
                        .federation_jwt_claims
                        .as_ref()
                        .and_then(|claims| claims.get(name))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .ok_or_else(|| authn::Reason::UnknownError {
                            source: Error::internal_error(
                                "stored federation identity is incomplete",
                            ),
                        })
                };
                Some(nexus_types::external_api::audit::FederationIdentity {
                    idp_id,
                    iss: claim("iss")?,
                    sub: claim("sub")?,
                })
            }
        };
        Ok(authn::Details {
            actor: authn::Actor::ServiceAccountSession {
                service_account_id: session.service_account_id,
                silo_id,
            },
            credential_id: Some(session.id),
            token_expiration: session.time_expires,
            service_account_roles: Some(roles),
            federation_identity,
        })
    }

    pub(crate) async fn service_account_token_create(
        &self,
        opctx: &OpContext,
        path: account::ServiceAccountPath,
        parent: account::ServiceAccountParentSelector,
        params: api::ServiceAccountTokenCreate,
    ) -> Result<api::ServiceAccountTokenGrant, Error> {
        self.db_datastore
            .service_account_token_create(opctx, path, parent, params)
            .await
            .map(Into::into)
    }

    pub(crate) async fn service_account_token_list(
        &self,
        opctx: &OpContext,
        path: account::ServiceAccountPath,
        parent: account::ServiceAccountParentSelector,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> Result<Vec<api::ServiceAccountToken>, Error> {
        self.db_datastore
            .service_account_token_list(opctx, path, parent, pagparams)
            .await
    }

    pub(crate) async fn service_account_token_view(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountTokenPath,
        parent: account::ServiceAccountParentSelector,
    ) -> Result<api::ServiceAccountToken, Error> {
        self.db_datastore.service_account_token_view(opctx, path, parent).await
    }

    pub(crate) async fn service_account_token_delete(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountTokenPath,
        parent: account::ServiceAccountParentSelector,
    ) -> Result<(), Error> {
        self.db_datastore
            .service_account_token_delete(opctx, path, parent)
            .await
    }
}
