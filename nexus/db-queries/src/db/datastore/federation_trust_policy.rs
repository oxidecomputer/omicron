// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::{ErrorHandler, OptionalError, public_error_from_diesel};
use nexus_db_lookup::DbConnection;
use nexus_db_model::{
    FederationRoleGrant, FederationTrustPolicy, Name,
    validate_federation_trust_policy,
};
use nexus_db_schema::schema::{
    federation_identity_provider, federation_role_grant,
    federation_trust_policy, project,
};
use nexus_types::external_api::federation as api;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    Error, InternalContext, LookupType, NameOrId, ResourceType,
};
use ref_cast::RefCast;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

type Conn = async_bb8_diesel::Connection<DbConnection>;
type DieselResult<T> = Result<T, diesel::result::Error>;

async fn authorize(
    opctx: &OpContext,
    action: authz::Action,
) -> Result<Uuid, Error> {
    let silo = opctx
        .authn
        .silo_required()
        .internal_context("managing federation trust policies")?;
    let silo_id = silo.id();
    opctx
        .authorize(action, &authz::SiloFederationTrustPolicyList::new(silo))
        .await?;
    Ok(silo_id)
}

fn not_found(id: Uuid) -> ErrorHandler<'static> {
    ErrorHandler::NotFoundByLookup(
        ResourceType::FederationTrustPolicy,
        LookupType::ById(id),
    )
}

async fn resolve_idp(
    conn: &Conn,
    silo_id: Uuid,
    selector: &NameOrId,
    err: &OptionalError<Error>,
) -> DieselResult<Uuid> {
    use federation_identity_provider::dsl;
    let mut query = dsl::federation_identity_provider
        .filter(dsl::silo_id.eq(silo_id))
        .filter(dsl::time_deleted.is_null())
        .into_boxed();
    let lookup = match selector {
        NameOrId::Id(id) => {
            query = query.filter(dsl::id.eq(*id));
            LookupType::ById(*id)
        }
        NameOrId::Name(name) => {
            query = query.filter(dsl::name.eq(name.to_string()));
            LookupType::ByName(name.to_string())
        }
    };
    query.select(dsl::id).first_async(conn).await.map_err(|e| {
        err.bail_retryable_or_else(e, |e| {
            public_error_from_diesel(
                e,
                ErrorHandler::NotFoundByLookup(
                    ResourceType::FederationIdentityProvider,
                    lookup,
                ),
            )
        })
    })
}

async fn validate_targets(
    conn: &Conn,
    silo_id: Uuid,
    grants: &[FederationRoleGrant],
    err: &OptionalError<Error>,
) -> DieselResult<()> {
    for grant in grants {
        match grant.resource_kind.as_str() {
            "silo" if grant.resource_id == silo_id => {}
            "project" => {
                use project::dsl;
                let exists = diesel::select(diesel::dsl::exists(
                    dsl::project
                        .filter(dsl::id.eq(grant.resource_id))
                        .filter(dsl::silo_id.eq(silo_id))
                        .filter(dsl::time_deleted.is_null()),
                ))
                .get_result_async::<bool>(conn)
                .await?;
                if !exists {
                    return Err(err.bail(Error::invalid_request(
                        "grant project must exist in the current silo",
                    )));
                }
            }
            _ => {
                return Err(err.bail(Error::invalid_request(
                    "silo grants must target the current silo",
                )));
            }
        }
    }
    Ok(())
}

async fn fetch_policy(
    conn: &Conn,
    silo_id: Uuid,
    id: Uuid,
    err: &OptionalError<Error>,
) -> DieselResult<FederationTrustPolicy> {
    use federation_trust_policy::dsl;
    dsl::federation_trust_policy
        .filter(dsl::id.eq(id))
        .filter(dsl::silo_id.eq(silo_id))
        .filter(dsl::time_deleted.is_null())
        .select(FederationTrustPolicy::as_select())
        .first_async(conn)
        .await
        .map_err(|e| {
            err.bail_retryable_or_else(e, |e| {
                public_error_from_diesel(e, not_found(id))
            })
        })
}

async fn load_grants(
    conn: &Conn,
    ids: &[Uuid],
) -> DieselResult<Vec<FederationRoleGrant>> {
    use federation_role_grant::dsl;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    dsl::federation_role_grant
        .filter(dsl::trust_policy_id.eq_any(ids.to_vec()))
        .select(FederationRoleGrant::as_select())
        .load_async(conn)
        .await
}

async fn insert_grants(
    conn: &Conn,
    grants: Vec<FederationRoleGrant>,
) -> DieselResult<Vec<FederationRoleGrant>> {
    if grants.is_empty() {
        return Ok(grants);
    }
    diesel::insert_into(federation_role_grant::table)
        .values(grants)
        .returning(FederationRoleGrant::as_returning())
        .get_results_async(conn)
        .await
}

impl DataStore {
    pub async fn federation_trust_policy_create(
        &self,
        opctx: &OpContext,
        params: api::FederationTrustPolicyCreate,
    ) -> Result<api::FederationTrustPolicy, Error> {
        let silo_id = authorize(opctx, authz::Action::CreateChild).await?;
        validate_federation_trust_policy(
            Some(&params.description),
            Some(&params.policy),
            Some(&params.grants),
        )?;
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("federation_trust_policy_create")
            .transaction(&conn, |conn| {
                let params = params.clone();
                let err = err.clone();
                async move {
                    let idp_id = resolve_idp(
                        &conn,
                        silo_id,
                        &params.identity_provider,
                        &err,
                    )
                    .await?;
                    let policy =
                        FederationTrustPolicy::new(silo_id, idp_id, &params);
                    let grants: Vec<_> = params
                        .grants
                        .iter()
                        .map(|grant| {
                            FederationRoleGrant::new(policy.id(), grant)
                        })
                        .collect();
                    validate_targets(&conn, silo_id, &grants, &err).await?;
                    diesel::insert_into(federation_trust_policy::table)
                        .values(policy.clone())
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Conflict(
                                        ResourceType::FederationTrustPolicy,
                                        params.name.as_str(),
                                    ),
                                )
                            })
                        })?;
                    let grants = insert_grants(&conn, grants).await?;
                    policy.into_view(grants).map_err(|e| err.bail(e))
                }
            })
            .await
            .map_err(|e| {
                err.take().unwrap_or_else(|| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })
            })
    }

    pub async fn federation_trust_policy_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> Result<Vec<api::FederationTrustPolicy>, Error> {
        let silo_id = authorize(opctx, authz::Action::ListChildren).await?;
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("federation_trust_policy_list")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    use federation_trust_policy::dsl;
                    let query = match pagparams {
                        PaginatedBy::Id(params) => paginated(
                            dsl::federation_trust_policy,
                            dsl::id,
                            params,
                        ),
                        PaginatedBy::Name(params) => paginated(
                            dsl::federation_trust_policy,
                            dsl::name,
                            &params.map_name(Name::ref_cast),
                        ),
                    };
                    let policies: Vec<FederationTrustPolicy> = query
                        .filter(dsl::silo_id.eq(silo_id))
                        .filter(dsl::time_deleted.is_null())
                        .select(FederationTrustPolicy::as_select())
                        .load_async(&conn)
                        .await?;
                    let ids: Vec<_> =
                        policies.iter().map(|policy| policy.id()).collect();
                    let mut grants_by_policy: BTreeMap<
                        Uuid,
                        Vec<FederationRoleGrant>,
                    > = BTreeMap::new();
                    for grant in load_grants(&conn, &ids).await? {
                        grants_by_policy
                            .entry(grant.trust_policy_id)
                            .or_default()
                            .push(grant);
                    }
                    policies
                        .into_iter()
                        .map(|policy| {
                            let grants = grants_by_policy
                                .remove(&policy.id())
                                .unwrap_or_default();
                            policy.into_view(grants).map_err(|e| err.bail(e))
                        })
                        .collect()
                }
            })
            .await
            .map_err(|e| {
                err.take().unwrap_or_else(|| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })
            })
    }

    pub async fn federation_trust_policy_view(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<api::FederationTrustPolicy, Error> {
        let silo_id = authorize(opctx, authz::Action::Read).await?;
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("federation_trust_policy_view")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    let policy = fetch_policy(&conn, silo_id, id, &err).await?;
                    let grants = load_grants(&conn, &[id]).await?;
                    policy.into_view(grants).map_err(|e| err.bail(e))
                }
            })
            .await
            .map_err(|e| {
                err.take().unwrap_or_else(|| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })
            })
    }

    pub async fn federation_trust_policy_update(
        &self,
        opctx: &OpContext,
        id: Uuid,
        params: api::FederationTrustPolicyUpdate,
    ) -> Result<api::FederationTrustPolicy, Error> {
        let silo_id = authorize(opctx, authz::Action::Modify).await?;
        validate_federation_trust_policy(
            params.description.as_deref(),
            params.policy.as_deref(),
            params.grants.as_deref(),
        )?;
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("federation_trust_policy_update")
            .transaction(&conn, |conn| {
                let params = params.clone();
                let err = err.clone();
                async move {
                    use federation_trust_policy::dsl;
                    let policy = fetch_policy(&conn, silo_id, id, &err).await?;
                    let idp_id = match &params.identity_provider {
                        Some(selector) => {
                            resolve_idp(&conn, silo_id, selector, &err).await?
                        }
                        None => policy.idp_id,
                    };
                    let old_grants = load_grants(&conn, &[id]).await?;
                    let new_grants = params.grants.as_ref().map(|grants| {
                        grants
                            .iter()
                            .map(|grant| FederationRoleGrant::new(id, grant))
                            .collect::<Vec<_>>()
                    });
                    let grants_changed = if let Some(grants) = &new_grants {
                        validate_targets(&conn, silo_id, grants, &err).await?;
                        grants
                            .iter()
                            .map(FederationRoleGrant::key)
                            .collect::<BTreeSet<_>>()
                            != old_grants
                                .iter()
                                .map(FederationRoleGrant::key)
                                .collect::<BTreeSet<_>>()
                    } else {
                        false
                    };
                    let policy_text =
                        params.policy.as_ref().unwrap_or(&policy.policy);
                    let changed = grants_changed
                        || idp_id != policy.idp_id
                        || *policy_text != policy.policy;
                    let revision = if changed {
                        policy.revision.checked_add(1).ok_or_else(|| {
                            err.bail(Error::internal_error(
                                "trust policy revision overflow",
                            ))
                        })?
                    } else {
                        policy.revision
                    };
                    let name = params
                        .name
                        .clone()
                        .unwrap_or_else(|| policy.name().clone());
                    let updated = diesel::update(
                        dsl::federation_trust_policy.filter(dsl::id.eq(id)),
                    )
                    .set((
                        dsl::name.eq(name.to_string()),
                        dsl::description.eq(params
                            .description
                            .as_ref()
                            .unwrap_or(&policy.identity.description)
                            .clone()),
                        dsl::time_modified.eq(Utc::now()),
                        dsl::idp_id.eq(idp_id),
                        dsl::policy.eq(policy_text.clone()),
                        dsl::revision.eq(revision),
                    ))
                    .returning(FederationTrustPolicy::as_returning())
                    .get_result_async(&conn)
                    .await
                    .map_err(|e| {
                        err.bail_retryable_or_else(e, |e| {
                            public_error_from_diesel(
                                e,
                                ErrorHandler::Conflict(
                                    ResourceType::FederationTrustPolicy,
                                    name.as_str(),
                                ),
                            )
                        })
                    })?;
                    let grants = if grants_changed {
                        diesel::delete(federation_role_grant::table.filter(
                            federation_role_grant::trust_policy_id.eq(id),
                        ))
                        .execute_async(&conn)
                        .await?;
                        insert_grants(&conn, new_grants.unwrap()).await?
                    } else {
                        old_grants
                    };
                    updated.into_view(grants).map_err(|e| err.bail(e))
                }
            })
            .await
            .map_err(|e| {
                err.take().unwrap_or_else(|| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })
            })
    }

    pub async fn federation_trust_policy_delete(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<(), Error> {
        let silo_id = authorize(opctx, authz::Action::Delete).await?;
        use federation_trust_policy::dsl;
        let now = Utc::now();
        diesel::update(dsl::federation_trust_policy)
            .filter(dsl::id.eq(id))
            .filter(dsl::silo_id.eq(silo_id))
            .filter(dsl::time_deleted.is_null())
            .set((dsl::time_deleted.eq(now), dsl::time_modified.eq(now)))
            .returning(dsl::id)
            .get_result_async::<Uuid>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, not_found(id)))?;
        Ok(())
    }
}
