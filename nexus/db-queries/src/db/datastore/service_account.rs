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
use nexus_db_model::{Name, ServiceAccount, ServiceAccountGrant};
use nexus_db_schema::schema::{
    federation_identity_provider, project, service_account,
    service_account_grant, silo,
};
use nexus_types::external_api::service_account as api;
use nexus_types::identity::Resource;
use omicron_common::api::external::Nullable;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    Error, InternalContext, LookupType, NameOrId, ResourceType,
};
use ref_cast::RefCast;
use std::collections::BTreeMap;
use uuid::Uuid;

type Conn = async_bb8_diesel::Connection<DbConnection>;
type DieselResult<T> = Result<T, diesel::result::Error>;

#[derive(Clone)]
enum Parent {
    Silo(authz::Silo),
    Project(authz::Project),
}

impl Parent {
    fn id(&self) -> Uuid {
        match self {
            Self::Silo(silo) => silo.id(),
            Self::Project(project) => project.id(),
        }
    }

    fn scope(&self) -> &'static str {
        match self {
            Self::Silo(_) => "silo",
            Self::Project(_) => "project",
        }
    }

    async fn authorize(
        &self,
        opctx: &OpContext,
        action: authz::Action,
    ) -> Result<(), Error> {
        match self {
            Self::Silo(silo) => {
                opctx
                    .authorize(
                        action,
                        &authz::SiloServiceAccountList::new(silo.clone()),
                    )
                    .await
            }
            Self::Project(project) => {
                opctx
                    .authorize(
                        action,
                        &authz::ProjectServiceAccountList::new(project.clone()),
                    )
                    .await
            }
        }
    }
}

fn lookup(selector: &NameOrId) -> LookupType {
    match selector {
        NameOrId::Id(id) => LookupType::ById(*id),
        NameOrId::Name(name) => LookupType::ByName(name.to_string()),
    }
}

fn not_found(selector: &NameOrId) -> ErrorHandler<'static> {
    ErrorHandler::NotFoundByLookup(
        ResourceType::ServiceAccount,
        lookup(selector),
    )
}

async fn resolve_parent(
    conn: &Conn,
    opctx: &OpContext,
    scope: api::ServiceAccountScope,
    selector: &NameOrId,
) -> Result<Parent, Error> {
    let authz_silo = opctx
        .authn
        .silo_required()
        .internal_context("managing service accounts")?;
    match scope {
        api::ServiceAccountScope::Silo => {
            use silo::dsl;
            let mut query = dsl::silo
                .filter(dsl::id.eq(authz_silo.id()))
                .filter(dsl::time_deleted.is_null())
                .into_boxed();
            query = match selector {
                NameOrId::Id(id) => query.filter(dsl::id.eq(*id)),
                NameOrId::Name(name) => {
                    query.filter(dsl::name.eq(name.to_string()))
                }
            };
            query.select(dsl::id).first_async::<Uuid>(conn).await.map_err(
                |e| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByLookup(
                            ResourceType::Silo,
                            lookup(selector),
                        ),
                    )
                },
            )?;
            Ok(Parent::Silo(authz_silo))
        }
        api::ServiceAccountScope::Project => {
            use project::dsl;
            let mut query = dsl::project
                .filter(dsl::silo_id.eq(authz_silo.id()))
                .filter(dsl::time_deleted.is_null())
                .into_boxed();
            query = match selector {
                NameOrId::Id(id) => query.filter(dsl::id.eq(*id)),
                NameOrId::Name(name) => {
                    query.filter(dsl::name.eq(name.to_string()))
                }
            };
            let id =
                query.select(dsl::id).first_async::<Uuid>(conn).await.map_err(
                    |e| {
                        public_error_from_diesel(
                            e,
                            ErrorHandler::NotFoundByLookup(
                                ResourceType::Project,
                                lookup(selector),
                            ),
                        )
                    },
                )?;
            Ok(Parent::Project(authz::Project::new(
                authz_silo,
                id,
                lookup(selector),
            )))
        }
    }
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
    query = match selector {
        NameOrId::Id(id) => query.filter(dsl::id.eq(*id)),
        NameOrId::Name(name) => query.filter(dsl::name.eq(name.to_string())),
    };
    query.select(dsl::id).first_async(conn).await.map_err(|e| {
        err.bail_retryable_or_else(e, |e| {
            public_error_from_diesel(
                e,
                ErrorHandler::NotFoundByLookup(
                    ResourceType::FederationIdentityProvider,
                    lookup(selector),
                ),
            )
        })
    })
}

async fn validate_targets(
    conn: &Conn,
    silo_id: Uuid,
    parent: &Parent,
    grants: &[ServiceAccountGrant],
    err: &OptionalError<Error>,
) -> DieselResult<()> {
    for grant in grants {
        if matches!(parent, Parent::Project(_))
            && (grant.resource_kind != "project"
                || grant.resource_id != parent.id())
        {
            return Err(err.bail(Error::invalid_request(
                "project-scoped service account grants must target the owning project",
            )));
        }
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
                    "silo grants must target the owning silo",
                )));
            }
        }
    }
    Ok(())
}

async fn load_grants(
    conn: &Conn,
    ids: &[Uuid],
) -> DieselResult<Vec<ServiceAccountGrant>> {
    use service_account_grant::dsl;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    dsl::service_account_grant
        .filter(dsl::service_account_id.eq_any(ids.to_vec()))
        .select(ServiceAccountGrant::as_select())
        .load_async(conn)
        .await
}

async fn insert_grants(
    conn: &Conn,
    grants: Vec<ServiceAccountGrant>,
) -> DieselResult<Vec<ServiceAccountGrant>> {
    if grants.is_empty() {
        return Ok(grants);
    }
    diesel::insert_into(service_account_grant::table)
        .values(grants)
        .returning(ServiceAccountGrant::as_returning())
        .get_results_async(conn)
        .await
}

async fn parent_exists(conn: &Conn, parent: &Parent) -> DieselResult<bool> {
    match parent {
        Parent::Silo(_) => {
            diesel::select(diesel::dsl::exists(
                silo::table
                    .filter(silo::id.eq(parent.id()))
                    .filter(silo::time_deleted.is_null()),
            ))
            .get_result_async(conn)
            .await
        }
        Parent::Project(_) => {
            diesel::select(diesel::dsl::exists(
                project::table
                    .filter(project::id.eq(parent.id()))
                    .filter(project::time_deleted.is_null()),
            ))
            .get_result_async(conn)
            .await
        }
    }
}

pub(super) async fn has_service_accounts(
    conn: &Conn,
    scope: &str,
    resource_id: Uuid,
) -> DieselResult<bool> {
    diesel::select(diesel::dsl::exists(
        service_account::table
            .filter(service_account::scope.eq(scope.to_owned()))
            .filter(service_account::resource_id.eq(resource_id))
            .filter(service_account::time_deleted.is_null()),
    ))
    .get_result_async(conn)
    .await
}

async fn fetch_account(
    conn: &Conn,
    parent: &Parent,
    selector: &NameOrId,
) -> DieselResult<ServiceAccount> {
    use service_account::dsl;
    let mut query = dsl::service_account
        .filter(dsl::scope.eq(parent.scope()))
        .filter(dsl::resource_id.eq(parent.id()))
        .filter(dsl::time_deleted.is_null())
        .into_boxed();
    query = match selector {
        NameOrId::Id(id) => query.filter(dsl::id.eq(*id)),
        NameOrId::Name(name) => query.filter(dsl::name.eq(name.to_string())),
    };
    query.select(ServiceAccount::as_select()).first_async(conn).await
}

async fn account_parent(
    conn: &Conn,
    opctx: &OpContext,
    scope: api::ServiceAccountScope,
    selector: &api::ServiceAccountParentSelector,
    account: &NameOrId,
) -> Result<Parent, Error> {
    let explicit = selector.for_scope(scope)?;
    if let NameOrId::Id(id) = account {
        use service_account::dsl;
        let scope_name = match scope {
            api::ServiceAccountScope::Silo => "silo",
            api::ServiceAccountScope::Project => "project",
        };
        let resource_id = dsl::service_account
            .filter(dsl::id.eq(*id))
            .filter(dsl::scope.eq(scope_name))
            .filter(dsl::time_deleted.is_null())
            .select(dsl::resource_id)
            .first_async::<Uuid>(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, not_found(account)))?;
        let parent =
            resolve_parent(conn, opctx, scope, &NameOrId::Id(resource_id))
                .await?;
        if let Some(explicit) = explicit {
            let requested =
                resolve_parent(conn, opctx, scope, explicit).await?;
            if requested.id() != parent.id() {
                return Err(Error::invalid_request(
                    "service account does not belong to the selected parent",
                ));
            }
        }
        Ok(parent)
    } else {
        resolve_parent(conn, opctx, scope, selector.required_for_scope(scope)?)
            .await
    }
}

impl DataStore {
    pub async fn service_account_create(
        &self,
        opctx: &OpContext,
        scope: api::ServiceAccountScope,
        selector: api::ServiceAccountParentSelector,
        params: api::ServiceAccountCreate,
    ) -> Result<api::ServiceAccount, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let parent = resolve_parent(
            &conn,
            opctx,
            scope,
            selector.required_for_scope(scope)?,
        )
        .await?;
        parent.authorize(opctx, authz::Action::CreateChild).await?;
        nexus_db_model::validate_service_account(
            Some(&params.description),
            params.federation.as_ref().map(|f| f.policy.as_str()),
            Some(&params.grants),
        )?;
        let silo_id = opctx
            .authn
            .silo_required()
            .internal_context("creating service account")?
            .id();
        let err = OptionalError::new();
        self.transaction_retry_wrapper("service_account_create")
            .transaction(&conn, |conn| {
                let params = params.clone();
                let parent = parent.clone();
                let err = err.clone();
                async move {
                    if !parent_exists(&conn, &parent).await? {
                        return Err(err.bail(Error::invalid_request(
                            "service account parent was deleted",
                        )));
                    }
                    let idp = match &params.federation {
                        Some(f) => Some(
                            resolve_idp(
                                &conn,
                                silo_id,
                                &f.identity_provider,
                                &err,
                            )
                            .await?,
                        ),
                        None => None,
                    };
                    let account =
                        ServiceAccount::new(scope, parent.id(), idp, &params);
                    let grants = params
                        .grants
                        .iter()
                        .map(|g| ServiceAccountGrant::new(account.id(), g))
                        .collect::<Vec<_>>();
                    validate_targets(&conn, silo_id, &parent, &grants, &err)
                        .await?;
                    diesel::insert_into(service_account::table)
                        .values(account.clone())
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Conflict(
                                        ResourceType::ServiceAccount,
                                        params.name.as_str(),
                                    ),
                                )
                            })
                        })?;
                    let grants = insert_grants(&conn, grants).await?;
                    account.into_view(grants).map_err(|e| err.bail(e))
                }
            })
            .await
            .map_err(|e| {
                err.take().unwrap_or_else(|| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })
            })
    }

    pub async fn service_account_list(
        &self,
        opctx: &OpContext,
        scope: api::ServiceAccountScope,
        selector: api::ServiceAccountParentSelector,
        pagparams: &PaginatedBy<'_>,
    ) -> Result<Vec<api::ServiceAccount>, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let parent = resolve_parent(
            &conn,
            opctx,
            scope,
            selector.required_for_scope(scope)?,
        )
        .await?;
        parent.authorize(opctx, authz::Action::ListChildren).await?;
        let err = OptionalError::new();
        self.transaction_retry_wrapper("service_account_list")
            .transaction(&conn, |conn| {
                let parent = parent.clone();
                let err = err.clone();
                async move {
                    use service_account::dsl;
                    let query = match pagparams {
                        PaginatedBy::Id(params) => {
                            paginated(dsl::service_account, dsl::id, params)
                        }
                        PaginatedBy::Name(params) => paginated(
                            dsl::service_account,
                            dsl::name,
                            &params.map_name(Name::ref_cast),
                        ),
                    };
                    let accounts = query
                        .filter(dsl::scope.eq(parent.scope()))
                        .filter(dsl::resource_id.eq(parent.id()))
                        .filter(dsl::time_deleted.is_null())
                        .select(ServiceAccount::as_select())
                        .load_async(&conn)
                        .await?;
                    let ids =
                        accounts.iter().map(|a| a.id()).collect::<Vec<_>>();
                    let mut grants_by_account: BTreeMap<
                        Uuid,
                        Vec<ServiceAccountGrant>,
                    > = BTreeMap::new();
                    for grant in load_grants(&conn, &ids).await? {
                        grants_by_account
                            .entry(grant.service_account_id)
                            .or_default()
                            .push(grant);
                    }
                    accounts
                        .into_iter()
                        .map(|a| {
                            let grants = grants_by_account
                                .remove(&a.id())
                                .unwrap_or_default();
                            a.into_view(grants).map_err(|e| err.bail(e))
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

    pub async fn service_account_view(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountPath,
        selector: api::ServiceAccountParentSelector,
    ) -> Result<api::ServiceAccount, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let parent = account_parent(
            &conn,
            opctx,
            path.scope,
            &selector,
            &path.service_account,
        )
        .await?;
        parent.authorize(opctx, authz::Action::Read).await?;
        let err = OptionalError::new();
        self.transaction_retry_wrapper("service_account_view")
            .transaction(&conn, |conn| {
                let parent = parent.clone();
                let selector = path.service_account.clone();
                let err = err.clone();
                async move {
                    let account = fetch_account(&conn, &parent, &selector)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    not_found(&selector),
                                )
                            })
                        })?;
                    let grants = load_grants(&conn, &[account.id()]).await?;
                    account.into_view(grants).map_err(|e| err.bail(e))
                }
            })
            .await
            .map_err(|e| {
                err.take().unwrap_or_else(|| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })
            })
    }

    pub async fn service_account_update(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountPath,
        selector: api::ServiceAccountParentSelector,
        params: api::ServiceAccountUpdate,
    ) -> Result<api::ServiceAccount, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let parent = account_parent(
            &conn,
            opctx,
            path.scope,
            &selector,
            &path.service_account,
        )
        .await?;
        parent.authorize(opctx, authz::Action::Modify).await?;
        nexus_db_model::validate_service_account(
            params.description.as_deref(),
            params
                .federation
                .as_ref()
                .and_then(|f| f.0.as_ref())
                .map(|f| f.policy.as_str()),
            params.grants.as_deref(),
        )?;
        let silo_id = opctx
            .authn
            .silo_required()
            .internal_context("updating service account")?
            .id();
        let err = OptionalError::new();
        self.transaction_retry_wrapper("service_account_update")
            .transaction(&conn, |conn| {
                let parent = parent.clone();
                let selector = path.service_account.clone();
                let params = params.clone();
                let err = err.clone();
                async move {
                    use service_account::dsl;
                    let account = fetch_account(&conn, &parent, &selector)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    not_found(&selector),
                                )
                            })
                        })?;
                    let (idp, policy, ttl) = match &params.federation {
                        Some(Nullable(Some(f))) => (
                            Some(
                                resolve_idp(
                                    &conn,
                                    silo_id,
                                    &f.identity_provider,
                                    &err,
                                )
                                .await?,
                            ),
                            Some(f.policy.clone()),
                            i64::from(f.max_ttl_seconds.get()),
                        ),
                        Some(Nullable(None)) => (
                            None,
                            None,
                            account.federation_token_max_ttl_seconds,
                        ),
                        None => (
                            account.identity_provider_id,
                            account.trust_policy.clone(),
                            account.federation_token_max_ttl_seconds,
                        ),
                    };
                    let generation = if idp != account.identity_provider_id
                        || policy != account.trust_policy
                    {
                        account
                            .federation_generation
                            .checked_next()
                            .ok_or_else(|| {
                                err.bail(Error::internal_error(
                                    "federation generation overflow",
                                ))
                            })?
                            .into()
                    } else {
                        account.federation_generation
                    };
                    let grants = match &params.grants {
                        Some(grants) => {
                            let grants = grants
                                .iter()
                                .map(|g| {
                                    ServiceAccountGrant::new(account.id(), g)
                                })
                                .collect::<Vec<_>>();
                            validate_targets(
                                &conn, silo_id, &parent, &grants, &err,
                            )
                            .await?;
                            diesel::delete(
                                service_account_grant::table.filter(
                                    service_account_grant::service_account_id
                                        .eq(account.id()),
                                ),
                            )
                            .execute_async(&conn)
                            .await?;
                            insert_grants(&conn, grants).await?
                        }
                        None => load_grants(&conn, &[account.id()]).await?,
                    };
                    let name =
                        params.name.as_ref().unwrap_or_else(|| account.name());
                    let updated = diesel::update(
                        dsl::service_account.filter(dsl::id.eq(account.id())),
                    )
                    .set((
                        dsl::name.eq(name.to_string()),
                        dsl::description.eq(params
                            .description
                            .as_ref()
                            .unwrap_or(&account.identity.description)
                            .clone()),
                        dsl::time_modified.eq(Utc::now()),
                        dsl::identity_provider_id.eq(idp),
                        dsl::trust_policy.eq(policy),
                        dsl::federation_token_max_ttl_seconds.eq(ttl),
                        dsl::federation_generation.eq(generation),
                    ))
                    .returning(ServiceAccount::as_returning())
                    .get_result_async(&conn)
                    .await
                    .map_err(|e| {
                        err.bail_retryable_or_else(e, |e| {
                            public_error_from_diesel(
                                e,
                                ErrorHandler::Conflict(
                                    ResourceType::ServiceAccount,
                                    name.as_str(),
                                ),
                            )
                        })
                    })?;
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

    pub async fn service_account_delete(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountPath,
        selector: api::ServiceAccountParentSelector,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let parent = account_parent(
            &conn,
            opctx,
            path.scope,
            &selector,
            &path.service_account,
        )
        .await?;
        parent.authorize(opctx, authz::Action::Delete).await?;
        let err = OptionalError::new();
        self.transaction_retry_wrapper("service_account_delete")
            .transaction(&conn, |conn| {
                let parent = parent.clone();
                let selector = path.service_account.clone();
                let err = err.clone();
                async move {
                    let account = fetch_account(&conn, &parent, &selector)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    not_found(&selector),
                                )
                            })
                        })?;
                    let now = Utc::now();
                    diesel::update(
                        service_account::table
                            .filter(service_account::id.eq(account.id())),
                    )
                    .set((
                        service_account::time_deleted.eq(now),
                        service_account::time_modified.eq(now),
                    ))
                    .execute_async(&conn)
                    .await?;
                    Ok(())
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
