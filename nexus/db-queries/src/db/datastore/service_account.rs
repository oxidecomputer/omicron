// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::identity::Resource;
use crate::db::model::IdentityType;
use crate::db::model::Name;
use crate::db::model::ServiceAccount;
use crate::db::model::ServiceAccountKey;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::GenericUuid;
use ref_cast::RefCast;
use uuid::Uuid;

impl DataStore {
    pub async fn service_account_create(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        service_account: ServiceAccount,
    ) -> CreateResult<ServiceAccount> {
        use nexus_db_schema::schema::service_account::dsl;

        opctx.authorize(authz::Action::Modify, authz_project).await?;

        let name = service_account.name().as_str().to_string();
        diesel::insert_into(dsl::service_account)
            .values(service_account)
            .returning(ServiceAccount::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(ResourceType::ServiceAccount, &name),
                )
            })
    }

    pub async fn service_account_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<ServiceAccount> {
        use nexus_db_schema::schema::service_account::dsl;

        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::service_account, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::service_account,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::project_id.eq(authz_project.id().into_untyped_uuid()))
        .filter(dsl::time_deleted.is_null())
        .select(ServiceAccount::as_select())
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn service_account_delete(
        &self,
        opctx: &OpContext,
        authz_service_account: &authz::ServiceAccount,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, authz_service_account).await?;

        let id = authz_service_account.id();
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("service_account_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    let now = Utc::now();
                    {
                        use nexus_db_schema::schema::service_account::dsl;
                        diesel::update(dsl::service_account)
                            .filter(dsl::id.eq(id))
                            .filter(dsl::time_deleted.is_null())
                            .set(dsl::time_deleted.eq(now))
                            .execute_async(&conn)
                            .await
                            .map_err(|e| {
                                err.bail_retryable_or_else(e, |e| {
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::NotFoundByResource(
                                            authz_service_account,
                                        ),
                                    )
                                })
                            })?;
                    }
                    {
                        use nexus_db_schema::schema::service_account_key::dsl;
                        diesel::update(dsl::service_account_key)
                            .filter(dsl::service_account_id.eq(id))
                            .filter(dsl::time_deleted.is_null())
                            .set(dsl::time_deleted.eq(now))
                            .execute_async(&conn)
                            .await
                            .map_err(|e| {
                                err.bail_retryable_or_else(e, |e| {
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                })
                            })?;
                    }
                    {
                        use nexus_db_schema::schema::role_assignment::dsl;
                        diesel::delete(dsl::role_assignment)
                            .filter(dsl::identity_id.eq(id))
                            .filter(
                                dsl::identity_type
                                    .eq(IdentityType::ServiceAccount),
                            )
                            .execute_async(&conn)
                            .await
                            .map_err(|e| {
                                err.bail_retryable_or_else(e, |e| {
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                })
                            })?;
                    }
                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    pub async fn service_account_key_create(
        &self,
        opctx: &OpContext,
        authz_service_account: &authz::ServiceAccount,
        key: ServiceAccountKey,
    ) -> CreateResult<ServiceAccountKey> {
        use nexus_db_schema::schema::service_account_key::dsl;

        opctx
            .authorize(authz::Action::CreateChild, authz_service_account)
            .await?;

        let name = key.name().as_str().to_string();
        diesel::insert_into(dsl::service_account_key)
            .values(key)
            .returning(ServiceAccountKey::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::ServiceAccountKey,
                        &name,
                    ),
                )
            })
    }

    pub async fn service_account_key_list(
        &self,
        opctx: &OpContext,
        authz_service_account: &authz::ServiceAccount,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<ServiceAccountKey> {
        use nexus_db_schema::schema::service_account_key::dsl;

        opctx
            .authorize(authz::Action::ListChildren, authz_service_account)
            .await?;

        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::service_account_key, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::service_account_key,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::service_account_id.eq(authz_service_account.id()))
        .filter(dsl::time_deleted.is_null())
        .select(ServiceAccountKey::as_select())
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn service_account_key_delete(
        &self,
        opctx: &OpContext,
        authz_key: &authz::ServiceAccountKey,
    ) -> DeleteResult {
        use nexus_db_schema::schema::service_account_key::dsl;

        opctx.authorize(authz::Action::Modify, authz_key).await?;

        diesel::update(dsl::service_account_key)
            .filter(dsl::id.eq(authz_key.id()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_key),
                )
            })?;
        Ok(())
    }

    pub async fn service_account_fetch_by_id(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> LookupResult<ServiceAccount> {
        use nexus_db_schema::schema::service_account::dsl;
        dsl::service_account
            .filter(dsl::id.eq(id))
            .filter(dsl::time_deleted.is_null())
            .select(ServiceAccount::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::ServiceAccount,
                        omicron_common::api::external::LookupType::ById(id),
                    ),
                )
            })
    }

    pub async fn project_ensure_no_service_accounts(
        &self,
        opctx: &OpContext,
        project_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::service_account::dsl;
        let found = dsl::service_account
            .filter(dsl::project_id.eq(project_id))
            .filter(dsl::time_deleted.is_null())
            .select(dsl::id)
            .first_async::<Uuid>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if found.is_some() {
            return Err(Error::invalid_request(
                "project to be deleted contains service accounts",
            ));
        }
        Ok(())
    }
}
