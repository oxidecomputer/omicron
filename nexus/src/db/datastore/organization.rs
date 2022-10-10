// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Organization`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::diesel_pool_result_optional;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::identity::Resource;
use crate::db::model::CollectionType;
use crate::db::model::Name;
use crate::db::model::Organization;
use crate::db::model::OrganizationUpdate;
use crate::db::model::Silo;
use crate::db::model::VirtualResourceProvisioning;
use crate::db::pagination::paginated;
use crate::external_api::params;
use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl};
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl DataStore {
    /// Create a organization
    pub async fn organization_create(
        &self,
        opctx: &OpContext,
        organization: &params::OrganizationCreate,
    ) -> CreateResult<Organization> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("creating an Organization")?;
        opctx.authorize(authz::Action::CreateChild, &authz_silo).await?;

        use db::schema::organization::dsl;
        let silo_id = authz_silo.id();
        let organization = Organization::new(organization.clone(), silo_id);
        let name = organization.name().as_str().to_string();

        self.pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                let org = Silo::insert_resource(
                    silo_id,
                    diesel::insert_into(dsl::organization).values(organization),
                )
                .insert_and_get_result_async(&conn)
                .await
                .map_err(|e| match e {
                    AsyncInsertError::CollectionNotFound => {
                        Error::InternalError {
                            internal_message: format!(
                                "attempting to create an \
                            organization under non-existent silo {}",
                                silo_id
                            ),
                        }
                    }
                    AsyncInsertError::DatabaseError(e) => {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::Conflict(
                                ResourceType::Organization,
                                &name,
                            ),
                        )
                    }
                })?;

                self.virtual_resource_provisioning_create_on_connection(
                    &conn,
                    VirtualResourceProvisioning::new(
                        org.id(),
                        CollectionType::Organization,
                    ),
                )
                .await?;

                Ok(org)
            })
            .await
            .map_err(|e| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })
    }

    /// Delete a organization
    pub async fn organization_delete(
        &self,
        opctx: &OpContext,
        authz_org: &authz::Organization,
        db_org: &db::model::Organization,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_org).await?;

        use db::schema::organization::dsl;
        use db::schema::project;

        // Make sure there are no projects present within this organization.
        let project_found = diesel_pool_result_optional(
            project::dsl::project
                .filter(project::dsl::organization_id.eq(authz_org.id()))
                .filter(project::dsl::time_deleted.is_null())
                .select(project::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        if project_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "organization to be deleted contains a project"
                    .to_string(),
            });
        }

        let now = Utc::now();
        let updated_rows = diesel::update(dsl::organization)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_org.id()))
            .filter(dsl::rcgen.eq(db_org.rcgen))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_org),
                )
            })?;

        if updated_rows == 0 {
            return Err(Error::InvalidRequest {
                message: "deletion failed due to concurrent modification"
                    .to_string(),
            });
        }
        Ok(())
    }

    pub async fn organizations_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Organization> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("listing Organizations")?;
        opctx.authorize(authz::Action::ListChildren, &authz_silo).await?;

        use db::schema::organization::dsl;
        paginated(dsl::organization, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .select(Organization::as_select())
            .load_async::<Organization>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn organizations_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Organization> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("listing Organizations")?;
        opctx.authorize(authz::Action::ListChildren, &authz_silo).await?;

        use db::schema::organization::dsl;
        paginated(dsl::organization, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .select(Organization::as_select())
            .load_async::<Organization>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Updates a organization by name (clobbering update -- no etag)
    pub async fn organization_update(
        &self,
        opctx: &OpContext,
        authz_org: &authz::Organization,
        updates: OrganizationUpdate,
    ) -> UpdateResult<Organization> {
        use db::schema::organization::dsl;

        opctx.authorize(authz::Action::Modify, authz_org).await?;
        diesel::update(dsl::organization)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_org.id()))
            .set(updates)
            .returning(Organization::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_org),
                )
            })
    }
}
