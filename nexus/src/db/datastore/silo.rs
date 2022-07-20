// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`Silo`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::diesel_pool_result_optional;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::fixed_data::silo::DEFAULT_SILO;
use crate::db::identity::Resource;
use crate::db::model::Name;
use crate::db::model::Silo;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Load built-in silos into the database
    pub async fn load_builtin_silos(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        debug!(opctx.log, "attempting to create built-in silo");

        use db::schema::silo::dsl;
        let count = diesel::insert_into(dsl::silo)
            .values(&*DEFAULT_SILO)
            .on_conflict(dsl::id)
            .do_nothing()
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        info!(opctx.log, "created {} built-in silos", count);
        Ok(())
    }

    pub async fn silo_create(
        &self,
        opctx: &OpContext,
        silo: Silo,
    ) -> CreateResult<Silo> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        let silo_id = silo.id();

        use db::schema::silo::dsl;
        diesel::insert_into(dsl::silo)
            .values(silo)
            .returning(Silo::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Silo,
                        silo_id.to_string().as_str(),
                    ),
                )
            })
    }

    pub async fn silos_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Silo> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::silo::dsl;
        paginated(dsl::silo, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::discoverable.eq(true))
            .select(Silo::as_select())
            .load_async::<Silo>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn silos_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Silo> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::silo::dsl;
        paginated(dsl::silo, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::discoverable.eq(true))
            .select(Silo::as_select())
            .load_async::<Silo>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn silo_delete(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        db_silo: &db::model::Silo,
    ) -> DeleteResult {
        assert_eq!(authz_silo.id(), db_silo.id());
        opctx.authorize(authz::Action::Delete, authz_silo).await?;

        use db::schema::organization;
        use db::schema::silo;
        use db::schema::silo_user;
        use db::schema::silo_group;
        use db::schema::silo_group_membership;

        // Make sure there are no organizations present within this silo.
        let id = authz_silo.id();
        let rcgen = db_silo.rcgen;
        let org_found = diesel_pool_result_optional(
            organization::dsl::organization
                .filter(organization::dsl::silo_id.eq(id))
                .filter(organization::dsl::time_deleted.is_null())
                .select(organization::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;

        if org_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "silo to be deleted contains an organization"
                    .to_string(),
            });
        }

        let now = Utc::now();
        let updated_rows = diesel::update(silo::dsl::silo)
            .filter(silo::dsl::time_deleted.is_null())
            .filter(silo::dsl::id.eq(id))
            .filter(silo::dsl::rcgen.eq(rcgen))
            .set(silo::dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_silo),
                )
            })?;

        if updated_rows == 0 {
            return Err(Error::InvalidRequest {
                message: "silo deletion failed due to concurrent modification"
                    .to_string(),
            });
        }

        info!(opctx.log, "deleted silo {}", id);

        // If silo deletion succeeded, delete all silo users
        // TODO-correctness This needs to happen in a saga or some other
        // mechanism that ensures it happens even if we crash at this point.
        // TODO-scalability This needs to happen in batches
        let updated_rows = diesel::update(silo_user::dsl::silo_user)
            .filter(silo_user::dsl::silo_id.eq(id))
            .filter(silo_user::dsl::time_deleted.is_null())
            .set(silo_user::dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_silo),
                )
            })?;

        info!(opctx.log, "deleted {} silo users for silo {}", updated_rows, id);

        // delete all silo group memberships
        let updated_rows =
            diesel::delete(silo_group_membership::dsl::silo_group_membership)
                .filter(
                    silo_group_membership::dsl::silo_group_id.eq_any(
                        silo_group::dsl::silo_group
                            .filter(silo_group::dsl::silo_id.eq(id))
                            .filter(silo_group::dsl::time_deleted.is_not_null())
                            .select(silo_group::dsl::id),
                    ),
                )
                .execute_async(self.pool_authorized(opctx).await?)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(
                        e,
                        ErrorHandler::NotFoundByResource(authz_silo),
                    )
                })?;

        info!(
            opctx.log,
            "deleted {} silo group memberships for silo {}", updated_rows, id
        );

        // delete all silo groups
        let updated_rows = diesel::update(silo_group::dsl::silo_group)
            .filter(silo_group::dsl::silo_id.eq(id))
            .filter(silo_group::dsl::time_deleted.is_null())
            .set(silo_group::dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_silo),
                )
            })?;

        info!(
            opctx.log,
            "deleted {} silo groups for silo {}", updated_rows, id
        );

        // delete all silo identity providers
        use db::schema::identity_provider::dsl as idp_dsl;

        let updated_rows = diesel::update(idp_dsl::identity_provider)
            .filter(idp_dsl::silo_id.eq(id))
            .filter(idp_dsl::time_deleted.is_null())
            .set(idp_dsl::time_deleted.eq(Utc::now()))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_silo),
                )
            })?;

        info!(opctx.log, "deleted {} silo IdPs for silo {}", updated_rows, id);

        use db::schema::saml_identity_provider::dsl as saml_idp_dsl;

        let updated_rows = diesel::update(saml_idp_dsl::saml_identity_provider)
            .filter(saml_idp_dsl::silo_id.eq(id))
            .filter(saml_idp_dsl::time_deleted.is_null())
            .set(saml_idp_dsl::time_deleted.eq(Utc::now()))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_silo),
                )
            })?;

        info!(
            opctx.log,
            "deleted {} silo saml IdPs for silo {}", updated_rows, id
        );

        Ok(())
    }
}
