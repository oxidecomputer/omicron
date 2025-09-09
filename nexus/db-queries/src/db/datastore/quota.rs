use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::SiloQuotas;
use nexus_db_model::SiloQuotasUpdate;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

fn constraint_to_error(constraint: &str) -> &str {
    match constraint {
        "cpus_not_negative" => "CPU quota must not be negative",
        "memory_not_negative" => "Memory quota must not be negative",
        "storage_not_negative" => "Storage quota must not be negative",
        _ => "Unknown constraint",
    }
}

impl DataStore {
    /// Creates new quotas for a silo. This is grouped with silo creation
    /// and shouldn't be called outside of that flow.
    ///
    /// An authz check _cannot_ be performed here because the authz initialization
    /// isn't complete and will lead to a db deadlock.
    ///
    /// See <https://github.com/oxidecomputer/omicron/blob/07eb7dafc20e35e44edf429fcbb759cbb33edd5f/nexus/db-queries/src/db/datastore/rack.rs#L407-L410>
    pub async fn silo_quotas_create(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_silo: &authz::Silo,
        quotas: SiloQuotas,
    ) -> Result<(), Error> {
        let silo_id = authz_silo.id();
        use nexus_db_schema::schema::silo_quotas::dsl;

        diesel::insert_into(dsl::silo_quotas)
            .values(quotas)
            .execute_async(conn)
            .await
            .map_err(|e| match e {
                DieselError::DatabaseError(
                    DatabaseErrorKind::CheckViolation,
                    ref info,
                ) => {
                    let msg = match info.constraint_name() {
                        Some(constraint) => constraint_to_error(constraint),
                        None => "Missing constraint name for Check Violation",
                    };
                    Error::invalid_request(&format!(
                        "Cannot create silo quota: {msg}"
                    ))
                }
                _ => public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SiloQuotas,
                        &silo_id.to_string(),
                    ),
                ),
            })
            .map(|_| ())
    }

    pub async fn silo_quotas_delete(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_silo: &authz::Silo,
    ) -> DeleteResult {
        // Given that the quotas right now are somewhat of an extension of the
        // Silo we just check for delete permission on the silo itself.
        opctx.authorize(authz::Action::Delete, authz_silo).await?;

        use nexus_db_schema::schema::silo_quotas::dsl;
        diesel::delete(dsl::silo_quotas)
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .execute_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    pub async fn silo_update_quota(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        updates: SiloQuotasUpdate,
    ) -> UpdateResult<SiloQuotas> {
        opctx.authorize(authz::Action::Modify, authz_silo).await?;
        use nexus_db_schema::schema::silo_quotas::dsl;
        let silo_id = authz_silo.id();
        diesel::update(dsl::silo_quotas)
            .filter(dsl::silo_id.eq(silo_id))
            .set(updates)
            .returning(SiloQuotas::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| match e {
                DieselError::DatabaseError(
                    DatabaseErrorKind::CheckViolation,
                    ref info,
                ) => {
                    let msg = match info.constraint_name() {
                        Some(constraint) => constraint_to_error(constraint),
                        None => "Missing constraint name for Check Violation",
                    };
                    Error::invalid_request(&format!(
                        "Cannot update silo quota: {msg}"
                    ))
                }
                _ => public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SiloQuotas,
                        &silo_id.to_string(),
                    ),
                ),
            })
    }

    pub async fn silo_quotas_view(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
    ) -> Result<SiloQuotas, Error> {
        opctx.authorize(authz::Action::Read, authz_silo).await?;
        use nexus_db_schema::schema::silo_quotas::dsl;
        dsl::silo_quotas
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn fleet_list_quotas(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SiloQuotas> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use nexus_db_schema::schema::silo_quotas::dsl;
        paginated(dsl::silo_quotas, dsl::silo_id, pagparams)
            .select(SiloQuotas::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
