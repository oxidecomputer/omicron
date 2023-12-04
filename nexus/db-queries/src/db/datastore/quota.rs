use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_model::SiloQuotas;
use nexus_types::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl DataStore {
    /// Creates new quotas for a silo. This is grouped with silo creation
    /// and shouldn't be called directly by the user.
    pub async fn silo_quotas_create(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        quotas: SiloQuotas,
    ) -> CreateResult<SiloQuotas> {
        opctx.authorize(authz::Action::Modify, authz_silo).await?;
        let silo_id = authz_silo.id();
        use db::schema::silo_quotas::dsl;

        diesel::insert_into(dsl::silo_quotas)
            .values(quotas)
            .returning(SiloQuotas::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SiloQuotas,
                        &silo_id.to_string(),
                    ),
                )
            })
    }

    pub async fn silo_update_quota(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        updates: params::SiloQuotasUpdate,
    ) -> UpdateResult<SiloQuotas> {
        opctx.authorize(authz::Action::Modify, authz_silo).await?;
        use db::schema::silo_quotas::dsl;
        let silo_id = authz_silo.id();
        diesel::update(dsl::silo_quotas)
            .filter(dsl::silo_id.eq(silo_id))
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::cpus.eq(updates.cpus),
                dsl::memory.eq(updates.memory),
                dsl::storage.eq(updates.storage),
            ))
            .returning(SiloQuotas::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SiloQuotas,
                        &silo_id.to_string(),
                    ),
                )
            })
    }

    pub async fn silo_quotas_view(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
    ) -> Result<SiloQuotas, Error> {
        opctx.authorize(authz::Action::Read, authz_silo).await?;
        use db::schema::silo_quotas::dsl;
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
        use db::schema::silo_quotas::dsl;
        paginated(dsl::silo_quotas, dsl::silo_id, pagparams)
            .select(SiloQuotas::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
