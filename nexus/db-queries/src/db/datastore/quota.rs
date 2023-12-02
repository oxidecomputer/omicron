use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use chrono::Utc;
use diesel::upsert::excluded;
use nexus_db_model::SiloQuotas;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;

impl DataStore {
    pub async fn silo_quotas_upsert(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        quotas: SiloQuotas,
    ) -> CreateResult<SiloQuotas> {
        opctx.authorize(authz::Action::Modify, authz_silo).await?;
        let silo_id = authz_silo.id();
        use db::schema::silo_quotas::dsl;

        let now = Utc::now();
        let quota_db = SiloQuotas::insert_resource(
            silo_id,
            diesel::insert_into(dsl::silo_quotas)
                .values(quotas.clone())
                .on_conflict(dsl::silo_id)
                .do_update()
                .set((
                    dsl::time_created.eq(excluded(dsl::time_created)),
                    dsl::time_modified.eq(now),
                )),
        )
        .insert_and_get_result_async(
            &*self.pool_connection_authorized(&opctx).await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::SiloQuotas,
                lookup_type: LookupType::ById(silo_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
        })?;

        Ok(quota_db)
    }

    pub async fn quotas_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Quota> {
        opctx
            .authorize(authz::Action::ListChildren, authz::Resource::FLEET)
            .await?;
        let mut query = db::model::Quota::query();
        query = query.filter(db::schema::quotas::silo_id.eq(opctx.silo_id));
        query = query.order_by(db::schema::quotas::id.asc());
        query = pagparams.paginate(query);
        query
            .load_async::<db::model::Quota>(&self.pool)
            .await
            .map_err(Error::from)
    }

    pub async fn fleet_list_quotas(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Quota> {
        opctx
            .authorize(authz::Action::ListChildren, authz::Resource::FLEET)
            .await?;
        let mut query = db::model::Quota::query();
        query = query.filter(db::schema::quotas::silo_id.eq(opctx.silo_id));
        query = query.order_by(db::schema::quotas::id.asc());
        query = pagparams.paginate(query);
        query
            .load_async::<db::model::Quota>(&self.pool)
            .await
            .map_err(Error::from)
    }
}
