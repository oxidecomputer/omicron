use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Name;
use crate::db::model::SiloUtilization;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::BoolExpressionMethods;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use ref_cast::RefCast;

impl DataStore {
    pub async fn silo_utilization_view(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
    ) -> Result<SiloUtilization, Error> {
        opctx.authorize(authz::Action::Read, authz_silo).await?;
        let silo_id = authz_silo.id();

        use db::schema::silo_utilization::dsl;
        dsl::silo_utilization
            .filter(dsl::silo_id.eq(silo_id))
            .select(SiloUtilization::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn silo_utilization_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<SiloUtilization> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::silo_utilization::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::silo_utilization, dsl::silo_id, pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::silo_utilization,
                dsl::silo_name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .select(SiloUtilization::as_select())
        .filter(
            dsl::silo_discoverable
                .eq(true)
                .or(dsl::cpus_allocated.gt(0))
                .or(dsl::memory_allocated.gt(0))
                .or(dsl::storage_allocated.gt(0)),
        )
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
