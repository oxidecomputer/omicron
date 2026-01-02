use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::Name;
use crate::db::model::SiloUtilization;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::BoolExpressionMethods;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_types::silo::DEFAULT_SILO_ID;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::http_pagination::PaginatedBy;
use ref_cast::RefCast;

impl DataStore {
    pub async fn silo_utilization_view(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
    ) -> Result<SiloUtilization, Error> {
        opctx.authorize(authz::Action::Read, authz_silo).await?;
        let silo_id = authz_silo.id();

        use nexus_db_schema::schema::silo_utilization::dsl;
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
        use nexus_db_schema::schema::silo_utilization::dsl;
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
        // Filter out default silo from utilization response by its well-known
        // ID because it has gigantic quotas that confuse everyone. The proper
        // solution will be to eliminate the default silo altogether, but this
        // is dramatically easier.
        // See https://github.com/oxidecomputer/omicron/issues/5731
        .filter(dsl::silo_id.ne(DEFAULT_SILO_ID))
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
