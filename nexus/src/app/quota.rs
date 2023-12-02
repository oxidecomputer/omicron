// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Resource limits and system quotas

use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_types::external_api::params;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;

impl super::Nexus {
    pub async fn quotas_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> Result<db::model::SiloQuotas, Error> {
        self.db_datastore.quotas_list(opctx, pagparams).await
    }

    pub(crate) async fn fleet_list_quotas(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::SiloQuotas> {
        self.db_datastore.fleet_list_quotas(opctx, pagparams).await
    }

    pub(crate) async fn silo_fetch_quota(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> Result<db::model::SiloQuotas, Error> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Read).await?;
        self.db_datastore.silo_fetch_quota(opctx, authz_silo).await
    }

    pub(crate) async fn silo_create_quotas(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        quotas: &params::SiloQuotasCreate,
    ) -> CreateResult<db::model::SiloQuotas> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore.silo_create_quota(opctx, authz_silo, quotas).await
    }

    pub(crate) async fn silo_update_quota(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        updates: &params::QuotaUpdate,
    ) -> Result<db::model::Quota> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore.silo_update_quota(opctx, authz_silo, updates).await
    }

    pub(crate) async fn silo_quota_delete(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> Result<db::model::Quota> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Delete).await?;
        self.db_datastore.silo_quota_delete(opctx, authz_silo).await
    }
}
