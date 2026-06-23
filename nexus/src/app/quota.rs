// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Resource limits and system quotas

use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_types_versions::v2025_11_20_00;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl super::Nexus {
    pub async fn silo_quotas_view(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> Result<db::model::SiloQuotas, Error> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Read).await?;
        self.db_datastore.silo_quotas_view(opctx, &authz_silo).await
    }

    pub(crate) async fn fleet_list_quotas(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::SiloQuotas> {
        self.db_datastore.fleet_list_quotas(opctx, pagparams).await
    }

    pub(crate) async fn silo_update_quota(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        updates: db::model::SiloQuotasUpdate,
    ) -> UpdateResult<db::model::SiloQuotas> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore.silo_update_quota(opctx, &authz_silo, updates).await
    }

    /// Update a silo's quotas from a prior-version (lenient) request body,
    /// where omitted fields are left unchanged. Keeping the version-specific
    /// merge here lets db-model stay unaware of older wire versions.
    pub(crate) async fn silo_update_quota_v2025_11_20_00(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        params: v2025_11_20_00::silo::SiloQuotasUpdate,
    ) -> UpdateResult<db::model::SiloQuotas> {
        let updates = db::model::SiloQuotasUpdate {
            cpus: params.cpus,
            memory: params.memory.map(|f| f.into()),
            storage: params.storage.map(|f| f.into()),
            time_modified: chrono::Utc::now(),
        };
        self.silo_update_quota(opctx, silo_lookup, updates).await
    }
}
