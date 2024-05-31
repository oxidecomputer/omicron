// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Resource limits and system quotas

use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use nexus_types::external_api::params;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::UpdateResult;
use std::sync::Arc;
use uuid::Uuid;

/// Application level operations on system quotas
pub struct Quota {
    datastore: Arc<db::DataStore>,
}

impl Quota {
    pub fn new(datastore: Arc<db::DataStore>) -> Quota {
        Quota { datastore }
    }

    pub async fn silo_quotas_view(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> Result<db::model::SiloQuotas, Error> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Read).await?;
        self.datastore.silo_quotas_view(opctx, &authz_silo).await
    }

    pub(crate) async fn fleet_list_quotas(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::SiloQuotas> {
        self.datastore.fleet_list_quotas(opctx, pagparams).await
    }

    pub(crate) async fn silo_update_quota(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        updates: &params::SiloQuotasUpdate,
    ) -> UpdateResult<db::model::SiloQuotas> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Modify).await?;
        self.datastore
            .silo_update_quota(opctx, &authz_silo, updates.clone().into())
            .await
    }
}
