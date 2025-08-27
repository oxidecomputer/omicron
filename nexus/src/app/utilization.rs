// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Insights into capacity and utilization

use nexus_db_lookup::lookup;
use nexus_db_model::IpPoolUtilization;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::http_pagination::PaginatedBy;

impl super::Nexus {
    pub async fn silo_utilization_view(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> Result<db::model::SiloUtilization, Error> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Read).await?;
        self.db_datastore.silo_utilization_view(opctx, &authz_silo).await
    }

    pub async fn silo_utilization_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::SiloUtilization> {
        self.db_datastore.silo_utilization_list(opctx, pagparams).await
    }

    pub async fn ip_pool_utilization_view(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
    ) -> Result<IpPoolUtilization, Error> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Read).await?;

        // TODO-correctness: We probably want to run these in the same
        // transaction, otherwise the two counts may change relative to one
        // another in between each.
        let allocated = self
            .db_datastore
            .ip_pool_allocated_count(opctx, &authz_pool)
            .await?;
        let capacity = self
            .db_datastore
            .ip_pool_total_capacity(opctx, &authz_pool)
            .await?;
        let capacity = capacity as f64;
        let remaining = capacity - allocated as f64;
        if remaining < 0.0 {
            return Err(Error::internal_error(
                format!(
                    "Computed an impossible negative count of remaining IP \
                addresses. Capacity = {capacity}, allocated = {allocated}"
                )
                .as_str(),
            ));
        }
        Ok(IpPoolUtilization { remaining, capacity })
    }
}
