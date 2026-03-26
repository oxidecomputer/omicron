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

        let (allocated, capacity) =
            self.db_datastore.ip_pool_utilization(opctx, &authz_pool).await?;

        // Compute the remaining count in full 128-bit arithmetic, checking for
        // negative values, and convert to f64s at the end.
        let Ok(allocated) = u128::try_from(allocated) else {
            return Err(Error::internal_error(
                "Impossible negative number of allocated IP addresses",
            ));
        };
        let Some(remaining) = capacity.checked_sub(allocated) else {
            return Err(Error::internal_error(
                format!(
                    "Computed an impossible negative count of remaining IP \
                addresses. Capacity = {capacity}, allocated = {allocated}"
                )
                .as_str(),
            ));
        };
        let remaining = remaining as f64;
        let capacity = capacity as f64;
        Ok(IpPoolUtilization { remaining, capacity })
    }
}
