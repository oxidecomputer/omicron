// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Insights into capacity and utilization

use nexus_db_model::IpPoolUtilization;
use nexus_db_model::Ipv4Utilization;
use nexus_db_model::Ipv6Utilization;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use std::sync::Arc;

/// Application level operations regarding capacity and utilization
#[derive(Clone)]
pub struct Utilization {
    datastore: Arc<db::DataStore>,
}

impl Utilization {
    pub fn new(datastore: Arc<db::DataStore>) -> Utilization {
        Utilization { datastore }
    }

    pub async fn silo_utilization_view(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
    ) -> Result<db::model::SiloUtilization, Error> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Read).await?;
        self.datastore.silo_utilization_view(opctx, &authz_silo).await
    }

    pub async fn silo_utilization_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::SiloUtilization> {
        self.datastore.silo_utilization_list(opctx, pagparams).await
    }

    pub async fn ip_pool_utilization_view(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
    ) -> Result<IpPoolUtilization, Error> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Read).await?;

        let allocated =
            self.datastore.ip_pool_allocated_count(opctx, &authz_pool).await?;
        let capacity =
            self.datastore.ip_pool_total_capacity(opctx, &authz_pool).await?;

        // we have one query for v4 and v6 allocated and one for both v4 and
        // v6 capacity so we can do two queries instead 4, but in the response
        // we want them paired by IP version
        Ok(IpPoolUtilization {
            ipv4: Ipv4Utilization {
                // This one less trivial to justify than the u128 conversion
                // below because an i64 could obviously be too big for u32.
                // In addition to the fact that it is unlikely for anyone to
                // allocate 4 billion IPs, we rely on the fact that there can
                // only be 2^32 IPv4 addresses, period.
                allocated: u32::try_from(allocated.ipv4).map_err(|_e| {
                    Error::internal_error(&format!(
                        "Failed to convert i64 {} IPv4 address count to u32",
                        allocated.ipv4
                    ))
                })?,
                capacity: capacity.ipv4,
            },
            ipv6: Ipv6Utilization {
                // SQL represents counts as signed integers for historical
                // or compatibility reasons even though they can't really be
                // negative, and Diesel follows that. We assume here that it
                // will be a positive i64.
                allocated: u128::try_from(allocated.ipv6).map_err(|_e| {
                    Error::internal_error(&format!(
                        "Failed to convert i64 {} IPv6 address count to u128",
                        allocated.ipv6
                    ))
                })?,
                capacity: capacity.ipv6,
            },
        })
    }
}
