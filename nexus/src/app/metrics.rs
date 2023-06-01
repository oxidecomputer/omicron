// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Metrics

use crate::authz;
use crate::external_api::http_entrypoints::SystemMetricName;
use crate::external_api::http_entrypoints::SystemMetricParams;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::Error;
use oximeter_db::Measurement;
use std::num::NonZeroU32;

impl super::Nexus {
    pub async fn system_metric_lookup(
        &self,
        opctx: &OpContext,
        metric_name: SystemMetricName,
        query: SystemMetricParams,
        limit: NonZeroU32,
    ) -> Result<dropshot::ResultsPage<Measurement>, Error> {
        let timeseries = match metric_name {
            SystemMetricName::VirtualDiskSpaceProvisioned
            | SystemMetricName::CpusProvisioned
            | SystemMetricName::RamProvisioned => {
                opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
                format!("collection_target:{metric_name}")
            }
        };
        self.select_timeseries(
            &timeseries,
            &[&format!("id=={}", query.id)],
            query.pagination,
            limit,
        )
        .await
    }
}
