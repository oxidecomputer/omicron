// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Metrics

use crate::external_api::http_entrypoints::SystemMetricName;
use crate::external_api::params::ResourceMetrics;
use dropshot::PaginationParams;
use nexus_db_queries::authz;
use nexus_db_queries::{
    context::OpContext,
    db::{fixed_data::FLEET_ID, lookup},
};
use omicron_common::api::external::{Error, InternalContext};
use oximeter_db::Measurement;
use std::num::NonZeroU32;

impl super::Nexus {
    pub(crate) async fn system_metric_list(
        &self,
        opctx: &OpContext,
        metric_name: SystemMetricName,
        silo_lookup: Option<lookup::Silo<'_>>,
        pagination: PaginationParams<ResourceMetrics, ResourceMetrics>,
        limit: NonZeroU32,
    ) -> Result<dropshot::ResultsPage<Measurement>, Error> {
        // must be a fleet reader to use this path at all
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        let resource_id = match silo_lookup {
            Some(silo_lookup) => {
                // Make sure silo exists and current user can read it
                let (.., authz_silo) =
                    silo_lookup.lookup_for(authz::Action::Read).await?;
                authz_silo.id()
            }
            // if no project specified, scope to entire fleet, i.e., all silos
            _ => *FLEET_ID,
        };

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
            &[&format!("id=={}", resource_id)],
            pagination,
            limit,
        )
        .await
    }

    pub(crate) async fn silo_metric_list(
        &self,
        opctx: &OpContext,
        metric_name: SystemMetricName,
        project_lookup: Option<lookup::Project<'_>>,
        pagination: PaginationParams<ResourceMetrics, ResourceMetrics>,
        limit: NonZeroU32,
    ) -> Result<dropshot::ResultsPage<Measurement>, Error> {
        let authz_silo =
            opctx.authn.silo_required().internal_context("listing metrics")?;
        // must be a silo reader to use this path at all
        opctx.authorize(authz::Action::Read, &authz_silo).await?;

        let resource_id = match project_lookup {
            Some(project_lookup) => {
                // make sure project exists and current user can read it
                let (.., authz_project) =
                    project_lookup.lookup_for(authz::Action::Read).await?;
                authz_project.id()
            }
            // if no project specified, scope to current silo
            _ => authz_silo.id(),
        };

        let timeseries = match metric_name {
            SystemMetricName::VirtualDiskSpaceProvisioned
            | SystemMetricName::CpusProvisioned
            | SystemMetricName::RamProvisioned => {
                opctx.authorize(authz::Action::Read, &authz_silo).await?;
                format!("collection_target:{metric_name}")
            }
        };
        self.select_timeseries(
            &timeseries,
            &[&format!("id=={}", resource_id)],
            pagination,
            limit,
        )
        .await
    }
}
