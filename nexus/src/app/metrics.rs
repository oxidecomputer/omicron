// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Metrics

use crate::authz;
use crate::external_api::http_entrypoints::SystemMetricName;
use crate::external_api::params::ResourceMetrics;
use dropshot::PaginationParams;
use nexus_db_queries::{
    context::OpContext,
    db::{fixed_data::FLEET_ID, lookup::LookupPath},
};
use omicron_common::api::external::{Error, InternalContext, NameOrId};
use oximeter_db::Measurement;
use std::num::NonZeroU32;
use uuid::Uuid;

impl super::Nexus {
    pub async fn system_metric_lookup(
        &self,
        opctx: &OpContext,
        metric_name: SystemMetricName,
        silo_id: Option<Uuid>,
        pagination: PaginationParams<ResourceMetrics, ResourceMetrics>,
        limit: NonZeroU32,
    ) -> Result<dropshot::ResultsPage<Measurement>, Error> {
        let resource_id = match silo_id {
            // If silo is specified, make sure it exists, primarily to make sure
            // it is a silo. There are other resources in this table and this is
            // the only check to make sure the provided ID is that of a silo.
            Some(silo_id) => {
                self.silo_lookup(opctx, NameOrId::Id(silo_id))?.fetch().await?;
                silo_id
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

    pub async fn silo_metric_lookup(
        &self,
        opctx: &OpContext,
        metric_name: SystemMetricName,
        project_id: Option<Uuid>,
        pagination: PaginationParams<ResourceMetrics, ResourceMetrics>,
        limit: NonZeroU32,
    ) -> Result<dropshot::ResultsPage<Measurement>, Error> {
        let current_silo =
            opctx.authn.silo_required().internal_context("listing metrics")?;

        let resource_id = match project_id {
            // If project is specified, make sure it exists (and, especially, is
            // a project) and the current user can read it
            Some(project_id) => {
                LookupPath::new(opctx, &self.db_datastore)
                    .project_id(project_id)
                    .lookup_for(authz::Action::Read)
                    .await?;
                project_id
            }
            // if no project specified, scope to current silo
            // TODO: check a permission here, like silo read
            _ => current_silo.id(),
        };

        let timeseries = match metric_name {
            SystemMetricName::VirtualDiskSpaceProvisioned
            | SystemMetricName::CpusProvisioned
            | SystemMetricName::RamProvisioned => {
                opctx.authorize(authz::Action::Read, &current_silo).await?;
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
