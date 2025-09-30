// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Metrics

use crate::external_api::params::ResourceMetrics;
use dropshot::PaginationParams;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::{context::OpContext, db::fixed_data::FLEET_ID};
use nexus_external_api::TimeseriesSchemaPaginationParams;
use nexus_types::external_api::params::SystemMetricName;
use omicron_common::api::external::{Error, InternalContext};
use oximeter_db::{
    Measurement, OxqlResult, TimeseriesSchema, oxql::query::QueryAuthzScope,
};
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

    /// List available timeseries schema.
    pub(crate) async fn timeseries_schema_list(
        &self,
        opctx: &OpContext,
        pagination: &TimeseriesSchemaPaginationParams,
        limit: NonZeroU32,
    ) -> Result<dropshot::ResultsPage<TimeseriesSchema>, Error> {
        // Must be a fleet user to list timeseries schema.
        //
        // TODO-security: We need to figure out how to implement proper security
        // checks here, letting less-privileged users fetch data for the
        // resources they have access to.
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.timeseries_client
            .timeseries_schema_list(&pagination.page, limit)
            .await
            .map_err(|e| match e {
                oximeter_db::Error::DatabaseUnavailable(_)
                | oximeter_db::Error::Connection(_) => {
                    Error::ServiceUnavailable {
                        internal_message: e.to_string(),
                    }
                }
                _ => Error::InternalError { internal_message: e.to_string() },
            })
    }

    /// Run an OxQL query against the timeseries database.
    pub(crate) async fn timeseries_query(
        &self,
        opctx: &OpContext,
        query: impl AsRef<str>,
    ) -> Result<OxqlResult, Error> {
        // Must be a fleet user to list timeseries schema.
        //
        // TODO-security: We need to figure out how to implement proper security
        // checks here, letting less-privileged users fetch data for the
        // resources they have access to.
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.timeseries_client
            .oxql_query(query, QueryAuthzScope::Fleet)
            .await
            .map_err(map_timeseries_err)
    }

    /// Run an OxQL query against the timeseries database, scoped to a specific project.
    pub(crate) async fn timeseries_query_project(
        &self,
        _opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        query: impl AsRef<str>,
    ) -> Result<OxqlResult, Error> {
        // Ensure the user has read access to the project
        let (authz_silo, authz_project) =
            project_lookup.lookup_for(authz::Action::Read).await?;
        self.timeseries_client
            .oxql_query(
                query,
                QueryAuthzScope::Project {
                    silo_id: authz_silo.id(),
                    project_id: authz_project.id(),
                },
            )
            .await
            .map_err(map_timeseries_err)
    }
}

fn map_timeseries_err(e: oximeter_db::Error) -> Error {
    match e {
        oximeter_db::Error::DatabaseUnavailable(_)
        | oximeter_db::Error::Connection(_) => Error::unavail(&e.to_string()),
        oximeter_db::Error::Oxql(_)
        | oximeter_db::Error::TimeseriesNotFound(_) => {
            Error::invalid_request(e.to_string())
        }
        _ => Error::internal_error(&e.to_string()),
    }
}
