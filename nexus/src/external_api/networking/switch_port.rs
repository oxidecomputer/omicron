// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use dropshot::{
    endpoint, HttpError, HttpResponseCreated, HttpResponseDeleted,
    HttpResponseOk, HttpResponseUpdatedNoContent, Path, Query, RequestContext,
    ResultsPage, TypedBody,
};
use nexus_types::external_api::{
    networking::{SwitchPort, SwitchPortSettings, SwitchPortSettingsView},
    params::{self, SwitchPortSettingsInfoSelector},
};
use omicron_common::api::external::http_pagination::{
    data_page_params_for, marker_for_name_or_id, name_or_id_pagination,
    PaginatedById, PaginatedByNameOrId, ScanById, ScanByNameOrId, ScanParams,
};
use std::sync::Arc;

use crate::context::ServerContext;

/// List switch port settings
#[endpoint {
    method = GET,
    path = "/v1/system/networking/switch-port-settings",
    tags = ["system/networking"],
}]
pub async fn networking_switch_port_settings_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<
        PaginatedByNameOrId<params::SwitchPortSettingsSelector>,
    >,
) -> Result<HttpResponseOk<ResultsPage<SwitchPortSettings>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let settings = nexus
            .switch_port_settings_list(&opctx, &paginated_by)
            .await?
            .into_iter()
            .map(|p| p.into())
            .collect();

        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            settings,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Get information about a switch port
#[endpoint {
    method = GET,
    path = "/v1/system/networking/switch-port-settings/{port}",
    tags = ["system/networking"],
}]
pub async fn networking_switch_port_settings_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<SwitchPortSettingsInfoSelector>,
) -> Result<HttpResponseOk<SwitchPortSettingsView>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = path_params.into_inner().port;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let settings = nexus.switch_port_settings_get(&opctx, &query).await?;
        Ok(HttpResponseOk(settings.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create switch port settings
#[endpoint {
    method = POST,
    path = "/v1/system/networking/switch-port-settings",
    tags = ["system/networking"],
}]
pub async fn networking_switch_port_settings_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    new_settings: TypedBody<params::SwitchPortSettingsCreate>,
) -> Result<HttpResponseCreated<SwitchPortSettingsView>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let params = new_settings.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let result = nexus.switch_port_settings_create(&opctx, params).await?;

        let settings: SwitchPortSettingsView = result.into();
        Ok(HttpResponseCreated(settings))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete switch port settings
#[endpoint {
    method = DELETE,
    path = "/v1/system/networking/switch-port-settings",
    tags = ["system/networking"],
}]
pub async fn networking_switch_port_settings_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::SwitchPortSettingsSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let selector = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        nexus.switch_port_settings_delete(&opctx, &selector).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List switch ports
#[endpoint {
    method = GET,
    path = "/v1/system/hardware/switch-port",
    tags = ["system/hardware"],
}]
pub async fn networking_switch_port_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById<params::SwitchPortPageSelector>>,
) -> Result<HttpResponseOk<ResultsPage<SwitchPort>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pagparams = data_page_params_for(&rqctx, &query)?;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let addrs = nexus
            .switch_port_list(&opctx, &pagparams)
            .await?
            .into_iter()
            .map(|p| p.into())
            .collect();

        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            addrs,
            &|_, x: &SwitchPort| x.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Apply switch port settings
#[endpoint {
    method = POST,
    path = "/v1/system/hardware/switch-port/{port}/settings",
    tags = ["system/hardware"],
}]
pub async fn networking_switch_port_apply_settings(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SwitchPortPathSelector>,
    query_params: Query<params::SwitchPortSelector>,
    settings_body: TypedBody<params::SwitchPortApplySettings>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let port = path_params.into_inner().port;
        let query = query_params.into_inner();
        let settings = settings_body.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        nexus
            .switch_port_apply_settings(&opctx, &port, &query, &settings)
            .await?;
        Ok(HttpResponseUpdatedNoContent {})
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Clear switch port settings
#[endpoint {
    method = DELETE,
    path = "/v1/system/hardware/switch-port/{port}/settings",
    tags = ["system/hardware"],
}]
pub async fn networking_switch_port_clear_settings(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SwitchPortPathSelector>,
    query_params: Query<params::SwitchPortSelector>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let port = path_params.into_inner().port;
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        nexus.switch_port_clear_settings(&opctx, &port, &query).await?;
        Ok(HttpResponseUpdatedNoContent {})
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}
