// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::ServerContext;
use cockroach_admin_api::*;
use cockroach_admin_types::NodeDecommission;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::RequestContext;
use dropshot::TypedBody;
use slog::info;
use std::sync::Arc;

type CrdbApiDescription = dropshot::ApiDescription<Arc<ServerContext>>;

pub fn api() -> CrdbApiDescription {
    cockroach_admin_api_mod::api_description::<CockroachAdminImpl>()
        .expect("registered entrypoints")
}

enum CockroachAdminImpl {}

impl CockroachAdminApi for CockroachAdminImpl {
    type Context = Arc<ServerContext>;

    async fn cluster_init(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();
        let cli = ctx.cockroach_cli();
        let log = ctx.log();

        info!(log, "Initializing CRDB cluster");
        cli.cluster_init().await?;
        info!(log, "CRDB cluster initialized - initializing Omicron schema");
        cli.schema_init().await?;
        info!(log, "Omicron schema initialized");

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn node_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ClusterNodeStatus>, HttpError> {
        let ctx = rqctx.context();
        let all_nodes =
            ctx.cockroach_cli().node_status().await.map_err(HttpError::from)?;
        Ok(HttpResponseOk(ClusterNodeStatus { all_nodes }))
    }

    async fn local_node_id(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<LocalNodeId>, HttpError> {
        let ctx = rqctx.context();
        let node_id = ctx.node_id().await?.to_string();
        let zone_id = ctx.zone_id();
        Ok(HttpResponseOk(LocalNodeId { zone_id, node_id }))
    }

    async fn node_decommission(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<NodeId>,
    ) -> Result<HttpResponseOk<NodeDecommission>, HttpError> {
        let ctx = rqctx.context();
        let NodeId { node_id } = body.into_inner();
        let decommission_status =
            ctx.cockroach_cli().node_decommission(&node_id).await?;
        info!(
            ctx.log(), "successfully decommissioned node";
            "node_id" => node_id,
            "status" => ?decommission_status,
        );
        Ok(HttpResponseOk(decommission_status))
    }

    async fn status_vars(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<String>, HttpError> {
        let ctx = rqctx.context();
        let cockroach_http_address =
            ctx.cockroach_cli().cockroach_http_address();
        let url = format!("http://{}/_status/vars", cockroach_http_address);

        let client = reqwest::Client::new();
        let response = client.get(&url).send().await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "Failed to proxy to CockroachDB: {}",
                e
            ))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_else(|_| {
                "Failed to read error response".to_string()
            });
            let status_code = dropshot::ErrorStatusCode::from_status(status)
                .unwrap_or(dropshot::ErrorStatusCode::INTERNAL_SERVER_ERROR);
            return Err(HttpError {
                status_code,
                error_code: None,
                external_message: body.clone(),
                internal_message: body,
                headers: None,
            });
        }

        let body = response.text().await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "Failed to read response body: {}",
                e
            ))
        })?;

        Ok(HttpResponseOk(body))
    }

    async fn status_nodes(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<String>, HttpError> {
        let ctx = rqctx.context();
        let cockroach_http_address =
            ctx.cockroach_cli().cockroach_http_address();
        let url = format!("http://{}/_status/nodes", cockroach_http_address);

        let client = reqwest::Client::new();
        let response = client.get(&url).send().await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "Failed to proxy to CockroachDB: {}",
                e
            ))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_else(|_| {
                "Failed to read error response".to_string()
            });
            let status_code = dropshot::ErrorStatusCode::from_status(status)
                .unwrap_or(dropshot::ErrorStatusCode::INTERNAL_SERVER_ERROR);
            return Err(HttpError {
                status_code,
                error_code: None,
                external_message: body.clone(),
                internal_message: body,
                headers: None,
            });
        }

        let body = response.text().await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "Failed to read response body: {}",
                e
            ))
        })?;

        Ok(HttpResponseOk(body))
    }
}
