// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cockroach_cli::NodeStatus;
use crate::context::ServerContext;
use dropshot::endpoint;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::RequestContext;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

type CrdbApiDescription = dropshot::ApiDescription<Arc<ServerContext>>;

pub fn api() -> CrdbApiDescription {
    fn register_endpoints(api: &mut CrdbApiDescription) -> Result<(), String> {
        api.register(node_status)?;
        Ok(())
    }

    let mut api = CrdbApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ClusterNodeStatus {
    pub all_nodes: Vec<NodeStatus>,
}

/// Get the status of all nodes in the CRDB cluster
#[endpoint {
    method = GET,
    path = "/node/status",
}]
async fn node_status(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<ClusterNodeStatus>, HttpError> {
    let ctx = rqctx.context();
    let all_nodes =
        ctx.cockroach_cli.node_status().await.map_err(HttpError::from)?;
    Ok(HttpResponseOk(ClusterNodeStatus { all_nodes }))
}
