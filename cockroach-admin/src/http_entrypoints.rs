// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::ServerContext;
use cockroach_admin_api::*;
use cockroach_admin_types::NodeDecommission;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::RequestContext;
use dropshot::TypedBody;
use std::sync::Arc;

type CrdbApiDescription = dropshot::ApiDescription<Arc<ServerContext>>;

pub fn api() -> CrdbApiDescription {
    cockroach_admin_api_mod::api_description::<CockroachAdminImpl>()
        .expect("registered entrypoints")
}

enum CockroachAdminImpl {}

impl CockroachAdminApi for CockroachAdminImpl {
    type Context = Arc<ServerContext>;

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
        Ok(HttpResponseOk(decommission_status))
    }
}
