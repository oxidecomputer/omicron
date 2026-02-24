// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the bootstrap agent's lockstep API.
//!
//! This API handles rack initialization and reset operations. It is a lockstep
//! API, meaning the client and server are always deployed together and only
//! need to support a single version.

use super::http_entrypoints::BootstrapServerContext;
use bootstrap_agent_lockstep_api::BootstrapAgentLockstepApi;
use bootstrap_agent_lockstep_api::bootstrap_agent_lockstep_api_mod;
use dropshot::{
    ApiDescription, HttpError, HttpResponseOk, RequestContext, TypedBody,
};
use omicron_uuid_kinds::RackInitUuid;
use omicron_uuid_kinds::RackResetUuid;
use sled_agent_types::rack_init::{
    RackInitializeRequest, RackInitializeRequestParams,
};
use sled_agent_types::rack_ops::RackOperationStatus;

/// Returns a description of the bootstrap agent lockstep API
pub(crate) fn api() -> ApiDescription<BootstrapServerContext> {
    bootstrap_agent_lockstep_api_mod::api_description::<
        BootstrapAgentLockstepImpl,
    >()
    .expect("registered entrypoints successfully")
}

enum BootstrapAgentLockstepImpl {}

impl BootstrapAgentLockstepApi for BootstrapAgentLockstepImpl {
    type Context = BootstrapServerContext;

    async fn rack_initialization_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackOperationStatus>, HttpError> {
        let ctx = rqctx.context();
        let status = ctx.rss_access.operation_status();
        Ok(HttpResponseOk(status))
    }

    async fn rack_initialize(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<RackInitializeRequest>,
    ) -> Result<HttpResponseOk<RackInitUuid>, HttpError> {
        // Note that if we are performing rack initialization in
        // response to an external request, we assume we are not
        // skipping timesync.
        const SKIP_TIMESYNC: bool = false;
        let ctx = rqctx.context();
        let request = body.into_inner();
        let params = RackInitializeRequestParams::new(request, SKIP_TIMESYNC);
        let id = ctx
            .start_rack_initialize(params)
            .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
        Ok(HttpResponseOk(id))
    }

    async fn rack_reset(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackResetUuid>, HttpError> {
        let ctx = rqctx.context();
        let id = ctx
            .rss_access
            .start_reset(
                &ctx.base_log,
                ctx.sprockets.clone(),
                ctx.global_zone_bootstrap_ip,
                ctx.measurements.clone(),
            )
            .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
        Ok(HttpResponseOk(id))
    }
}
