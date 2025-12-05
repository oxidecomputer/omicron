// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the bootstrap agent's API.
//!
//! Note that the bootstrap agent also communicates over Sprockets,
//! and has a separate interface for establishing the trust quorum.

use super::BootstrapError;
use super::RssAccessError;
use super::rack_ops::RssAccess;
use crate::updates::ConfigUpdates;
use crate::updates::UpdateManager;
use bootstore::schemes::v0 as bootstore;
use bootstrap_agent_api::BootstrapAgentApi;
use bootstrap_agent_api::Component;
use bootstrap_agent_api::bootstrap_agent_api_mod;
use dropshot::{
    ApiDescription, HttpError, HttpResponseOk, HttpResponseUpdatedNoContent,
    RequestContext, TypedBody,
};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::RackInitUuid;
use omicron_uuid_kinds::RackResetUuid;
use sled_agent_config_reconciler::InternalDisksReceiver;
use sled_agent_types::rack_init::{
    RackInitializeRequest, RackInitializeRequestParams,
};
use sled_agent_types::rack_ops::RackOperationStatus;
use sled_hardware_types::Baseboard;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use sprockets_tls::keys::SprocketsConfig;
use std::net::Ipv6Addr;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

pub(crate) struct BootstrapServerContext {
    pub(crate) base_log: Logger,
    pub(crate) global_zone_bootstrap_ip: Ipv6Addr,
    pub(crate) internal_disks_rx: InternalDisksReceiver,
    pub(crate) bootstore_node_handle: bootstore::NodeHandle,
    pub(crate) baseboard: Baseboard,
    pub(crate) rss_access: RssAccess,
    pub(crate) updates: ConfigUpdates,
    pub(crate) sled_reset_tx:
        mpsc::Sender<oneshot::Sender<Result<(), BootstrapError>>>,
    pub(crate) sprockets: SprocketsConfig,
    pub(crate) trust_quorum_handle: trust_quorum::NodeTaskHandle,
}

impl BootstrapServerContext {
    pub(super) fn start_rack_initialize(
        &self,
        request: RackInitializeRequestParams,
    ) -> Result<RackInitUuid, RssAccessError> {
        self.rss_access.start_initializing(
            &self.base_log,
            self.sprockets.clone(),
            self.global_zone_bootstrap_ip,
            &self.internal_disks_rx,
            &self.bootstore_node_handle,
            &self.trust_quorum_handle,
            request,
        )
    }
}

/// Returns a description of the bootstrap agent API
pub(crate) fn api() -> ApiDescription<BootstrapServerContext> {
    bootstrap_agent_api_mod::api_description::<BootstrapAgentImpl>()
        .expect("registered entrypoints successfully")
}

enum BootstrapAgentImpl {}

impl BootstrapAgentApi for BootstrapAgentImpl {
    type Context = BootstrapServerContext;

    async fn baseboard_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Baseboard>, HttpError> {
        let ctx = rqctx.context();
        Ok(HttpResponseOk(ctx.baseboard.clone()))
    }

    async fn components_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<Component>>, HttpError> {
        let ctx = rqctx.context();
        let updates = UpdateManager::new(ctx.updates.clone());
        let components = updates.components_get().await.map_err(|err| {
            HttpError::for_internal_error(
                InlineErrorChain::new(&err).to_string(),
            )
        })?;
        Ok(HttpResponseOk(components))
    }

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
        let request =
            RackInitializeRequestParams::new(body.into_inner(), SKIP_TIMESYNC);
        let id = ctx
            .start_rack_initialize(request)
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
            )
            .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
        Ok(HttpResponseOk(id))
    }

    async fn sled_reset(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();
        let (response_tx, response_rx) = oneshot::channel();

        let make_channel_closed_err = || {
            Err(HttpError::for_internal_error(
                "sled_reset channel closed: task panic?".to_string(),
            ))
        };

        match ctx.sled_reset_tx.try_send(response_tx) {
            Ok(()) => (),
            Err(TrySendError::Full(_)) => {
                return Err(HttpError::for_client_error_with_status(
                    Some("ResetPending".to_string()),
                    dropshot::ClientErrorStatusCode::TOO_MANY_REQUESTS,
                ));
            }
            Err(TrySendError::Closed(_)) => {
                return make_channel_closed_err();
            }
        }

        match response_rx.await {
            Ok(result) => {
                () = result.map_err(Error::from)?;
                Ok(HttpResponseUpdatedNoContent())
            }
            Err(_) => make_channel_closed_err(),
        }
    }
}
