// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the bootstrap agent's API.
//!
//! Note that the bootstrap agent also communicates over Sprockets,
//! and has a separate interface for establishing the trust quorum.

use super::rack_ops::RssAccess;
use super::BootstrapError;
use super::RssAccessError;
use crate::updates::ConfigUpdates;
use crate::updates::{Component, UpdateManager};
use bootstore::schemes::v0 as bootstore;
use dropshot::ApiDescriptionRegisterError;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseOk,
    HttpResponseUpdatedNoContent, RequestContext, TypedBody,
};
use http::StatusCode;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::RackInitUuid;
use omicron_uuid_kinds::RackResetUuid;
use sled_agent_types::rack_init::RackInitializeRequest;
use sled_agent_types::rack_ops::RackOperationStatus;
use sled_hardware_types::Baseboard;
use sled_storage::manager::StorageHandle;
use slog::Logger;
use std::net::Ipv6Addr;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

pub(crate) struct BootstrapServerContext {
    pub(crate) base_log: Logger,
    pub(crate) global_zone_bootstrap_ip: Ipv6Addr,
    pub(crate) storage_manager: StorageHandle,
    pub(crate) bootstore_node_handle: bootstore::NodeHandle,
    pub(crate) baseboard: Baseboard,
    pub(crate) rss_access: RssAccess,
    pub(crate) updates: ConfigUpdates,
    pub(crate) sled_reset_tx:
        mpsc::Sender<oneshot::Sender<Result<(), BootstrapError>>>,
}

impl BootstrapServerContext {
    pub(super) fn start_rack_initialize(
        &self,
        request: RackInitializeRequest,
    ) -> Result<RackInitUuid, RssAccessError> {
        self.rss_access.start_initializing(
            &self.base_log,
            self.global_zone_bootstrap_ip,
            &self.storage_manager,
            &self.bootstore_node_handle,
            request,
        )
    }
}

type BootstrapApiDescription = ApiDescription<BootstrapServerContext>;

/// Returns a description of the bootstrap agent API
pub(crate) fn api() -> BootstrapApiDescription {
    fn register_endpoints(
        api: &mut BootstrapApiDescription,
    ) -> Result<(), ApiDescriptionRegisterError> {
        api.register(baseboard_get)?;
        api.register(components_get)?;
        api.register(rack_initialization_status)?;
        api.register(rack_initialize)?;
        api.register(rack_reset)?;
        api.register(sled_reset)?;
        Ok(())
    }

    let mut api = BootstrapApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

/// Return the baseboard identity of this sled.
#[endpoint {
    method = GET,
    path = "/baseboard",
}]
async fn baseboard_get(
    rqctx: RequestContext<BootstrapServerContext>,
) -> Result<HttpResponseOk<Baseboard>, HttpError> {
    let ctx = rqctx.context();
    Ok(HttpResponseOk(ctx.baseboard.clone()))
}

/// Provides a list of components known to the bootstrap agent.
///
/// This API is intended to allow early boot services (such as Wicket)
/// to query the underlying component versions installed on a sled.
#[endpoint {
    method = GET,
    path = "/components",
}]
async fn components_get(
    rqctx: RequestContext<BootstrapServerContext>,
) -> Result<HttpResponseOk<Vec<Component>>, HttpError> {
    let ctx = rqctx.context();
    let updates = UpdateManager::new(ctx.updates.clone());
    let components = updates
        .components_get()
        .await
        .map_err(|err| HttpError::for_internal_error(err.to_string()))?;
    Ok(HttpResponseOk(components))
}

/// Get the current status of rack initialization or reset.
#[endpoint {
    method = GET,
    path = "/rack-initialize",
}]
async fn rack_initialization_status(
    rqctx: RequestContext<BootstrapServerContext>,
) -> Result<HttpResponseOk<RackOperationStatus>, HttpError> {
    let ctx = rqctx.context();
    let status = ctx.rss_access.operation_status();
    Ok(HttpResponseOk(status))
}

/// Initializes the rack with the provided configuration.
#[endpoint {
    method = POST,
    path = "/rack-initialize",
}]
async fn rack_initialize(
    rqctx: RequestContext<BootstrapServerContext>,
    body: TypedBody<RackInitializeRequest>,
) -> Result<HttpResponseOk<RackInitUuid>, HttpError> {
    let ctx = rqctx.context();
    let request = body.into_inner();
    let id = ctx
        .start_rack_initialize(request)
        .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
    Ok(HttpResponseOk(id))
}

/// Resets the rack to an unconfigured state.
#[endpoint {
    method = DELETE,
    path = "/rack-initialize",
}]
async fn rack_reset(
    rqctx: RequestContext<BootstrapServerContext>,
) -> Result<HttpResponseOk<RackResetUuid>, HttpError> {
    let ctx = rqctx.context();
    let id = ctx
        .rss_access
        .start_reset(&ctx.base_log, ctx.global_zone_bootstrap_ip)
        .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
    Ok(HttpResponseOk(id))
}

/// Resets this particular sled to an unconfigured state.
#[endpoint {
    method = DELETE,
    path = "/sled-initialize",
}]
async fn sled_reset(
    rqctx: RequestContext<BootstrapServerContext>,
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
            return Err(HttpError::for_status(
                Some("ResetPending".to_string()),
                StatusCode::TOO_MANY_REQUESTS,
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
