// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the bootstrap agent's API.
//!
//! Note that the bootstrap agent also communicates over Sprockets,
//! and has a separate interface for establishing the trust quorum.

use crate::bootstrap::agent::Agent;
use crate::bootstrap::params::RackInitializeRequest;
use crate::updates::Component;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseOk,
    HttpResponseUpdatedNoContent, RequestContext, TypedBody,
};
use omicron_common::api::external::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use std::sync::Arc;
use uuid::Uuid;

type BootstrapApiDescription = ApiDescription<Arc<Agent>>;

/// Returns a description of the bootstrap agent API
pub(crate) fn api() -> BootstrapApiDescription {
    fn register_endpoints(
        api: &mut BootstrapApiDescription,
    ) -> Result<(), String> {
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

/// Current status of any rack-level operation being performed by this bootstrap
/// agent.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RackOperationStatus {
    None,
    Initializing {
        id: Uuid,
    },
    /// `id` will be none if the rack was already initialized on startup.
    Initialized {
        id: Option<Uuid>,
    },
    InitializationFailed {
        id: Uuid,
        message: String,
    },
    InitializationPanicked {
        id: Uuid,
    },
    ResetOnStartup,
    Resetting {
        id: Uuid,
    },
    /// `id` will be none if the rack was already reset on startup.
    Reset {
        id: Option<Uuid>,
    },
    ResetFailed {
        id: Uuid,
        message: String,
    },
    ResetPanicked {
        id: Uuid,
    },
}

#[derive(
    Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct OperationId {
    pub id: Uuid,
}

/// Return the baseboard identity of this sled.
#[endpoint {
    method = GET,
    path = "/baseboard",
}]
async fn baseboard_get(
    rqctx: RequestContext<Arc<Agent>>,
) -> Result<HttpResponseOk<Baseboard>, HttpError> {
    let ba = rqctx.context();
    Ok(HttpResponseOk(ba.baseboard().clone()))
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
    rqctx: RequestContext<Arc<Agent>>,
) -> Result<HttpResponseOk<Vec<Component>>, HttpError> {
    let ba = rqctx.context();
    let components = ba.components_get().await.map_err(|e| Error::from(e))?;
    Ok(HttpResponseOk(components))
}

/// Get the current status of rack initialization or reset.
#[endpoint {
    method = GET,
    path = "/rack-initialize",
}]
async fn rack_initialization_status(
    rqctx: RequestContext<Arc<Agent>>,
) -> Result<HttpResponseOk<RackOperationStatus>, HttpError> {
    let ba = rqctx.context();
    let status = ba.initialization_reset_op_status();
    Ok(HttpResponseOk(status))
}

/// Initializes the rack with the provided configuration.
#[endpoint {
    method = POST,
    path = "/rack-initialize",
}]
async fn rack_initialize(
    rqctx: RequestContext<Arc<Agent>>,
    body: TypedBody<RackInitializeRequest>,
) -> Result<HttpResponseOk<OperationId>, HttpError> {
    let ba = rqctx.context();
    let request = body.into_inner();
    let id = ba
        .start_rack_initialize(request)
        .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
    Ok(HttpResponseOk(OperationId { id: id.0 }))
}

/// Resets the rack to an unconfigured state.
#[endpoint {
    method = DELETE,
    path = "/rack-initialize",
}]
async fn rack_reset(
    rqctx: RequestContext<Arc<Agent>>,
) -> Result<HttpResponseOk<OperationId>, HttpError> {
    let ba = rqctx.context();
    let id = ba
        .start_rack_reset()
        .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
    Ok(HttpResponseOk(OperationId { id: id.0 }))
}

/// Resets this particular sled to an unconfigured state.
#[endpoint {
    method = DELETE,
    path = "/sled-initialize",
}]
async fn sled_reset(
    rqctx: RequestContext<Arc<Agent>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let ba = rqctx.context();
    ba.sled_reset().await.map_err(|e| Error::from(e))?;
    Ok(HttpResponseUpdatedNoContent())
}
