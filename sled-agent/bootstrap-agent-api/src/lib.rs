// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The bootstrap agent's API.
//!
//! Note that the bootstrap agent also communicates over Sprockets,
//! and has a separate interface for establishing the trust quorum.

use dropshot::{
    HttpError, HttpResponseOk, HttpResponseUpdatedNoContent, RequestContext,
    TypedBody,
};
use dropshot_api_manager_types::api_versions;
use omicron_uuid_kinds::{RackInitUuid, RackResetUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::{
    rack_init::RackInitializeRequest, rack_ops::RackOperationStatus,
};
use sled_hardware_types::Baseboard;
use tufaceous_artifact::ArtifactVersion;

api_versions!([
    // Do not create new versions of this client-side versioned API.
    // https://github.com/oxidecomputer/omicron/issues/9290
    (1, INITIAL),
]);

#[dropshot::api_description]
pub trait BootstrapAgentApi {
    type Context;

    /// Return the baseboard identity of this sled.
    #[endpoint {
        method = GET,
        path = "/baseboard",
    }]
    async fn baseboard_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Baseboard>, HttpError>;

    /// Provide a list of components known to the bootstrap agent.
    ///
    /// This API is intended to allow early boot services (such as Wicket)
    /// to query the underlying component versions installed on a sled.
    #[endpoint {
        method = GET,
        path = "/components",
    }]
    async fn components_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<Component>>, HttpError>;

    /// Get the current status of rack initialization or reset.
    #[endpoint {
        method = GET,
        path = "/rack-initialize",
    }]
    async fn rack_initialization_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackOperationStatus>, HttpError>;

    /// Initialize the rack with the provided configuration.
    #[endpoint {
        method = POST,
        path = "/rack-initialize",
    }]
    async fn rack_initialize(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<RackInitializeRequest>,
    ) -> Result<HttpResponseOk<RackInitUuid>, HttpError>;

    /// Reset the rack to an unconfigured state.
    #[endpoint {
        method = DELETE,
        path = "/rack-initialize",
    }]
    async fn rack_reset(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackResetUuid>, HttpError>;

    /// Reset this particular sled to an unconfigured state.
    #[endpoint {
        method = DELETE,
        path = "/sled-initialize",
    }]
    async fn sled_reset(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Component {
    pub name: String,
    pub version: ArtifactVersion,
}
