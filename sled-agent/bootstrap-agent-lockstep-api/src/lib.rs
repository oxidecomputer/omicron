// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Lockstep API for bootstrap agent rack initialization.
//!
//! This API handles rack initialization and reset operations. It is a lockstep
//! API as we do not expect rack initialization functions to be called during
//! and upgrade. Furthermore when rack initialization functions are called
//! it's expected that software components are on the same version.

use bootstrap_agent_lockstep_types::RackInitializeRequest;
use bootstrap_agent_lockstep_types::RackOperationStatus;
use bootstrap_agent_lockstep_types::ReplicatedNetworkConfig;
use dropshot::{HttpError, HttpResponseOk, RequestContext, TypedBody};
use omicron_uuid_kinds::RackInitUuid;
use omicron_uuid_kinds::RackResetUuid;

#[dropshot::api_description]
pub trait BootstrapAgentLockstepApi {
    type Context;

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

    /// Get the current contents of the network config kept in the replicated
    /// bootstore.
    ///
    /// This should ONLY be used for debugging (e.g., via `omdb`). Nexus, RSS,
    /// and sled-agent should never access this endpoint in production - the
    /// bootstore contents should be treated as "write only" from their point of
    /// view.
    #[endpoint {
        method = GET,
        path = "/debug/network-config-contents",
    }]
    async fn network_config_contents_for_debug(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ReplicatedNetworkConfig>, HttpError>;
}
