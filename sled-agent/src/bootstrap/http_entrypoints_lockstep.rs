// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the bootstrap agent's lockstep API.
//!
//! This API handles rack initialization and reset operations. It is a lockstep
//! API, meaning the client and server are always deployed together and only
//! need to support a single version.

use std::net::Ipv6Addr;

use super::RssAccessError;
use base64::Engine;
use bootstore::NetworkConfig;
use bootstore::schemes::v0 as bootstore;
use bootstrap_agent_lockstep_api::BootstrapAgentLockstepApi;
use bootstrap_agent_lockstep_api::bootstrap_agent_lockstep_api_mod;
use bootstrap_agent_lockstep_types::BaseboardIds;
use bootstrap_agent_lockstep_types::BootstrapIpOfBaseboardId;
use bootstrap_agent_lockstep_types::MultirackJoinRequest;
use bootstrap_agent_lockstep_types::RackInitializeRequest;
use bootstrap_agent_lockstep_types::RackOperationStatus;
use bootstrap_agent_lockstep_types::ReplicatedNetworkConfig;
use bootstrap_agent_lockstep_types::ReplicatedNetworkConfigContents;
use dropshot::ClientErrorStatusCode;
use dropshot::{
    ApiDescription, HttpError, HttpResponseOk, RequestContext, TypedBody,
};
use omicron_uuid_kinds::MultirackJoinUuid;
use omicron_uuid_kinds::RackInitUuid;
use sled_agent_bootstrap_common::RssContext;
use sled_agent_config_reconciler::InternalDisksReceiver;
use sled_agent_measurements::MeasurementsHandle;
use sled_agent_multirack_join::MultirackJoinServiceState;
use sled_agent_rack_setup::RackInitializeRequestParams;
use slog::Logger;
use sprockets_tls::keys::SprocketsConfig;
use std::sync::Arc;

use crate::bootstrap::rack_ops::RssAccess;

#[derive(Clone)]
pub(crate) struct BootstrapServerContext {
    pub(crate) base_log: Logger,
    pub(crate) global_zone_bootstrap_ip: Ipv6Addr,
    pub(crate) internal_disks_rx: InternalDisksReceiver,
    pub(crate) bootstore_node_handle: bootstore::NodeHandle,
    pub(crate) rss_access: RssAccess,
    pub(crate) sprockets_config: SprocketsConfig,
    pub(crate) trust_quorum_handle: trust_quorum::NodeTaskHandle,
    pub(crate) measurements: Arc<MeasurementsHandle>,
}

impl From<&BootstrapServerContext> for RssContext {
    fn from(value: &BootstrapServerContext) -> Self {
        RssContext {
            base_log: value.base_log.clone(),
            global_zone_bootstrap_ip: value.global_zone_bootstrap_ip,
            internal_disks_rx: value.internal_disks_rx.clone(),
            bootstore_node_handle: value.bootstore_node_handle.clone(),
            sprockets_config: value.sprockets_config.clone(),
            trust_quorum_handle: value.trust_quorum_handle.clone(),
            measurements: value.measurements.clone(),
        }
    }
}

impl BootstrapServerContext {
    /// This is mutually exclusive with `start_multirack_join`.
    pub(super) fn start_rack_initialize(
        &self,
        request: RackInitializeRequestParams,
    ) -> Result<RackInitUuid, RssAccessError> {
        self.rss_access.start_initializing(self.into(), request)
    }

    /// This is mutually exclusive with `start_rack_initialize`.
    pub(super) fn start_multirack_join(
        &self,
        request: MultirackJoinRequest,
    ) -> Result<MultirackJoinUuid, RssAccessError> {
        self.rss_access.start_multirack_join(self.into(), request)
    }
}

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

    async fn network_config_contents_for_debug(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ReplicatedNetworkConfig>, HttpError> {
        let ctx = rqctx.context();
        let contents = ctx.bootstore_node_handle.network_config_contents().map(
            |NetworkConfig { generation, blob }| {
                ReplicatedNetworkConfigContents {
                    generation,
                    base64_blob: base64::engine::general_purpose::STANDARD
                        .encode(&blob),
                }
            },
        );
        Ok(HttpResponseOk(ReplicatedNetworkConfig { contents }))
    }

    async fn baseboard_ids(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BaseboardIds>, HttpError> {
        let ctx = rqctx.context();

        // The trust quorum connection manager already knows who is connected.
        // It polls DDM and connects itself. We use its knowledge so that we
        // don't spam sprockets connections unnecessarily.
        let status =
            ctx.trust_quorum_handle.conn_mgr_status().await.map_err(|err| {
                HttpError::for_internal_error(err.to_string())
            })?;

        // We then also have to join our own information, since trust quorum
        // doesn't connect to itself.
        let ourself = BootstrapIpOfBaseboardId {
            id: ctx.trust_quorum_handle.baseboard_id().clone(),
            ip: ctx.global_zone_bootstrap_ip,
        };

        let data = status
            .connected_peers()
            .into_iter()
            .map(|(id, ip)| BootstrapIpOfBaseboardId { id, ip })
            .chain(std::iter::once(ourself))
            .collect();

        Ok(HttpResponseOk(BaseboardIds { data }))
    }

    async fn multirack_join(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<MultirackJoinRequest>,
    ) -> Result<HttpResponseOk<MultirackJoinUuid>, HttpError> {
        let ctx = rqctx.context();
        let request = body.into_inner();
        let id = ctx
            .start_multirack_join(request)
            .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
        Ok(HttpResponseOk(id))
    }

    async fn multirack_join_state(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<MultirackJoinServiceState>, HttpError> {
        let ctx = rqctx.context();
        let state =
            ctx.rss_access.get_multirack_join_state().map_err(|_| {
                HttpError::for_client_error(
                    Some("Conflict".to_string()),
                    ClientErrorStatusCode::CONFLICT,
                    "Cannot run multirack join: RSS has been run on this rack"
                        .to_string(),
                )
            })?;

        Ok(HttpResponseOk(state))
    }
}
