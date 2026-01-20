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
use bootstrap_agent_lockstep_api::{
    RackInitializeRequest as LockstepRackInitializeRequest,
    RackOperationStatus as LockstepRackOperationStatus,
    RssStep as LockstepRssStep,
};
use dropshot::{
    ApiDescription, HttpError, HttpResponseOk, RequestContext, TypedBody,
};
use omicron_uuid_kinds::RackInitUuid;
use omicron_uuid_kinds::RackResetUuid;
use sled_agent_types::rack_init::{
    BootstrapAddressDiscovery, RackInitializeRequest,
    RackInitializeRequestParams, RecoverySiloConfig,
};
use sled_agent_types::rack_ops::{RackOperationStatus, RssStep};

/// Returns a description of the bootstrap agent lockstep API
pub(crate) fn api() -> ApiDescription<BootstrapServerContext> {
    bootstrap_agent_lockstep_api_mod::api_description::<
        BootstrapAgentLockstepImpl,
    >()
    .expect("registered entrypoints successfully")
}

enum BootstrapAgentLockstepImpl {}

/// Convert from the lockstep API's RackInitializeRequest to the internal type.
fn convert_rack_initialize_request(
    lockstep: LockstepRackInitializeRequest,
) -> RackInitializeRequest {
    RackInitializeRequest {
        trust_quorum_peers: lockstep.trust_quorum_peers,
        bootstrap_discovery: match lockstep.bootstrap_discovery {
            bootstrap_agent_lockstep_api::BootstrapAddressDiscovery::OnlyOurs => {
                BootstrapAddressDiscovery::OnlyOurs
            }
            bootstrap_agent_lockstep_api::BootstrapAddressDiscovery::OnlyThese {
                addrs,
            } => BootstrapAddressDiscovery::OnlyThese { addrs },
        },
        ntp_servers: lockstep.ntp_servers,
        dns_servers: lockstep.dns_servers,
        internal_services_ip_pool_ranges: lockstep.internal_services_ip_pool_ranges,
        external_dns_ips: lockstep.external_dns_ips,
        external_dns_zone_name: lockstep.external_dns_zone_name,
        external_certificates: lockstep.external_certificates,
        recovery_silo: RecoverySiloConfig {
            silo_name: lockstep.recovery_silo.silo_name,
            user_name: lockstep.recovery_silo.user_name,
            user_password_hash: lockstep.recovery_silo.user_password_hash,
        },
        rack_network_config: lockstep.rack_network_config,
        allowed_source_ips: lockstep.allowed_source_ips,
    }
}

/// Convert from the internal RackOperationStatus to the lockstep API's type.
fn convert_rack_operation_status(
    status: RackOperationStatus,
) -> LockstepRackOperationStatus {
    match status {
        RackOperationStatus::Initializing { id, step } => {
            LockstepRackOperationStatus::Initializing {
                id,
                step: convert_rss_step(step),
            }
        }
        RackOperationStatus::Initialized { id } => {
            LockstepRackOperationStatus::Initialized { id }
        }
        RackOperationStatus::InitializationFailed { id, message } => {
            LockstepRackOperationStatus::InitializationFailed { id, message }
        }
        RackOperationStatus::InitializationPanicked { id } => {
            LockstepRackOperationStatus::InitializationPanicked { id }
        }
        RackOperationStatus::Resetting { id } => {
            LockstepRackOperationStatus::Resetting { id }
        }
        RackOperationStatus::Uninitialized { reset_id } => {
            LockstepRackOperationStatus::Uninitialized { reset_id }
        }
        RackOperationStatus::ResetFailed { id, message } => {
            LockstepRackOperationStatus::ResetFailed { id, message }
        }
        RackOperationStatus::ResetPanicked { id } => {
            LockstepRackOperationStatus::ResetPanicked { id }
        }
    }
}

fn convert_rss_step(step: RssStep) -> LockstepRssStep {
    match step {
        RssStep::Requested => LockstepRssStep::Requested,
        RssStep::Starting => LockstepRssStep::Starting,
        RssStep::LoadExistingPlan => LockstepRssStep::LoadExistingPlan,
        RssStep::CreateSledPlan => LockstepRssStep::CreateSledPlan,
        RssStep::InitTrustQuorum => LockstepRssStep::InitTrustQuorum,
        RssStep::NetworkConfigUpdate => LockstepRssStep::NetworkConfigUpdate,
        RssStep::SledInit => LockstepRssStep::SledInit,
        RssStep::InitDns => LockstepRssStep::InitDns,
        RssStep::ConfigureDns => LockstepRssStep::ConfigureDns,
        RssStep::InitNtp => LockstepRssStep::InitNtp,
        RssStep::WaitForTimeSync => LockstepRssStep::WaitForTimeSync,
        RssStep::WaitForDatabase => LockstepRssStep::WaitForDatabase,
        RssStep::ClusterInit => LockstepRssStep::ClusterInit,
        RssStep::ZonesInit => LockstepRssStep::ZonesInit,
        RssStep::NexusHandoff => LockstepRssStep::NexusHandoff,
    }
}

impl BootstrapAgentLockstepApi for BootstrapAgentLockstepImpl {
    type Context = BootstrapServerContext;

    async fn rack_initialization_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<LockstepRackOperationStatus>, HttpError> {
        let ctx = rqctx.context();
        let status = ctx.rss_access.operation_status();
        Ok(HttpResponseOk(convert_rack_operation_status(status)))
    }

    async fn rack_initialize(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<LockstepRackInitializeRequest>,
    ) -> Result<HttpResponseOk<RackInitUuid>, HttpError> {
        // Note that if we are performing rack initialization in
        // response to an external request, we assume we are not
        // skipping timesync.
        const SKIP_TIMESYNC: bool = false;
        let ctx = rqctx.context();
        let lockstep_request = body.into_inner();
        let request = convert_rack_initialize_request(lockstep_request);
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
            )
            .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
        Ok(HttpResponseOk(id))
    }
}
