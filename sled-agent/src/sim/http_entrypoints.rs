// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the sled agent's exposed API

use super::collection::PokeMode;
use camino::Utf8PathBuf;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::FreeformBody;
use dropshot::HttpError;
use dropshot::HttpResponseCreated;
use dropshot::HttpResponseDeleted;
use dropshot::HttpResponseHeaders;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::StreamingBody;
use dropshot::TypedBody;
use nexus_sled_agent_shared::inventory::SledRole;
use nexus_sled_agent_shared::inventory::{Inventory, OmicronZonesConfig};
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::SledVmmState;
use omicron_common::api::internal::nexus::UpdateArtifactId;
use omicron_common::api::internal::shared::ExternalIpGatewayMap;
use omicron_common::api::internal::shared::SledIdentifiers;
use omicron_common::api::internal::shared::VirtualNetworkInterfaceHost;
use omicron_common::api::internal::shared::{
    ResolvedVpcRouteSet, ResolvedVpcRouteState, SwitchPorts,
};
use omicron_common::disk::DatasetsConfig;
use omicron_common::disk::DatasetsManagementResult;
use omicron_common::disk::DisksManagementResult;
use omicron_common::disk::OmicronPhysicalDisksConfig;
use sled_agent_api::*;
use sled_agent_types::boot_disk::BootDiskOsWriteStatus;
use sled_agent_types::boot_disk::BootDiskPathParams;
use sled_agent_types::boot_disk::BootDiskUpdatePathParams;
use sled_agent_types::boot_disk::BootDiskWriteStartQueryParams;
use sled_agent_types::bootstore::BootstoreStatus;
use sled_agent_types::disk::DiskEnsureBody;
use sled_agent_types::early_networking::EarlyNetworkConfig;
use sled_agent_types::firewall_rules::VpcFirewallRulesEnsureBody;
use sled_agent_types::instance::InstanceEnsureBody;
use sled_agent_types::instance::InstanceExternalIpBody;
use sled_agent_types::instance::VmmPutStateBody;
use sled_agent_types::instance::VmmPutStateResponse;
use sled_agent_types::instance::VmmUnregisterResponse;
use sled_agent_types::sled::AddSledRequest;
use sled_agent_types::time_sync::TimeSync;
use sled_agent_types::zone_bundle::BundleUtilization;
use sled_agent_types::zone_bundle::CleanupContext;
use sled_agent_types::zone_bundle::CleanupCount;
use sled_agent_types::zone_bundle::ZoneBundleId;
use sled_agent_types::zone_bundle::ZoneBundleMetadata;
use std::collections::BTreeMap;
use std::sync::Arc;

use super::sled_agent::SledAgent;

type SledApiDescription = ApiDescription<Arc<SledAgent>>;

/// Returns a description of the sled agent API
pub fn api() -> SledApiDescription {
    fn register_endpoints() -> Result<SledApiDescription, anyhow::Error> {
        let mut api = sled_agent_api::sled_agent_api_mod::api_description::<
            SledAgentSimImpl,
        >()?;
        api.register(instance_poke_post)?;
        api.register(instance_poke_single_step_post)?;
        api.register(instance_post_sim_migration_source)?;
        api.register(disk_poke_post)?;

        Ok(api)
    }

    register_endpoints().expect("failed to register entrypoints")
}

enum SledAgentSimImpl {}

impl SledAgentApi for SledAgentSimImpl {
    type Context = Arc<SledAgent>;

    async fn vmm_register(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        let sa = rqctx.context();
        let propolis_id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        Ok(HttpResponseOk(sa.instance_register(propolis_id, body_args).await?))
    }

    async fn vmm_unregister(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
    ) -> Result<HttpResponseOk<VmmUnregisterResponse>, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        Ok(HttpResponseOk(sa.instance_unregister(id).await?))
    }

    async fn vmm_put_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<VmmPutStateBody>,
    ) -> Result<HttpResponseOk<VmmPutStateResponse>, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        Ok(HttpResponseOk(sa.instance_ensure_state(id, body_args.state).await?))
    }

    async fn vmm_get_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        Ok(HttpResponseOk(sa.instance_get_state(id).await?))
    }

    async fn vmm_put_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        sa.instance_put_external_ip(id, &body_args).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn vmm_delete_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        sa.instance_delete_external_ip(id, &body_args).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn disk_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<DiskPathParam>,
        body: TypedBody<DiskEnsureBody>,
    ) -> Result<HttpResponseOk<DiskRuntimeState>, HttpError> {
        let sa = rqctx.context();
        let disk_id = path_params.into_inner().disk_id;
        let body_args = body.into_inner();
        Ok(HttpResponseOk(
            sa.disk_ensure(
                disk_id,
                body_args.initial_runtime.clone(),
                body_args.target.clone(),
            )
            .await?,
        ))
    }

    async fn update_artifact(
        rqctx: RequestContext<Self::Context>,
        artifact: TypedBody<UpdateArtifactId>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        sa.updates()
            .download_artifact(
                artifact.into_inner(),
                rqctx.context().nexus_client.as_ref(),
            )
            .await
            .map_err(|e| HttpError::for_internal_error(e.to_string()))?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn vmm_issue_disk_snapshot_request(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmIssueDiskSnapshotRequestPathParam>,
        body: TypedBody<VmmIssueDiskSnapshotRequestBody>,
    ) -> Result<HttpResponseOk<VmmIssueDiskSnapshotRequestResponse>, HttpError>
    {
        let sa = rqctx.context();
        let path_params = path_params.into_inner();
        let body = body.into_inner();

        sa.instance_issue_disk_snapshot_request(
            path_params.propolis_id,
            path_params.disk_id,
            body.snapshot_id,
        )
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

        Ok(HttpResponseOk(VmmIssueDiskSnapshotRequestResponse {
            snapshot_id: body.snapshot_id,
        }))
    }

    async fn vpc_firewall_rules_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VpcPathParam>,
        body: TypedBody<VpcFirewallRulesEnsureBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let _sa = rqctx.context();
        let _vpc_id = path_params.into_inner().vpc_id;
        let _body_args = body.into_inner();

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn set_v2p(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<VirtualNetworkInterfaceHost>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let body_args = body.into_inner();

        sa.set_virtual_nic_host(&body_args)
            .await
            .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn del_v2p(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<VirtualNetworkInterfaceHost>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let body_args = body.into_inner();

        sa.unset_virtual_nic_host(&body_args)
            .await
            .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn list_v2p(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<VirtualNetworkInterfaceHost>>, HttpError>
    {
        let sa = rqctx.context();

        let vnics = sa.list_virtual_nics().await.map_err(HttpError::from)?;

        Ok(HttpResponseOk(vnics))
    }

    async fn uplink_ensure(
        _rqctx: RequestContext<Self::Context>,
        _body: TypedBody<SwitchPorts>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn read_network_bootstore_config_cache(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<EarlyNetworkConfig>, HttpError> {
        let config =
            rqctx.context().bootstore_network_config.lock().await.clone();
        Ok(HttpResponseOk(config))
    }

    async fn write_network_bootstore_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<EarlyNetworkConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let mut config = rqctx.context().bootstore_network_config.lock().await;
        *config = body.into_inner();
        Ok(HttpResponseUpdatedNoContent())
    }

    /// Fetch basic information about this sled
    async fn inventory(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Inventory>, HttpError> {
        let sa = rqctx.context();
        Ok(HttpResponseOk(
            sa.inventory(rqctx.server.local_addr).await.map_err(|e| {
                HttpError::for_internal_error(format!("{:#}", e))
            })?,
        ))
    }

    async fn datasets_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<DatasetsConfig>,
    ) -> Result<HttpResponseOk<DatasetsManagementResult>, HttpError> {
        let sa = rqctx.context();
        let body_args = body.into_inner();
        let result = sa.datasets_ensure(body_args).await?;
        Ok(HttpResponseOk(result))
    }

    async fn datasets_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<DatasetsConfig>, HttpError> {
        let sa = rqctx.context();
        Ok(HttpResponseOk(sa.datasets_config_list().await?))
    }

    async fn omicron_physical_disks_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<OmicronPhysicalDisksConfig>,
    ) -> Result<HttpResponseOk<DisksManagementResult>, HttpError> {
        let sa = rqctx.context();
        let body_args = body.into_inner();
        let result = sa.omicron_physical_disks_ensure(body_args).await?;
        Ok(HttpResponseOk(result))
    }

    async fn omicron_physical_disks_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<OmicronPhysicalDisksConfig>, HttpError> {
        let sa = rqctx.context();
        Ok(HttpResponseOk(sa.omicron_physical_disks_list().await?))
    }

    async fn omicron_zones_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<OmicronZonesConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let body_args = body.into_inner();
        sa.omicron_zones_ensure(body_args).await;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn sled_add(
        _rqctx: RequestContext<Self::Context>,
        _body: TypedBody<AddSledRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn list_vpc_routes(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<ResolvedVpcRouteState>>, HttpError> {
        let sa = rqctx.context();
        Ok(HttpResponseOk(sa.list_vpc_routes().await))
    }

    async fn set_vpc_routes(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<Vec<ResolvedVpcRouteSet>>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        sa.set_vpc_routes(body.into_inner()).await;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn set_eip_gateways(
        rqctx: RequestContext<Self::Context>,
        _body: TypedBody<ExternalIpGatewayMap>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let _sa = rqctx.context();
        // sa.set_vpc_routes(body.into_inner()).await;
        Ok(HttpResponseUpdatedNoContent())
    }

    // --- Unimplemented endpoints ---

    async fn zone_bundle_list_all(
        _rqctx: RequestContext<Self::Context>,
        _query: Query<ZoneBundleFilter>,
    ) -> Result<HttpResponseOk<Vec<ZoneBundleMetadata>>, HttpError> {
        method_unimplemented()
    }

    async fn zone_bundle_list(
        _rqctx: RequestContext<Self::Context>,
        _params: Path<ZonePathParam>,
    ) -> Result<HttpResponseOk<Vec<ZoneBundleMetadata>>, HttpError> {
        method_unimplemented()
    }

    async fn zone_bundle_create(
        _rqctx: RequestContext<Self::Context>,
        _params: Path<ZonePathParam>,
    ) -> Result<HttpResponseCreated<ZoneBundleMetadata>, HttpError> {
        method_unimplemented()
    }

    async fn zone_bundle_get(
        _rqctx: RequestContext<Self::Context>,
        _params: Path<ZoneBundleId>,
    ) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError>
    {
        method_unimplemented()
    }

    async fn zone_bundle_delete(
        _rqctx: RequestContext<Self::Context>,
        _params: Path<ZoneBundleId>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        method_unimplemented()
    }

    async fn zone_bundle_utilization(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<BTreeMap<Utf8PathBuf, BundleUtilization>>,
        HttpError,
    > {
        method_unimplemented()
    }

    async fn zone_bundle_cleanup_context(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<CleanupContext>, HttpError> {
        method_unimplemented()
    }

    async fn zone_bundle_cleanup_context_update(
        _rqctx: RequestContext<Self::Context>,
        _body: TypedBody<CleanupContextUpdate>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        method_unimplemented()
    }

    async fn zone_bundle_cleanup(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BTreeMap<Utf8PathBuf, CleanupCount>>, HttpError>
    {
        method_unimplemented()
    }

    async fn zones_list(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
        method_unimplemented()
    }

    async fn zpools_get(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<Zpool>>, HttpError> {
        method_unimplemented()
    }

    async fn sled_role_get(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledRole>, HttpError> {
        method_unimplemented()
    }

    async fn cockroachdb_init(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        method_unimplemented()
    }

    async fn timesync_get(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<TimeSync>, HttpError> {
        method_unimplemented()
    }

    async fn host_os_write_start(
        _rqctx: RequestContext<Self::Context>,
        _path_params: Path<BootDiskPathParams>,
        _query_params: Query<BootDiskWriteStartQueryParams>,
        _body: StreamingBody,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        method_unimplemented()
    }

    async fn host_os_write_status_get(
        _rqctx: RequestContext<Self::Context>,
        _path_params: Path<BootDiskPathParams>,
    ) -> Result<HttpResponseOk<BootDiskOsWriteStatus>, HttpError> {
        method_unimplemented()
    }

    async fn host_os_write_status_delete(
        _rqctx: RequestContext<Self::Context>,
        _path_params: Path<BootDiskUpdatePathParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        method_unimplemented()
    }

    async fn sled_identifiers(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledIdentifiers>, HttpError> {
        method_unimplemented()
    }

    async fn bootstore_status(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BootstoreStatus>, HttpError> {
        method_unimplemented()
    }
}

fn method_unimplemented<T>() -> Result<T, HttpError> {
    Err(HttpError {
        // Use a client error here (405 Method Not Allowed vs 501 Not
        // Implemented) even though it isn't strictly accurate here, so tests
        // get to see the error message.
        status_code: http::StatusCode::METHOD_NOT_ALLOWED,
        error_code: None,
        external_message: "Method not implemented in sled-agent-sim"
            .to_string(),
        internal_message: "Method not implemented in sled-agent-sim"
            .to_string(),
    })
}

// --- Extra endpoints only available in the sim implementation ---

#[endpoint {
    method = POST,
    path = "/vmms/{propolis_id}/poke",
}]
async fn instance_poke_post(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<VmmPathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let id = path_params.into_inner().propolis_id;
    sa.vmm_poke(id, PokeMode::Drain).await;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/vmms/{propolis_id}/poke-single-step",
}]
async fn instance_poke_single_step_post(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<VmmPathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let id = path_params.into_inner().propolis_id;
    sa.vmm_poke(id, PokeMode::SingleStep).await;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/vmms/{propolis_id}/sim-migration-source",
}]
async fn instance_post_sim_migration_source(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<VmmPathParam>,
    body: TypedBody<super::instance::SimulateMigrationSource>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let id = path_params.into_inner().propolis_id;
    sa.instance_simulate_migration_source(id, body.into_inner()).await?;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/disks/{disk_id}/poke",
}]
async fn disk_poke_post(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<DiskPathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let disk_id = path_params.into_inner().disk_id;
    sa.disk_poke(disk_id).await;
    Ok(HttpResponseUpdatedNoContent())
}
