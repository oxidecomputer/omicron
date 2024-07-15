// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the sled agent's exposed API

use super::sled_agent::SledAgent;
use crate::bootstrap::early_networking::EarlyNetworkConfig;
use crate::bootstrap::params::AddSledRequest;
use crate::params::{
    BootstoreStatus, CleanupContextUpdate, DiskEnsureBody, InstanceEnsureBody,
    InstanceExternalIpBody, InstancePutMigrationIdsBody, InstancePutStateBody,
    InstancePutStateResponse, InstanceUnregisterResponse, Inventory,
    OmicronPhysicalDisksConfig, OmicronZonesConfig, SledRole, TimeSync,
    VpcFirewallRulesEnsureBody, ZoneBundleId, ZoneBundleMetadata, Zpool,
};
use crate::sled_agent::Error as SledAgentError;
use crate::zone_bundle;
use bootstore::schemes::v0::NetworkConfig;
use camino::Utf8PathBuf;
use display_error_chain::DisplayErrorChain;
use dropshot::{
    endpoint, ApiDescription, ApiDescriptionRegisterError, FreeformBody,
    HttpError, HttpResponseCreated, HttpResponseDeleted, HttpResponseHeaders,
    HttpResponseOk, HttpResponseUpdatedNoContent, Path, Query, RequestContext,
    StreamingBody, TypedBody,
};
use illumos_utils::opte::params::VirtualNetworkInterfaceHost;
use installinator_common::M2Slot;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::{
    DiskRuntimeState, SledInstanceState, UpdateArtifactId,
};
use omicron_common::api::internal::shared::{
    ResolvedVpcRouteSet, ResolvedVpcRouteState, SwitchPorts,
};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware::DiskVariant;
use sled_storage::resources::DisksManagementResult;
use std::collections::BTreeMap;
use uuid::Uuid;

type SledApiDescription = ApiDescription<SledAgent>;

/// Returns a description of the sled agent API
pub fn api() -> SledApiDescription {
    fn register_endpoints(
        api: &mut SledApiDescription,
    ) -> Result<(), ApiDescriptionRegisterError> {
        api.register(disk_put)?;
        api.register(cockroachdb_init)?;
        api.register(instance_issue_disk_snapshot_request)?;
        api.register(instance_put_migration_ids)?;
        api.register(instance_put_state)?;
        api.register(instance_get_state)?;
        api.register(instance_put_external_ip)?;
        api.register(instance_delete_external_ip)?;
        api.register(instance_register)?;
        api.register(instance_unregister)?;
        api.register(omicron_zones_get)?;
        api.register(omicron_zones_put)?;
        api.register(zones_list)?;
        api.register(omicron_physical_disks_get)?;
        api.register(omicron_physical_disks_put)?;
        api.register(zone_bundle_list)?;
        api.register(zone_bundle_list_all)?;
        api.register(zone_bundle_create)?;
        api.register(zone_bundle_get)?;
        api.register(zone_bundle_delete)?;
        api.register(zone_bundle_utilization)?;
        api.register(zone_bundle_cleanup_context)?;
        api.register(zone_bundle_cleanup_context_update)?;
        api.register(zone_bundle_cleanup)?;
        api.register(sled_role_get)?;
        api.register(list_v2p)?;
        api.register(set_v2p)?;
        api.register(del_v2p)?;
        api.register(timesync_get)?;
        api.register(update_artifact)?;
        api.register(vpc_firewall_rules_put)?;
        api.register(zpools_get)?;
        api.register(uplink_ensure)?;
        api.register(read_network_bootstore_config_cache)?;
        api.register(write_network_bootstore_config)?;
        api.register(sled_add)?;
        api.register(host_os_write_start)?;
        api.register(host_os_write_status_get)?;
        api.register(host_os_write_status_delete)?;
        api.register(inventory)?;
        api.register(bootstore_status)?;
        api.register(list_vpc_routes)?;
        api.register(set_vpc_routes)?;

        Ok(())
    }

    let mut api = SledApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct ZonePathParam {
    /// The name of the zone.
    zone_name: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct ZoneBundleFilter {
    /// An optional substring used to filter zone bundles.
    filter: Option<String>,
}

/// List all zone bundles that exist, even for now-deleted zones.
#[endpoint {
    method = GET,
    path = "/zones/bundles",
}]
async fn zone_bundle_list_all(
    rqctx: RequestContext<SledAgent>,
    query: Query<ZoneBundleFilter>,
) -> Result<HttpResponseOk<Vec<ZoneBundleMetadata>>, HttpError> {
    let sa = rqctx.context();
    let filter = query.into_inner().filter;
    sa.list_all_zone_bundles(filter.as_deref())
        .await
        .map(HttpResponseOk)
        .map_err(HttpError::from)
}

/// List the zone bundles that are available for a running zone.
#[endpoint {
    method = GET,
    path = "/zones/bundles/{zone_name}",
}]
async fn zone_bundle_list(
    rqctx: RequestContext<SledAgent>,
    params: Path<ZonePathParam>,
) -> Result<HttpResponseOk<Vec<ZoneBundleMetadata>>, HttpError> {
    let params = params.into_inner();
    let zone_name = params.zone_name;
    let sa = rqctx.context();
    sa.list_zone_bundles(&zone_name)
        .await
        .map(HttpResponseOk)
        .map_err(HttpError::from)
}

/// Ask the sled agent to create a zone bundle.
#[endpoint {
    method = POST,
    path = "/zones/bundles/{zone_name}",
}]
async fn zone_bundle_create(
    rqctx: RequestContext<SledAgent>,
    params: Path<ZonePathParam>,
) -> Result<HttpResponseCreated<ZoneBundleMetadata>, HttpError> {
    let params = params.into_inner();
    let zone_name = params.zone_name;
    let sa = rqctx.context();
    sa.create_zone_bundle(&zone_name)
        .await
        .map(HttpResponseCreated)
        .map_err(HttpError::from)
}

/// Fetch the binary content of a single zone bundle.
#[endpoint {
    method = GET,
    path = "/zones/bundles/{zone_name}/{bundle_id}",
}]
async fn zone_bundle_get(
    rqctx: RequestContext<SledAgent>,
    params: Path<ZoneBundleId>,
) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError> {
    let params = params.into_inner();
    let zone_name = params.zone_name;
    let bundle_id = params.bundle_id;
    let sa = rqctx.context();
    let Some(path) = sa
        .get_zone_bundle_paths(&zone_name, &bundle_id)
        .await
        .map_err(HttpError::from)?
        .into_iter()
        .next()
    else {
        return Err(HttpError::for_not_found(
            None,
            format!(
                "No zone bundle for zone '{}' with ID '{}'",
                zone_name, bundle_id
            ),
        ));
    };
    let f = tokio::fs::File::open(&path).await.map_err(|e| {
        HttpError::for_internal_error(format!(
            "failed to open zone bundle file at {}: {:?}",
            path, e,
        ))
    })?;
    let stream = hyper_staticfile::FileBytesStream::new(f);
    let body = FreeformBody(stream.into_body());
    let mut response = HttpResponseHeaders::new_unnamed(HttpResponseOk(body));
    response.headers_mut().append(
        http::header::CONTENT_TYPE,
        "application/gzip".try_into().unwrap(),
    );
    Ok(response)
}

/// Delete a zone bundle.
#[endpoint {
    method = DELETE,
    path = "/zones/bundles/{zone_name}/{bundle_id}",
}]
async fn zone_bundle_delete(
    rqctx: RequestContext<SledAgent>,
    params: Path<ZoneBundleId>,
) -> Result<HttpResponseDeleted, HttpError> {
    let params = params.into_inner();
    let zone_name = params.zone_name;
    let bundle_id = params.bundle_id;
    let sa = rqctx.context();
    let paths = sa
        .get_zone_bundle_paths(&zone_name, &bundle_id)
        .await
        .map_err(HttpError::from)?;
    if paths.is_empty() {
        return Err(HttpError::for_not_found(
            None,
            format!(
                "No zone bundle for zone '{}' with ID '{}'",
                zone_name, bundle_id
            ),
        ));
    };
    for path in paths.into_iter() {
        tokio::fs::remove_file(&path).await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "Failed to delete zone bundle: {e}"
            ))
        })?;
    }
    Ok(HttpResponseDeleted())
}

/// Return utilization information about all zone bundles.
#[endpoint {
    method = GET,
    path = "/zones/bundle-cleanup/utilization",
}]
async fn zone_bundle_utilization(
    rqctx: RequestContext<SledAgent>,
) -> Result<
    HttpResponseOk<BTreeMap<Utf8PathBuf, zone_bundle::BundleUtilization>>,
    HttpError,
> {
    let sa = rqctx.context();
    sa.zone_bundle_utilization()
        .await
        .map(HttpResponseOk)
        .map_err(HttpError::from)
}

/// Return context used by the zone-bundle cleanup task.
#[endpoint {
    method = GET,
    path = "/zones/bundle-cleanup/context",
}]
async fn zone_bundle_cleanup_context(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<zone_bundle::CleanupContext>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.zone_bundle_cleanup_context().await))
}

/// Update context used by the zone-bundle cleanup task.
#[endpoint {
    method = PUT,
    path = "/zones/bundle-cleanup/context",
}]
async fn zone_bundle_cleanup_context_update(
    rqctx: RequestContext<SledAgent>,
    body: TypedBody<CleanupContextUpdate>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let params = body.into_inner();
    let new_period = params
        .period
        .map(zone_bundle::CleanupPeriod::new)
        .transpose()
        .map_err(|e| HttpError::from(SledAgentError::from(e)))?;
    let new_priority = params.priority;
    let new_limit = params
        .storage_limit
        .map(zone_bundle::StorageLimit::new)
        .transpose()
        .map_err(|e| HttpError::from(SledAgentError::from(e)))?;
    sa.update_zone_bundle_cleanup_context(new_period, new_limit, new_priority)
        .await
        .map(|_| HttpResponseUpdatedNoContent())
        .map_err(HttpError::from)
}

/// Trigger a zone bundle cleanup.
#[endpoint {
    method = POST,
    path = "/zones/bundle-cleanup",
}]
async fn zone_bundle_cleanup(
    rqctx: RequestContext<SledAgent>,
) -> Result<
    HttpResponseOk<BTreeMap<Utf8PathBuf, zone_bundle::CleanupCount>>,
    HttpError,
> {
    let sa = rqctx.context();
    sa.zone_bundle_cleanup().await.map(HttpResponseOk).map_err(HttpError::from)
}

/// List the zones that are currently managed by the sled agent.
#[endpoint {
    method = GET,
    path = "/zones",
}]
async fn zones_list(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
    let sa = rqctx.context();
    sa.zones_list().await.map(HttpResponseOk).map_err(HttpError::from)
}

#[endpoint {
    method = GET,
    path = "/omicron-zones",
}]
async fn omicron_zones_get(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<OmicronZonesConfig>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.omicron_zones_list().await?))
}

#[endpoint {
    method = PUT,
    path = "/omicron-physical-disks",
}]
async fn omicron_physical_disks_put(
    rqctx: RequestContext<SledAgent>,
    body: TypedBody<OmicronPhysicalDisksConfig>,
) -> Result<HttpResponseOk<DisksManagementResult>, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();
    let result = sa.omicron_physical_disks_ensure(body_args).await?;
    Ok(HttpResponseOk(result))
}

#[endpoint {
    method = GET,
    path = "/omicron-physical-disks",
}]
async fn omicron_physical_disks_get(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<OmicronPhysicalDisksConfig>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.omicron_physical_disks_list().await?))
}

#[endpoint {
    method = PUT,
    path = "/omicron-zones",
}]
async fn omicron_zones_put(
    rqctx: RequestContext<SledAgent>,
    body: TypedBody<OmicronZonesConfig>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();
    sa.omicron_zones_ensure(body_args).await?;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = GET,
    path = "/zpools",
}]
async fn zpools_get(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<Vec<Zpool>>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.zpools_get().await))
}

#[endpoint {
    method = GET,
    path = "/sled-role",
}]
async fn sled_role_get(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<SledRole>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.get_role()))
}

/// Initializes a CockroachDB cluster
#[endpoint {
    method = POST,
    path = "/cockroachdb",
}]
async fn cockroachdb_init(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    sa.cockroachdb_initialize().await?;
    Ok(HttpResponseUpdatedNoContent())
}

/// Path parameters for Instance requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
struct InstancePathParam {
    instance_id: InstanceUuid,
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}",
}]
async fn instance_register(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstanceEnsureBody>,
) -> Result<HttpResponseOk<SledInstanceState>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.instance_ensure_registered(
            instance_id,
            body_args.propolis_id,
            body_args.hardware,
            body_args.instance_runtime,
            body_args.vmm_runtime,
            body_args.propolis_addr,
            body_args.metadata,
        )
        .await?,
    ))
}

#[endpoint {
    method = DELETE,
    path = "/instances/{instance_id}",
}]
async fn instance_unregister(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<InstanceUnregisterResponse>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    Ok(HttpResponseOk(sa.instance_ensure_unregistered(instance_id).await?))
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}/state",
}]
async fn instance_put_state(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstancePutStateBody>,
) -> Result<HttpResponseOk<InstancePutStateResponse>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.instance_ensure_state(instance_id, body_args.state).await?,
    ))
}

#[endpoint {
    method = GET,
    path = "/instances/{instance_id}/state",
}]
async fn instance_get_state(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<SledInstanceState>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    Ok(HttpResponseOk(sa.instance_get_state(instance_id).await?))
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}/migration-ids",
}]
async fn instance_put_migration_ids(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstancePutMigrationIdsBody>,
) -> Result<HttpResponseOk<SledInstanceState>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.instance_put_migration_ids(
            instance_id,
            &body_args.old_runtime,
            &body_args.migration_params,
        )
        .await?,
    ))
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}/external-ip",
}]
async fn instance_put_external_ip(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstanceExternalIpBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    sa.instance_put_external_ip(instance_id, &body_args).await?;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = DELETE,
    path = "/instances/{instance_id}/external-ip",
}]
async fn instance_delete_external_ip(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstanceExternalIpBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    sa.instance_delete_external_ip(instance_id, &body_args).await?;
    Ok(HttpResponseUpdatedNoContent())
}

/// Path parameters for Disk requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
struct DiskPathParam {
    disk_id: Uuid,
}

#[endpoint {
    method = PUT,
    path = "/disks/{disk_id}",
}]
async fn disk_put(
    rqctx: RequestContext<SledAgent>,
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
        .await
        .map_err(|e| Error::from(e))?,
    ))
}

#[endpoint {
    method = POST,
    path = "/update"
}]
async fn update_artifact(
    rqctx: RequestContext<SledAgent>,
    artifact: TypedBody<UpdateArtifactId>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    sa.update_artifact(artifact.into_inner()).await.map_err(Error::from)?;
    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub struct InstanceIssueDiskSnapshotRequestPathParam {
    instance_id: Uuid,
    disk_id: Uuid,
}

#[derive(Deserialize, JsonSchema)]
pub struct InstanceIssueDiskSnapshotRequestBody {
    snapshot_id: Uuid,
}

#[derive(Serialize, JsonSchema)]
pub struct InstanceIssueDiskSnapshotRequestResponse {
    snapshot_id: Uuid,
}

/// Take a snapshot of a disk that is attached to an instance
#[endpoint {
    method = POST,
    path = "/instances/{instance_id}/disks/{disk_id}/snapshot",
}]
async fn instance_issue_disk_snapshot_request(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstanceIssueDiskSnapshotRequestPathParam>,
    body: TypedBody<InstanceIssueDiskSnapshotRequestBody>,
) -> Result<HttpResponseOk<InstanceIssueDiskSnapshotRequestResponse>, HttpError>
{
    let sa = rqctx.context();
    let path_params = path_params.into_inner();
    let body = body.into_inner();

    sa.instance_issue_disk_snapshot_request(
        InstanceUuid::from_untyped_uuid(path_params.instance_id),
        path_params.disk_id,
        body.snapshot_id,
    )
    .await?;

    Ok(HttpResponseOk(InstanceIssueDiskSnapshotRequestResponse {
        snapshot_id: body.snapshot_id,
    }))
}

/// Path parameters for VPC requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
struct VpcPathParam {
    vpc_id: Uuid,
}

#[endpoint {
    method = PUT,
    path = "/vpc/{vpc_id}/firewall/rules",
}]
async fn vpc_firewall_rules_put(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<VpcPathParam>,
    body: TypedBody<VpcFirewallRulesEnsureBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let _vpc_id = path_params.into_inner().vpc_id;
    let body_args = body.into_inner();

    sa.firewall_rules_ensure(body_args.vni, &body_args.rules[..])
        .await
        .map_err(Error::from)?;

    Ok(HttpResponseUpdatedNoContent())
}

/// Create a mapping from a virtual NIC to a physical host
// Keep interface_id to maintain parity with the simulated sled agent, which
// requires interface_id on the path.
#[endpoint {
    method = PUT,
    path = "/v2p/",
}]
async fn set_v2p(
    rqctx: RequestContext<SledAgent>,
    body: TypedBody<VirtualNetworkInterfaceHost>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();

    sa.set_virtual_nic_host(&body_args).await.map_err(Error::from)?;

    Ok(HttpResponseUpdatedNoContent())
}

/// Delete a mapping from a virtual NIC to a physical host
// Keep interface_id to maintain parity with the simulated sled agent, which
// requires interface_id on the path.
#[endpoint {
    method = DELETE,
    path = "/v2p/",
}]
async fn del_v2p(
    rqctx: RequestContext<SledAgent>,
    body: TypedBody<VirtualNetworkInterfaceHost>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();

    sa.unset_virtual_nic_host(&body_args).await.map_err(Error::from)?;

    Ok(HttpResponseUpdatedNoContent())
}

/// List v2p mappings present on sled
// Used by nexus background task
#[endpoint {
    method = GET,
    path = "/v2p/",
}]
async fn list_v2p(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<Vec<VirtualNetworkInterfaceHost>>, HttpError> {
    let sa = rqctx.context();

    let vnics = sa.list_virtual_nics().await.map_err(Error::from)?;

    Ok(HttpResponseOk(vnics))
}

#[endpoint {
    method = GET,
    path = "/timesync",
}]
async fn timesync_get(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<TimeSync>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.timesync_get().await.map_err(|e| Error::from(e))?))
}

#[endpoint {
    method = POST,
    path = "/switch-ports",
}]
async fn uplink_ensure(
    rqctx: RequestContext<SledAgent>,
    body: TypedBody<SwitchPorts>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    sa.ensure_scrimlet_host_ports(body.into_inner().uplinks).await?;
    Ok(HttpResponseUpdatedNoContent())
}

/// This API endpoint is only reading the local sled agent's view of the
/// bootstore. The boostore is a distributed data store that is eventually
/// consistent. Reads from individual nodes may not represent the latest state.
#[endpoint {
    method = GET,
    path = "/network-bootstore-config",
}]
async fn read_network_bootstore_config_cache(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<EarlyNetworkConfig>, HttpError> {
    let sa = rqctx.context();
    let bs = sa.bootstore();

    let config = bs.get_network_config().await.map_err(|e| {
        HttpError::for_internal_error(format!("failed to get bootstore: {e}"))
    })?;

    let config = match config {
        Some(config) => EarlyNetworkConfig::deserialize_bootstore_config(
            &rqctx.log, &config,
        )
        .map_err(|e| {
            HttpError::for_internal_error(format!(
                "deserialize early network config: {e}"
            ))
        })?,
        None => {
            return Err(HttpError::for_unavail(
                None,
                "early network config does not exist yet".into(),
            ));
        }
    };

    Ok(HttpResponseOk(config))
}

#[endpoint {
    method = PUT,
    path = "/network-bootstore-config",
}]
async fn write_network_bootstore_config(
    rqctx: RequestContext<SledAgent>,
    body: TypedBody<EarlyNetworkConfig>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let bs = sa.bootstore();
    let config = body.into_inner();

    bs.update_network_config(NetworkConfig::from(config)).await.map_err(
        |e| {
            HttpError::for_internal_error(format!(
                "failed to write updated config to boot store: {e}"
            ))
        },
    )?;

    Ok(HttpResponseUpdatedNoContent())
}

/// Add a sled to a rack that was already initialized via RSS
#[endpoint {
    method = PUT,
    path = "/sleds"
}]
async fn sled_add(
    rqctx: RequestContext<SledAgent>,
    body: TypedBody<AddSledRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let request = body.into_inner();

    // Perform some minimal validation
    if request.start_request.body.use_trust_quorum
        && !request.start_request.body.is_lrtq_learner
    {
        return Err(HttpError::for_bad_request(
            None,
            "New sleds must be LRTQ learners if trust quorum is in use"
                .to_string(),
        ));
    }

    crate::sled_agent::sled_add(
        sa.logger().clone(),
        request.sled_id,
        request.start_request,
    )
    .await
    .map_err(|e| {
        let message = format!("Failed to add sled to rack cluster: {e}");
        HttpError {
            status_code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error_code: None,
            external_message: message.clone(),
            internal_message: message,
        }
    })?;
    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BootDiskPathParams {
    pub boot_disk: M2Slot,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BootDiskUpdatePathParams {
    pub boot_disk: M2Slot,
    pub update_id: Uuid,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BootDiskWriteStartQueryParams {
    pub update_id: Uuid,
    // TODO do we already have sha2-256 hashes of the OS images, and if so
    // should we use that instead? Another option is to use the external API
    // `Digest` type, although it predates `serde_human_bytes` so just stores
    // the hash as a `String`.
    #[serde(with = "serde_human_bytes::hex_array")]
    #[schemars(schema_with = "omicron_common::hex_schema::<32>")]
    pub sha3_256_digest: [u8; 32],
}

/// Write a new host OS image to the specified boot disk
#[endpoint {
    method = POST,
    path = "/boot-disk/{boot_disk}/os/write",
}]
async fn host_os_write_start(
    request_context: RequestContext<SledAgent>,
    path_params: Path<BootDiskPathParams>,
    query_params: Query<BootDiskWriteStartQueryParams>,
    body: StreamingBody,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = request_context.context();
    let boot_disk = path_params.into_inner().boot_disk;

    // Find our corresponding disk.
    let maybe_disk_path =
        sa.storage().get_latest_disks().await.iter_managed().find_map(
            |(_identity, disk)| {
                // Synthetic disks panic if asked for their `slot()`, so filter
                // them out first; additionally, filter out any non-M2 disks.
                if disk.is_synthetic() || disk.variant() != DiskVariant::M2 {
                    return None;
                }

                // Convert this M2 disk's slot to an M2Slot, and skip any that
                // don't match the requested boot_disk.
                let Ok(slot) = M2Slot::try_from(disk.slot()) else {
                    return None;
                };
                if slot != boot_disk {
                    return None;
                }

                let raw_devs_path = true;
                Some(disk.boot_image_devfs_path(raw_devs_path))
            },
        );

    let disk_path = match maybe_disk_path {
        Some(Ok(path)) => path,
        Some(Err(err)) => {
            let message = format!(
                "failed to find devfs path for {boot_disk:?}: {}",
                DisplayErrorChain::new(&err)
            );
            return Err(HttpError {
                status_code: http::StatusCode::SERVICE_UNAVAILABLE,
                error_code: None,
                external_message: message.clone(),
                internal_message: message,
            });
        }
        None => {
            let message = format!("no disk found for slot {boot_disk:?}",);
            return Err(HttpError {
                status_code: http::StatusCode::SERVICE_UNAVAILABLE,
                error_code: None,
                external_message: message.clone(),
                internal_message: message,
            });
        }
    };

    let BootDiskWriteStartQueryParams { update_id, sha3_256_digest } =
        query_params.into_inner();
    sa.boot_disk_os_writer()
        .start_update(
            boot_disk,
            disk_path,
            update_id,
            sha3_256_digest,
            body.into_stream(),
        )
        .await
        .map_err(|err| HttpError::from(&*err))?;
    Ok(HttpResponseUpdatedNoContent())
}

/// Current progress of an OS image being written to disk.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema, Serialize,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum BootDiskOsWriteProgress {
    /// The image is still being uploaded.
    ReceivingUploadedImage { bytes_received: usize },
    /// The image is being written to disk.
    WritingImageToDisk { bytes_written: usize },
    /// The image is being read back from disk for validation.
    ValidatingWrittenImage { bytes_read: usize },
}

/// Status of an update to a boot disk OS.
#[derive(Debug, Clone, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BootDiskOsWriteStatus {
    /// No update has been started for this disk, or any previously-started
    /// update has completed and had its status cleared.
    NoUpdateStarted,
    /// An update is currently running.
    InProgress { update_id: Uuid, progress: BootDiskOsWriteProgress },
    /// The most recent update completed successfully.
    Complete { update_id: Uuid },
    /// The most recent update failed.
    Failed { update_id: Uuid, message: String },
}

/// Get the status of writing a new host OS
#[endpoint {
    method = GET,
    path = "/boot-disk/{boot_disk}/os/write/status",
}]
async fn host_os_write_status_get(
    request_context: RequestContext<SledAgent>,
    path_params: Path<BootDiskPathParams>,
) -> Result<HttpResponseOk<BootDiskOsWriteStatus>, HttpError> {
    let sa = request_context.context();
    let boot_disk = path_params.into_inner().boot_disk;
    let status = sa.boot_disk_os_writer().status(boot_disk);
    Ok(HttpResponseOk(status))
}

/// Clear the status of a completed write of a new host OS
#[endpoint {
    method = DELETE,
    path = "/boot-disk/{boot_disk}/os/write/status/{update_id}",
}]
async fn host_os_write_status_delete(
    request_context: RequestContext<SledAgent>,
    path_params: Path<BootDiskUpdatePathParams>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = request_context.context();
    let BootDiskUpdatePathParams { boot_disk, update_id } =
        path_params.into_inner();
    sa.boot_disk_os_writer()
        .clear_terminal_status(boot_disk, update_id)
        .map_err(|err| HttpError::from(&err))?;
    Ok(HttpResponseUpdatedNoContent())
}

/// Fetch basic information about this sled
#[endpoint {
    method = GET,
    path = "/inventory",
}]
async fn inventory(
    request_context: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<Inventory>, HttpError> {
    let sa = request_context.context();
    Ok(HttpResponseOk(sa.inventory().await?))
}

/// Get the internal state of the local bootstore node
#[endpoint {
    method = GET,
    path = "/bootstore/status",
}]
async fn bootstore_status(
    request_context: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<BootstoreStatus>, HttpError> {
    let sa = request_context.context();
    let bootstore = sa.bootstore();
    let status = bootstore
        .get_status()
        .await
        .map_err(|e| {
            HttpError::from(omicron_common::api::external::Error::from(e))
        })?
        .into();
    Ok(HttpResponseOk(status))
}

/// Get the current versions of VPC routing rules.
#[endpoint {
    method = GET,
    path = "/vpc-routes",
}]
async fn list_vpc_routes(
    request_context: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<Vec<ResolvedVpcRouteState>>, HttpError> {
    let sa = request_context.context();
    Ok(HttpResponseOk(sa.list_vpc_routes()))
}

/// Update VPC routing rules.
#[endpoint {
    method = PUT,
    path = "/vpc-routes",
}]
async fn set_vpc_routes(
    request_context: RequestContext<SledAgent>,
    body: TypedBody<Vec<ResolvedVpcRouteSet>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = request_context.context();
    sa.set_vpc_routes(body.into_inner())?;
    Ok(HttpResponseUpdatedNoContent())
}
