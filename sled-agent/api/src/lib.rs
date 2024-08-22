// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{collections::BTreeMap, time::Duration};

use camino::Utf8PathBuf;
use dropshot::{
    FreeformBody, HttpError, HttpResponseCreated, HttpResponseDeleted,
    HttpResponseHeaders, HttpResponseOk, HttpResponseUpdatedNoContent, Path,
    Query, RequestContext, StreamingBody, TypedBody,
};
use nexus_sled_agent_shared::inventory::{
    Inventory, OmicronZonesConfig, SledRole,
};
use omicron_common::{
    api::internal::{
        nexus::{DiskRuntimeState, SledInstanceState, UpdateArtifactId},
        shared::{
            ResolvedVpcRouteSet, ResolvedVpcRouteState, SledIdentifiers,
            SwitchPorts, VirtualNetworkInterfaceHost,
        },
    },
    disk::{DiskVariant, DisksManagementResult, OmicronPhysicalDisksConfig},
};
use omicron_uuid_kinds::{PropolisUuid, ZpoolUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::{
    boot_disk::{
        BootDiskOsWriteStatus, BootDiskPathParams, BootDiskUpdatePathParams,
        BootDiskWriteStartQueryParams,
    },
    bootstore::BootstoreStatus,
    disk::DiskEnsureBody,
    early_networking::EarlyNetworkConfig,
    firewall_rules::VpcFirewallRulesEnsureBody,
    instance::{
        InstanceEnsureBody, InstanceExternalIpBody, InstancePutStateBody,
        InstancePutStateResponse, InstanceUnregisterResponse,
    },
    sled::AddSledRequest,
    time_sync::TimeSync,
    zone_bundle::{
        BundleUtilization, CleanupContext, CleanupCount, PriorityOrder,
        ZoneBundleId, ZoneBundleMetadata,
    },
};
use uuid::Uuid;

#[dropshot::api_description]
pub trait SledAgentApi {
    type Context;

    /// List all zone bundles that exist, even for now-deleted zones.
    #[endpoint {
        method = GET,
        path = "/zones/bundles",
    }]
    async fn zone_bundle_list_all(
        rqctx: RequestContext<Self::Context>,
        query: Query<ZoneBundleFilter>,
    ) -> Result<HttpResponseOk<Vec<ZoneBundleMetadata>>, HttpError>;

    /// List the zone bundles that are available for a running zone.
    #[endpoint {
        method = GET,
        path = "/zones/bundles/{zone_name}",
    }]
    async fn zone_bundle_list(
        rqctx: RequestContext<Self::Context>,
        params: Path<ZonePathParam>,
    ) -> Result<HttpResponseOk<Vec<ZoneBundleMetadata>>, HttpError>;

    /// Ask the sled agent to create a zone bundle.
    #[endpoint {
        method = POST,
        path = "/zones/bundles/{zone_name}",
    }]
    async fn zone_bundle_create(
        rqctx: RequestContext<Self::Context>,
        params: Path<ZonePathParam>,
    ) -> Result<HttpResponseCreated<ZoneBundleMetadata>, HttpError>;

    /// Fetch the binary content of a single zone bundle.
    #[endpoint {
        method = GET,
        path = "/zones/bundles/{zone_name}/{bundle_id}",
    }]
    async fn zone_bundle_get(
        rqctx: RequestContext<Self::Context>,
        params: Path<ZoneBundleId>,
    ) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError>;

    /// Delete a zone bundle.
    #[endpoint {
        method = DELETE,
        path = "/zones/bundles/{zone_name}/{bundle_id}",
    }]
    async fn zone_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        params: Path<ZoneBundleId>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Return utilization information about all zone bundles.
    #[endpoint {
        method = GET,
        path = "/zones/bundle-cleanup/utilization",
    }]
    async fn zone_bundle_utilization(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<BTreeMap<Utf8PathBuf, BundleUtilization>>,
        HttpError,
    >;

    /// Return context used by the zone-bundle cleanup task.
    #[endpoint {
        method = GET,
        path = "/zones/bundle-cleanup/context",
    }]
    async fn zone_bundle_cleanup_context(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<CleanupContext>, HttpError>;

    /// Update context used by the zone-bundle cleanup task.
    #[endpoint {
        method = PUT,
        path = "/zones/bundle-cleanup/context",
    }]
    async fn zone_bundle_cleanup_context_update(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<CleanupContextUpdate>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Trigger a zone bundle cleanup.
    #[endpoint {
        method = POST,
        path = "/zones/bundle-cleanup",
    }]
    async fn zone_bundle_cleanup(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BTreeMap<Utf8PathBuf, CleanupCount>>, HttpError>;

    /// List the zones that are currently managed by the sled agent.
    #[endpoint {
        method = GET,
        path = "/zones",
    }]
    async fn zones_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<String>>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/omicron-zones",
    }]
    async fn omicron_zones_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<OmicronZonesConfig>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/omicron-zones",
    }]
    async fn omicron_zones_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<OmicronZonesConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = GET,
        path = "/omicron-physical-disks",
    }]
    async fn omicron_physical_disks_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<OmicronPhysicalDisksConfig>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/omicron-physical-disks",
    }]
    async fn omicron_physical_disks_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<OmicronPhysicalDisksConfig>,
    ) -> Result<HttpResponseOk<DisksManagementResult>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/zpools",
    }]
    async fn zpools_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<Zpool>>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/sled-role",
    }]
    async fn sled_role_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledRole>, HttpError>;

    /// Initializes a CockroachDB cluster
    #[endpoint {
        method = POST,
        path = "/cockroachdb",
    }]
    async fn cockroachdb_init(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}",
    }]
    async fn vmm_register(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledInstanceState>, HttpError>;

    #[endpoint {
        method = DELETE,
        path = "/vmms/{propolis_id}",
    }]
    async fn vmm_unregister(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
    ) -> Result<HttpResponseOk<InstanceUnregisterResponse>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}/state",
    }]
    async fn vmm_put_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<InstancePutStateBody>,
    ) -> Result<HttpResponseOk<InstancePutStateResponse>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/vmms/{propolis_id}/state",
    }]
    async fn vmm_get_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
    ) -> Result<HttpResponseOk<SledInstanceState>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}/external-ip",
    }]
    async fn vmm_put_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = DELETE,
        path = "/vmms/{propolis_id}/external-ip",
    }]
    async fn vmm_delete_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/disks/{disk_id}",
    }]
    async fn disk_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<DiskPathParam>,
        body: TypedBody<DiskEnsureBody>,
    ) -> Result<HttpResponseOk<DiskRuntimeState>, HttpError>;

    #[endpoint {
        method = POST,
        path = "/update"
    }]
    async fn update_artifact(
        rqctx: RequestContext<Self::Context>,
        artifact: TypedBody<UpdateArtifactId>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Take a snapshot of a disk that is attached to an instance
    #[endpoint {
        method = POST,
        path = "/vmms/{propolis_id}/disks/{disk_id}/snapshot",
    }]
    async fn vmm_issue_disk_snapshot_request(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmIssueDiskSnapshotRequestPathParam>,
        body: TypedBody<VmmIssueDiskSnapshotRequestBody>,
    ) -> Result<
        HttpResponseOk<VmmIssueDiskSnapshotRequestResponse>,
        HttpError,
    >;

    #[endpoint {
        method = PUT,
        path = "/vpc/{vpc_id}/firewall/rules",
    }]
    async fn vpc_firewall_rules_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VpcPathParam>,
        body: TypedBody<VpcFirewallRulesEnsureBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Create a mapping from a virtual NIC to a physical host
    // Keep interface_id to maintain parity with the simulated sled agent, which
    // requires interface_id on the path.
    #[endpoint {
        method = PUT,
        path = "/v2p/",
    }]
    async fn set_v2p(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<VirtualNetworkInterfaceHost>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Delete a mapping from a virtual NIC to a physical host
    // Keep interface_id to maintain parity with the simulated sled agent, which
    // requires interface_id on the path.
    #[endpoint {
        method = DELETE,
        path = "/v2p/",
    }]
    async fn del_v2p(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<VirtualNetworkInterfaceHost>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// List v2p mappings present on sled
    // Used by nexus background task
    #[endpoint {
        method = GET,
        path = "/v2p/",
    }]
    async fn list_v2p(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<VirtualNetworkInterfaceHost>>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/timesync",
    }]
    async fn timesync_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<TimeSync>, HttpError>;

    #[endpoint {
        method = POST,
        path = "/switch-ports",
    }]
    async fn uplink_ensure(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<SwitchPorts>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// This API endpoint is only reading the local sled agent's view of the
    /// bootstore. The boostore is a distributed data store that is eventually
    /// consistent. Reads from individual nodes may not represent the latest state.
    #[endpoint {
        method = GET,
        path = "/network-bootstore-config",
    }]
    async fn read_network_bootstore_config_cache(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<EarlyNetworkConfig>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/network-bootstore-config",
    }]
    async fn write_network_bootstore_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<EarlyNetworkConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Add a sled to a rack that was already initialized via RSS
    #[endpoint {
        method = PUT,
        path = "/sleds"
    }]
    async fn sled_add(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<AddSledRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Write a new host OS image to the specified boot disk
    #[endpoint {
        method = POST,
        path = "/boot-disk/{boot_disk}/os/write",
    }]
    async fn host_os_write_start(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<BootDiskPathParams>,
        query_params: Query<BootDiskWriteStartQueryParams>,
        body: StreamingBody,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = GET,
        path = "/boot-disk/{boot_disk}/os/write/status",
    }]
    async fn host_os_write_status_get(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<BootDiskPathParams>,
    ) -> Result<HttpResponseOk<BootDiskOsWriteStatus>, HttpError>;

    /// Clear the status of a completed write of a new host OS
    #[endpoint {
        method = DELETE,
        path = "/boot-disk/{boot_disk}/os/write/status/{update_id}",
    }]
    async fn host_os_write_status_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<BootDiskUpdatePathParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Fetch basic information about this sled
    #[endpoint {
        method = GET,
        path = "/inventory",
    }]
    async fn inventory(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Inventory>, HttpError>;

    /// Fetch sled identifiers
    #[endpoint {
        method = GET,
        path = "/sled-identifiers",
    }]
    async fn sled_identifiers(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledIdentifiers>, HttpError>;

    /// Get the internal state of the local bootstore node
    #[endpoint {
        method = GET,
        path = "/bootstore/status",
    }]
    async fn bootstore_status(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BootstoreStatus>, HttpError>;

    /// Get the current versions of VPC routing rules.
    #[endpoint {
        method = GET,
        path = "/vpc-routes",
    }]
    async fn list_vpc_routes(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<ResolvedVpcRouteState>>, HttpError>;

    /// Update VPC routing rules.
    #[endpoint {
        method = PUT,
        path = "/vpc-routes",
    }]
    async fn set_vpc_routes(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<Vec<ResolvedVpcRouteSet>>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ZoneBundleFilter {
    /// An optional substring used to filter zone bundles.
    pub filter: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ZonePathParam {
    /// The name of the zone.
    pub zone_name: String,
}

/// Parameters used to update the zone bundle cleanup context.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CleanupContextUpdate {
    /// The new period on which automatic cleanups are run.
    pub period: Option<Duration>,
    /// The priority ordering for preserving old zone bundles.
    pub priority: Option<PriorityOrder>,
    /// The new limit on the underlying dataset quota allowed for bundles.
    pub storage_limit: Option<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Zpool {
    pub id: ZpoolUuid,
    pub disk_type: DiskType,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum DiskType {
    U2,
    M2,
}

impl From<DiskVariant> for DiskType {
    fn from(v: DiskVariant) -> Self {
        match v {
            DiskVariant::U2 => Self::U2,
            DiskVariant::M2 => Self::M2,
        }
    }
}

/// Path parameters for Instance requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
pub struct VmmPathParam {
    pub propolis_id: PropolisUuid,
}

/// Path parameters for Disk requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
pub struct DiskPathParam {
    pub disk_id: Uuid,
}

#[derive(Deserialize, JsonSchema)]
pub struct VmmIssueDiskSnapshotRequestPathParam {
    pub propolis_id: PropolisUuid,
    pub disk_id: Uuid,
}

#[derive(Deserialize, JsonSchema)]
pub struct VmmIssueDiskSnapshotRequestBody {
    pub snapshot_id: Uuid,
}

#[derive(Serialize, JsonSchema)]
pub struct VmmIssueDiskSnapshotRequestResponse {
    pub snapshot_id: Uuid,
}

/// Path parameters for VPC requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
pub struct VpcPathParam {
    pub vpc_id: Uuid,
}
