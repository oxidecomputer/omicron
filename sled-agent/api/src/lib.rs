// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use camino::Utf8PathBuf;
use dropshot::{
    Body, FreeformBody, Header, HttpError, HttpResponseAccepted,
    HttpResponseCreated, HttpResponseDeleted, HttpResponseHeaders,
    HttpResponseOk, HttpResponseUpdatedNoContent, Path, Query, RequestContext,
    StreamingBody, TypedBody,
};
use nexus_sled_agent_shared::inventory::{
    Inventory, OmicronSledConfig, SledRole,
};
use omicron_common::{
    api::external::Generation,
    api::internal::{
        nexus::{DiskRuntimeState, SledVmmState},
        shared::{
            ExternalIpGatewayMap, ResolvedVpcRouteSet, ResolvedVpcRouteState,
            SledIdentifiers, SwitchPorts, VirtualNetworkInterfaceHost,
        },
    },
    disk::DiskVariant,
    ledger::Ledgerable,
};
use omicron_uuid_kinds::{
    DatasetUuid, PropolisUuid, SupportBundleUuid, ZpoolUuid,
};
use openapi_manager_types::{
    SupportedVersion, SupportedVersions, api_versions,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::{
    bootstore::BootstoreStatus,
    disk::DiskEnsureBody,
    early_networking::EarlyNetworkConfig,
    firewall_rules::VpcFirewallRulesEnsureBody,
    instance::{
        InstanceEnsureBody, InstanceExternalIpBody, VmmPutStateBody,
        VmmPutStateResponse, VmmUnregisterResponse,
    },
    sled::AddSledRequest,
    zone_bundle::{
        BundleUtilization, CleanupContext, CleanupCount, PriorityOrder,
        ZoneBundleId, ZoneBundleMetadata,
    },
};
use sled_diagnostics::SledDiagnosticsQueryOutput;
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

api_versions!([
    // WHEN CHANGING THE API (part 1 of 2):
    //
    // +- Pick a new semver and define it in the list below.  The list MUST
    // |  remain sorted, which generally means that your version should go at
    // |  the very top.
    // |
    // |  Duplicate this line, uncomment the *second* copy, update that copy for
    // |  your new API version, and leave the first copy commented out as an
    // |  example for the next person.
    // v
    // (next_int, IDENT),
    (3, ADD_SWITCH_ZONE_OPERATOR_POLICY),
    (2, REMOVE_DESTROY_ORPHANED_DATASETS_CHICKEN_SWITCH),
    (1, INITIAL),
]);

// WHEN CHANGING THE API (part 2 of 2):
//
// The call to `api_versions!` above defines constants of type
// `semver::Version` that you can use in your Dropshot API definition to specify
// the version when a particular endpoint was added or removed.  For example, if
// you used:
//
//     (2, ADD_FOOBAR)
//
// Then you could use `VERSION_ADD_FOOBAR` as the version in which endpoints
// were added or removed.

// Host OS images are just over 800 MiB currently; set this to 2 GiB to give
// some breathing room.
const HOST_OS_IMAGE_MAX_BYTES: usize = 2 * 1024 * 1024 * 1024;
// The largest TUF repository artifact is in fact the host OS image. (TODO: or
// at least, it will be when we split up the composite control plane artifact;
// tracked by issue #4411.)
const UPDATE_ARTIFACT_MAX_BYTES: usize = HOST_OS_IMAGE_MAX_BYTES;
// TODO This was the previous API-wide max; what is the largest support bundle
// we expect to need to store?
const SUPPORT_BUNDLE_MAX_BYTES: usize = 2 * 1024 * 1024 * 1024;

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

    /// List all support bundles within a particular dataset
    #[endpoint {
        method = GET,
        path = "/support-bundles/{zpool_id}/{dataset_id}"
    }]
    async fn support_bundle_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundleListPathParam>,
    ) -> Result<HttpResponseOk<Vec<SupportBundleMetadata>>, HttpError>;

    /// Starts creation of a support bundle within a particular dataset
    ///
    /// Callers should transfer chunks of the bundle with
    /// "support_bundle_transfer", and then call "support_bundle_finalize"
    /// once the bundle has finished transferring.
    ///
    /// If a support bundle was previously created without being finalized
    /// successfully, this endpoint will reset the state.
    ///
    /// If a support bundle was previously created and finalized successfully,
    /// this endpoint will return metadata indicating that it already exists.
    #[endpoint {
        method = POST,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}"
    }]
    async fn support_bundle_start_creation(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<HttpResponseCreated<SupportBundleMetadata>, HttpError>;

    /// Transfers a chunk of a support bundle within a particular dataset
    #[endpoint {
        method = PUT,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/transfer",
        request_body_max_bytes = SUPPORT_BUNDLE_MAX_BYTES,
    }]
    async fn support_bundle_transfer(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundlePathParam>,
        query_params: Query<SupportBundleTransferQueryParams>,
        body: StreamingBody,
    ) -> Result<HttpResponseCreated<SupportBundleMetadata>, HttpError>;

    /// Finalizes the creation of a support bundle
    ///
    /// If the requested hash matched the bundle, the bundle is created.
    /// Otherwise, an error is returned.
    #[endpoint {
        method = POST,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/finalize"
    }]
    async fn support_bundle_finalize(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundlePathParam>,
        query_params: Query<SupportBundleFinalizeQueryParams>,
    ) -> Result<HttpResponseCreated<SupportBundleMetadata>, HttpError>;

    /// Fetch a support bundle from a particular dataset
    #[endpoint {
        method = GET,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download"
    }]
    async fn support_bundle_download(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch a file within a support bundle from a particular dataset
    #[endpoint {
        method = GET,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download/{file}"
    }]
    async fn support_bundle_download_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundleFilePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch the index (list of files within a support bundle)
    #[endpoint {
        method = GET,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/index"
    }]
    async fn support_bundle_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch metadata about a support bundle from a particular dataset
    #[endpoint {
        method = HEAD,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download"
    }]
    async fn support_bundle_head(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch metadata about a file within a support bundle from a particular dataset
    #[endpoint {
        method = HEAD,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download/{file}"
    }]
    async fn support_bundle_head_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundleFilePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch metadata about the list of files within a support bundle
    #[endpoint {
        method = HEAD,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/index"
    }]
    async fn support_bundle_head_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Delete a support bundle from a particular dataset
    #[endpoint {
        method = DELETE,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}"
    }]
    async fn support_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/omicron-config",
    }]
    async fn omicron_config_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<OmicronSledConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = GET,
        path = "/sled-role",
    }]
    async fn sled_role_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledRole>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}",
    }]
    async fn vmm_register(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError>;

    #[endpoint {
        method = DELETE,
        path = "/vmms/{propolis_id}",
    }]
    async fn vmm_unregister(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
    ) -> Result<HttpResponseOk<VmmUnregisterResponse>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}/state",
    }]
    async fn vmm_put_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<VmmPutStateBody>,
    ) -> Result<HttpResponseOk<VmmPutStateResponse>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/vmms/{propolis_id}/state",
    }]
    async fn vmm_get_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError>;

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
        method = GET,
        path = "/artifacts-config"
    }]
    async fn artifact_config_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ArtifactConfig>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/artifacts-config"
    }]
    async fn artifact_config_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<ArtifactConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = GET,
        path = "/artifacts"
    }]
    async fn artifact_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ArtifactListResponse>, HttpError>;

    #[endpoint {
        method = POST,
        path = "/artifacts/{sha256}/copy-from-depot"
    }]
    async fn artifact_copy_from_depot(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ArtifactPathParam>,
        query_params: Query<ArtifactQueryParam>,
        body: TypedBody<ArtifactCopyFromDepotBody>,
    ) -> Result<HttpResponseAccepted<ArtifactCopyFromDepotResponse>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/artifacts/{sha256}",
        request_body_max_bytes = UPDATE_ARTIFACT_MAX_BYTES,
    }]
    async fn artifact_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ArtifactPathParam>,
        query_params: Query<ArtifactQueryParam>,
        body: StreamingBody,
    ) -> Result<HttpResponseOk<ArtifactPutResponse>, HttpError>;

    /// Take a snapshot of a disk that is attached to an instance
    #[endpoint {
        method = POST,
        path = "/vmms/{propolis_id}/disks/{disk_id}/snapshot",
    }]
    async fn vmm_issue_disk_snapshot_request(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmIssueDiskSnapshotRequestPathParam>,
        body: TypedBody<VmmIssueDiskSnapshotRequestBody>,
    ) -> Result<HttpResponseOk<VmmIssueDiskSnapshotRequestResponse>, HttpError>;

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

    /// Update per-NIC IP address <-> internet gateway mappings.
    #[endpoint {
        method = PUT,
        path = "/eip-gateways",
    }]
    async fn set_eip_gateways(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<ExternalIpGatewayMap>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = GET,
        path = "/support/zoneadm-info",
    }]
    async fn support_zoneadm_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/support/ipadm-info",
    }]
    async fn support_ipadm_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/support/dladm-info",
    }]
    async fn support_dladm_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/support/nvmeadm-info",
    }]
    async fn support_nvmeadm_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/support/pargs-info",
    }]
    async fn support_pargs_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/support/pstack-info",
    }]
    async fn support_pstack_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/support/pfiles-info",
    }]
    async fn support_pfiles_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/support/zfs-info",
    }]
    async fn support_zfs_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/support/zpool-info",
    }]
    async fn support_zpool_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/support/health-check",
    }]
    async fn support_health_check(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>;

    /// This endpoint returns a list of known zones on a sled that have service
    /// logs that can be collected into a support bundle.
    #[endpoint {
        method = GET,
        path = "/support/logs/zones",
    }]
    async fn support_logs(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<String>>, HttpError>;

    /// This endpoint returns a zip file of a zone's logs organized by service.
    #[endpoint {
        method = GET,
        path = "/support/logs/download/{zone}",
    }]
    async fn support_logs_download(
        request_context: RequestContext<Self::Context>,
        path_params: Path<SledDiagnosticsLogsDownloadPathParm>,
        query_params: Query<SledDiagnosticsLogsDownloadQueryParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// This endpoint reports the status of the `destroy_orphaned_datasets`
    /// chicken switch. It will be removed with omicron#6177.
    #[endpoint {
        method = GET,
        path = "/chicken-switch/destroy-orphaned-datasets",
        versions = ..VERSION_REMOVE_DESTROY_ORPHANED_DATASETS_CHICKEN_SWITCH,
    }]
    async fn chicken_switch_destroy_orphaned_datasets_get(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ChickenSwitchDestroyOrphanedDatasets>, HttpError>;

    /// This endpoint sets the `destroy_orphaned_datasets` chicken switch
    /// (allowing sled-agent to delete datasets it believes are orphaned). It
    /// will be removed with omicron#6177.
    #[endpoint {
        method = PUT,
        path = "/chicken-switch/destroy-orphaned-datasets",
        // This should have been removed in
        // `VERSION_REMOVE_DESTROY_ORPHANED_DATASETS_CHICKEN_SWITCH`, but was
        // overlooked. This removes it as of the next version instead.
        versions = ..VERSION_ADD_SWITCH_ZONE_OPERATOR_POLICY,
    }]
    async fn chicken_switch_destroy_orphaned_datasets_put(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<ChickenSwitchDestroyOrphanedDatasets>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// A debugging endpoint only used by `omdb` that allows us to test
    /// restarting the switch zone without restarting sled-agent. See
    /// https://github.com/oxidecomputer/omicron/issues/8480 for context.
    #[endpoint {
        method = GET,
        path = "/debug/switch-zone-policy",
        versions = VERSION_ADD_SWITCH_ZONE_OPERATOR_POLICY..,
    }]
    async fn debug_operator_switch_zone_policy_get(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<OperatorSwitchZonePolicy>, HttpError>;

    /// A debugging endpoint only used by `omdb` that allows us to test
    /// restarting the switch zone without restarting sled-agent. See
    /// https://github.com/oxidecomputer/omicron/issues/8480 for context.
    #[endpoint {
        method = PUT,
        path = "/debug/switch-zone-policy",
        versions = VERSION_ADD_SWITCH_ZONE_OPERATOR_POLICY..,
    }]
    async fn debug_operator_switch_zone_policy_put(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<OperatorSwitchZonePolicy>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ChickenSwitchDestroyOrphanedDatasets {
    /// If true, sled-agent will attempt to destroy durable ZFS datasets that it
    /// believes were associated with now-expunged Omicron zones.
    pub destroy_orphans: bool,
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

/// Path parameters for Support Bundle requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
pub struct SupportBundleListPathParam {
    /// The zpool on which this support bundle was provisioned
    pub zpool_id: ZpoolUuid,

    /// The dataset on which this support bundle was provisioned
    pub dataset_id: DatasetUuid,
}

/// Path parameters for Support Bundle requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
pub struct SupportBundlePathParam {
    /// The zpool on which this support bundle was provisioned
    pub zpool_id: ZpoolUuid,

    /// The dataset on which this support bundle was provisioned
    pub dataset_id: DatasetUuid,

    /// The ID of the support bundle itself
    pub support_bundle_id: SupportBundleUuid,
}

/// Path parameters for Support Bundle requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
pub struct SupportBundleFilePathParam {
    #[serde(flatten)]
    pub parent: SupportBundlePathParam,

    /// The path of the file within the support bundle to query
    pub file: String,
}

/// Metadata about a support bundle transfer
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct SupportBundleTransferQueryParams {
    pub offset: u64,
}

/// Metadata about a support bundle
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct SupportBundleFinalizeQueryParams {
    pub hash: ArtifactHash,
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub struct SupportBundleGetHeaders {
    range: String,
}

#[derive(Deserialize, Debug, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SupportBundleState {
    Complete,
    Incomplete,
}

/// Metadata about a support bundle
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SupportBundleMetadata {
    pub support_bundle_id: SupportBundleUuid,
    pub state: SupportBundleState,
}

/// Range request headers
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct RangeRequestHeaders {
    /// A request to access a portion of the resource, such as `bytes=0-499`
    ///
    /// See: <https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Range>
    pub range: Option<String>,
}

/// Path parameters for sled-diagnostics log requests used by support bundles
/// (sled agent API)
#[derive(Deserialize, JsonSchema)]
pub struct SledDiagnosticsLogsDownloadPathParm {
    /// The zone for which one would like to collect logs for
    pub zone: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct SledDiagnosticsLogsDownloadQueryParam {
    /// The max number of rotated logs to include in the final support bundle
    pub max_rotated: usize,
}

/// Path parameters for Disk requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
pub struct DiskPathParam {
    pub disk_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct ArtifactConfig {
    pub generation: Generation,
    pub artifacts: BTreeSet<ArtifactHash>,
}

impl Ledgerable for ArtifactConfig {
    fn is_newer_than(&self, other: &ArtifactConfig) -> bool {
        self.generation > other.generation
    }

    // No need to do this, the generation number is provided externally.
    fn generation_bump(&mut self) {}
}

#[derive(Deserialize, JsonSchema)]
pub struct ArtifactPathParam {
    pub sha256: ArtifactHash,
}

#[derive(Deserialize, JsonSchema)]
pub struct ArtifactQueryParam {
    pub generation: Generation,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ArtifactListResponse {
    pub generation: Generation,
    pub list: BTreeMap<ArtifactHash, usize>,
}

#[derive(Deserialize, JsonSchema)]
pub struct ArtifactCopyFromDepotBody {
    pub depot_base_url: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ArtifactCopyFromDepotResponse {}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ArtifactPutResponse {
    /// The number of valid M.2 artifact datasets we found on the sled. There is
    /// typically one of these datasets for each functional M.2.
    pub datasets: usize,

    /// The number of valid writes to the M.2 artifact datasets. This should be
    /// less than or equal to the number of artifact datasets.
    pub successful_writes: usize,
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

/// Policy allowing an operator (via `omdb`) to control whether the switch zone
/// is started or stopped.
///
/// This is an _extremely_ dicey operation in general; a stopped switch zone
/// leaves the rack inoperable! We are only adding this as a workaround and test
/// tool for handling sidecar resets; see
/// https://github.com/oxidecomputer/omicron/issues/8480 for background.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum OperatorSwitchZonePolicy {
    /// Start the switch zone if a switch is present.
    ///
    /// This is the default policy.
    StartIfSwitchPresent,

    /// Even if a switch zone is present, stop the switch zone.
    StopDespiteSwitchPresence,
}
