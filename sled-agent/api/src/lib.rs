// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use dropshot::{
    Body, FreeformBody, Header, HttpError, HttpResponseAccepted,
    HttpResponseCreated, HttpResponseDeleted, HttpResponseHeaders,
    HttpResponseOk, HttpResponseUpdatedNoContent, Path, Query, RequestContext,
    StreamingBody, TypedBody,
};
use dropshot_api_manager_types::api_versions;
use omicron_common::api::internal::{
    nexus::{DiskRuntimeState, SledVmmState},
    shared::{
        ExternalIpGatewayMap, ResolvedVpcRouteSet, ResolvedVpcRouteState,
        SledIdentifiers, SwitchPorts, VirtualNetworkInterfaceHost,
    },
};
use sled_agent_types_migrations::{v1, v3, v4, v6, v7, v9, v10};
use sled_diagnostics::SledDiagnosticsQueryOutput;

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
    (10, ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES),
    (9, DELEGATE_ZVOL_TO_PROPOLIS),
    (8, REMOVE_SLED_ROLE),
    (7, MULTICAST_SUPPORT),
    (6, ADD_PROBE_PUT_ENDPOINT),
    (5, NEWTYPE_UUID_BUMP),
    (4, ADD_NEXUS_LOCKSTEP_PORT_TO_INVENTORY),
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
        query: Query<v1::params::ZoneBundleFilter>,
    ) -> Result<
        HttpResponseOk<Vec<v1::zone_bundle::ZoneBundleMetadata>>,
        HttpError,
    >;

    /// List the zone bundles that are available for a running zone.
    #[endpoint {
        method = GET,
        path = "/zones/bundles/{zone_name}",
    }]
    async fn zone_bundle_list(
        rqctx: RequestContext<Self::Context>,
        params: Path<v1::params::ZonePathParam>,
    ) -> Result<
        HttpResponseOk<Vec<v1::zone_bundle::ZoneBundleMetadata>>,
        HttpError,
    >;

    /// Fetch the binary content of a single zone bundle.
    #[endpoint {
        method = GET,
        path = "/zones/bundles/{zone_name}/{bundle_id}",
    }]
    async fn zone_bundle_get(
        rqctx: RequestContext<Self::Context>,
        params: Path<v1::zone_bundle::ZoneBundleId>,
    ) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError>;

    /// Delete a zone bundle.
    #[endpoint {
        method = DELETE,
        path = "/zones/bundles/{zone_name}/{bundle_id}",
    }]
    async fn zone_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        params: Path<v1::zone_bundle::ZoneBundleId>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Return utilization information about all zone bundles.
    #[endpoint {
        method = GET,
        path = "/zones/bundle-cleanup/utilization",
    }]
    async fn zone_bundle_utilization(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<
            std::collections::BTreeMap<
                Utf8PathBuf,
                v1::zone_bundle::BundleUtilization,
            >,
        >,
        HttpError,
    >;

    /// Return context used by the zone-bundle cleanup task.
    #[endpoint {
        method = GET,
        path = "/zones/bundle-cleanup/context",
    }]
    async fn zone_bundle_cleanup_context(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::zone_bundle::CleanupContext>, HttpError>;

    /// Update context used by the zone-bundle cleanup task.
    #[endpoint {
        method = PUT,
        path = "/zones/bundle-cleanup/context",
    }]
    async fn zone_bundle_cleanup_context_update(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v1::params::CleanupContextUpdate>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Trigger a zone bundle cleanup.
    #[endpoint {
        method = POST,
        path = "/zones/bundle-cleanup",
    }]
    async fn zone_bundle_cleanup(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<
            std::collections::BTreeMap<
                Utf8PathBuf,
                v1::zone_bundle::CleanupCount,
            >,
        >,
        HttpError,
    >;

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
        path_params: Path<v1::params::SupportBundleListPathParam>,
    ) -> Result<
        HttpResponseOk<Vec<v1::support_bundle::SupportBundleMetadata>>,
        HttpError,
    >;

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
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<
        HttpResponseCreated<v1::support_bundle::SupportBundleMetadata>,
        HttpError,
    >;

    /// Transfers a chunk of a support bundle within a particular dataset
    #[endpoint {
        method = PUT,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/transfer",
        request_body_max_bytes = SUPPORT_BUNDLE_MAX_BYTES,
    }]
    async fn support_bundle_transfer(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::SupportBundlePathParam>,
        query_params: Query<v1::params::SupportBundleTransferQueryParams>,
        body: StreamingBody,
    ) -> Result<
        HttpResponseCreated<v1::support_bundle::SupportBundleMetadata>,
        HttpError,
    >;

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
        path_params: Path<v1::params::SupportBundlePathParam>,
        query_params: Query<v1::params::SupportBundleFinalizeQueryParams>,
    ) -> Result<
        HttpResponseCreated<v1::support_bundle::SupportBundleMetadata>,
        HttpError,
    >;

    /// Fetch a support bundle from a particular dataset
    #[endpoint {
        method = GET,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download"
    }]
    async fn support_bundle_download(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch a file within a support bundle from a particular dataset
    #[endpoint {
        method = GET,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download/{file}"
    }]
    async fn support_bundle_download_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundleFilePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch the index (list of files within a support bundle)
    #[endpoint {
        method = GET,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/index"
    }]
    async fn support_bundle_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch metadata about a support bundle from a particular dataset
    #[endpoint {
        method = HEAD,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download"
    }]
    async fn support_bundle_head(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch metadata about a file within a support bundle from a particular dataset
    #[endpoint {
        method = HEAD,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download/{file}"
    }]
    async fn support_bundle_head_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundleFilePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch metadata about the list of files within a support bundle
    #[endpoint {
        method = HEAD,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/index"
    }]
    async fn support_bundle_head_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Delete a support bundle from a particular dataset
    #[endpoint {
        method = DELETE,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}"
    }]
    async fn support_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/omicron-config",
        versions = VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES..
    }]
    async fn omicron_config_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v10::inventory::OmicronSledConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/omicron-config",
        versions =
            VERSION_ADD_NEXUS_LOCKSTEP_PORT_TO_INVENTORY..VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES,
    }]
    async fn v4_omicron_config_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v4::inventory::OmicronSledConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let body = body.try_map(v10::inventory::OmicronSledConfig::try_from)?;
        Self::omicron_config_put(rqctx, body).await
    }

    #[endpoint {
        operation_id = "omicron_config_put",
        method = PUT,
        path = "/omicron-config",
        versions = ..VERSION_ADD_NEXUS_LOCKSTEP_PORT_TO_INVENTORY,
    }]
    async fn v1_omicron_config_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v1::inventory::OmicronSledConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        Self::v4_omicron_config_put(rqctx, body.map(Into::into)).await
    }

    #[endpoint {
        method = GET,
        path = "/sled-role",
        versions = ..VERSION_REMOVE_SLED_ROLE,
    }]
    async fn sled_role_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::inventory::SledRole>, HttpError>;

    #[endpoint {
        operation_id = "vmm_register",
        method = PUT,
        path = "/vmms/{propolis_id}",
        versions = VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES..
    }]
    async fn vmm_register(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v10::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}",
        operation_id = "vmm_register",
        versions =
            VERSION_DELEGATE_ZVOL_TO_PROPOLIS..VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES
    }]
    async fn v9_vmm_register(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v9::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        let body = body.try_map(v10::instance::InstanceEnsureBody::try_from)?;
        Self::vmm_register(rqctx, path_params, body).await
    }

    #[endpoint {
        operation_id = "vmm_register",
        method = PUT,
        path = "/vmms/{propolis_id}",
        versions = VERSION_MULTICAST_SUPPORT..VERSION_DELEGATE_ZVOL_TO_PROPOLIS
    }]
    async fn v8_vmm_register(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v7::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        Self::v9_vmm_register(rqctx, path_params, body.map(Into::into)).await
    }

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}",
        operation_id = "vmm_register",
        versions = ..VERSION_MULTICAST_SUPPORT
    }]
    async fn v1_vmm_register(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v1::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        Self::v8_vmm_register(rqctx, path_params, body.map(Into::into)).await
    }

    #[endpoint {
        method = DELETE,
        path = "/vmms/{propolis_id}"
    }]
    async fn vmm_unregister(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
    ) -> Result<HttpResponseOk<v1::instance::VmmUnregisterResponse>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}/state",
    }]
    async fn vmm_put_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v1::instance::VmmPutStateBody>,
    ) -> Result<HttpResponseOk<v1::instance::VmmPutStateResponse>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/vmms/{propolis_id}/state",
    }]
    async fn vmm_get_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}/external-ip",
    }]
    async fn vmm_put_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v1::instance::InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = DELETE,
        path = "/vmms/{propolis_id}/external-ip",
    }]
    async fn vmm_delete_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v1::instance::InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}/multicast-group",
        versions = VERSION_MULTICAST_SUPPORT..,
    }]
    async fn vmm_join_multicast_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v7::instance::InstanceMulticastBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = DELETE,
        path = "/vmms/{propolis_id}/multicast-group",
        versions = VERSION_MULTICAST_SUPPORT..,
    }]
    async fn vmm_leave_multicast_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v7::instance::InstanceMulticastBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/disks/{disk_id}",
    }]
    async fn disk_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::DiskPathParam>,
        body: TypedBody<v1::disk::DiskEnsureBody>,
    ) -> Result<HttpResponseOk<DiskRuntimeState>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/artifacts-config"
    }]
    async fn artifact_config_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::artifact::ArtifactConfig>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/artifacts-config"
    }]
    async fn artifact_config_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v1::artifact::ArtifactConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = GET,
        path = "/artifacts"
    }]
    async fn artifact_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::views::ArtifactListResponse>, HttpError>;

    #[endpoint {
        method = POST,
        path = "/artifacts/{sha256}/copy-from-depot"
    }]
    async fn artifact_copy_from_depot(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::ArtifactPathParam>,
        query_params: Query<v1::params::ArtifactQueryParam>,
        body: TypedBody<v1::params::ArtifactCopyFromDepotBody>,
    ) -> Result<
        HttpResponseAccepted<v1::views::ArtifactCopyFromDepotResponse>,
        HttpError,
    >;

    #[endpoint {
        method = PUT,
        path = "/artifacts/{sha256}",
        request_body_max_bytes = UPDATE_ARTIFACT_MAX_BYTES,
    }]
    async fn artifact_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::ArtifactPathParam>,
        query_params: Query<v1::params::ArtifactQueryParam>,
        body: StreamingBody,
    ) -> Result<HttpResponseOk<v1::views::ArtifactPutResponse>, HttpError>;

    /// Take a snapshot of a disk that is attached to an instance
    #[endpoint {
        method = POST,
        path = "/vmms/{propolis_id}/disks/{disk_id}/snapshot",
    }]
    async fn vmm_issue_disk_snapshot_request(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmIssueDiskSnapshotRequestPathParam>,
        body: TypedBody<v1::params::VmmIssueDiskSnapshotRequestBody>,
    ) -> Result<
        HttpResponseOk<v1::views::VmmIssueDiskSnapshotRequestResponse>,
        HttpError,
    >;

    #[endpoint {
        method = PUT,
        path = "/vpc/{vpc_id}/firewall/rules",
        versions = VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES..,
    }]
    async fn vpc_firewall_rules_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VpcPathParam>,
        body: TypedBody<v10::instance::VpcFirewallRulesEnsureBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        operation_id = "vpc_firewall_rules_put",
        method = PUT,
        path = "/vpc/{vpc_id}/firewall/rules",
        versions = ..VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES,
    }]
    async fn v9_vpc_firewall_rules_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VpcPathParam>,
        body: TypedBody<v9::instance::VpcFirewallRulesEnsureBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let body =
            body.try_map(v10::instance::VpcFirewallRulesEnsureBody::try_from)?;
        Self::vpc_firewall_rules_put(rqctx, path_params, body).await
    }

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
    ) -> Result<
        HttpResponseOk<v1::early_networking::EarlyNetworkConfig>,
        HttpError,
    >;

    #[endpoint {
        method = PUT,
        path = "/network-bootstore-config",
    }]
    async fn write_network_bootstore_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v1::early_networking::EarlyNetworkConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Add a sled to a rack that was already initialized via RSS
    #[endpoint {
        method = PUT,
        path = "/sleds"
    }]
    async fn sled_add(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v1::sled::AddSledRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Fetch basic information about this sled
    #[endpoint {
        method = GET,
        path = "/inventory",
        versions = VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES..,
    }]
    async fn inventory(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v10::inventory::Inventory>, HttpError>;

    /// Fetch basic information about this sled
    #[endpoint {
        operation_id = "inventory",
        method = GET,
        path = "/inventory",
        versions =
            VERSION_ADD_NEXUS_LOCKSTEP_PORT_TO_INVENTORY..VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES,
    }]
    async fn v4_inventory(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v4::inventory::Inventory>, HttpError> {
        let HttpResponseOk(inventory) = Self::inventory(rqctx).await?;
        inventory.try_into().map_err(HttpError::from).map(HttpResponseOk)
    }

    /// Fetch basic information about this sled
    #[endpoint {
        operation_id = "inventory",
        method = GET,
        path = "/inventory",
        versions = ..VERSION_ADD_NEXUS_LOCKSTEP_PORT_TO_INVENTORY,
    }]
    async fn v1_inventory(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::inventory::Inventory>, HttpError> {
        Self::v4_inventory(rqctx).await.map(|HttpResponseOk(inv)| {
            HttpResponseOk(v1::inventory::Inventory::from(inv))
        })
    }

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
    ) -> Result<HttpResponseOk<v1::bootstore::BootstoreStatus>, HttpError>;

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
        path_params: Path<v1::params::SledDiagnosticsLogsDownloadPathParam>,
        query_params: Query<v1::params::SledDiagnosticsLogsDownloadQueryParam>,
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
    ) -> Result<
        HttpResponseOk<v1::shared::ChickenSwitchDestroyOrphanedDatasets>,
        HttpError,
    >;

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
        body: TypedBody<v1::shared::ChickenSwitchDestroyOrphanedDatasets>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// A debugging endpoint only used by `omdb` that allows us to test
    /// restarting the switch zone without restarting sled-agent. See
    /// <https://github.com/oxidecomputer/omicron/issues/8480> for context.
    #[endpoint {
        method = GET,
        path = "/debug/switch-zone-policy",
        versions = VERSION_ADD_SWITCH_ZONE_OPERATOR_POLICY..,
    }]
    async fn debug_operator_switch_zone_policy_get(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v3::shared::OperatorSwitchZonePolicy>, HttpError>;

    /// A debugging endpoint only used by `omdb` that allows us to test
    /// restarting the switch zone without restarting sled-agent. See
    /// <https://github.com/oxidecomputer/omicron/issues/8480> for context.
    ///
    /// Setting the switch zone policy is asynchronous and inherently racy with
    /// the standard process of starting the switch zone. If the switch zone is
    /// in the process of being started or stopped when this policy is changed,
    /// the new policy may not take effect until that transition completes.
    #[endpoint {
        method = PUT,
        path = "/debug/switch-zone-policy",
        versions = VERSION_ADD_SWITCH_ZONE_OPERATOR_POLICY..,
    }]
    async fn debug_operator_switch_zone_policy_put(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<v3::shared::OperatorSwitchZonePolicy>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Update the entire set of probe zones on this sled.
    ///
    /// Probe zones are used to debug networking configuration. They look
    /// similar to instances, in that they have an OPTE port on a VPC subnet and
    /// external addresses, but no actual VM.
    #[endpoint {
        method = PUT,
        path = "/probes",
        versions = VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES..,
    }]
    async fn probes_put(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<v10::probes::ProbeSet>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Update the entire set of probe zones on this sled.
    ///
    /// Probe zones are used to debug networking configuration. They look
    /// similar to instances, in that they have an OPTE port on a VPC subnet and
    /// external addresses, but no actual VM.
    #[endpoint {
        method = PUT,
        path = "/probes",
        versions =
            VERSION_ADD_PROBE_PUT_ENDPOINT..VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES,
    }]
    async fn v6_probes_put(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<v6::probes::ProbeSet>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let body = body.try_map(v10::probes::ProbeSet::try_from)?;
        Self::probes_put(request_context, body).await
    }

    /// Create a local storage dataset
    #[endpoint {
        method = POST,
        path = "/local-storage/{zpool_id}/{dataset_id}",
        versions = VERSION_DELEGATE_ZVOL_TO_PROPOLIS..,
    }]
    async fn local_storage_dataset_ensure(
        request_context: RequestContext<Self::Context>,
        path_params: Path<v9::params::LocalStoragePathParam>,
        body: TypedBody<v9::params::LocalStorageDatasetEnsureRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Delete a local storage dataset
    #[endpoint {
        method = DELETE,
        path = "/local-storage/{zpool_id}/{dataset_id}",
        versions = VERSION_DELEGATE_ZVOL_TO_PROPOLIS..,
    }]
    async fn local_storage_dataset_delete(
        request_context: RequestContext<Self::Context>,
        path_params: Path<v9::params::LocalStoragePathParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;
}
