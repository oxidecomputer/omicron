// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

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
        SledIdentifiers, VirtualNetworkInterfaceHost,
    },
};
use sled_agent_types_versions::{
    latest, v1, v4, v6, v7, v9, v10, v11, v12, v14, v16, v17, v22,
};
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
    (23, ADD_ZPOOL_HEALTH_TO_INVENTORY),
    (22, REMOVE_HEALTH_MONITOR_KEEP_CHECKS),
    (21, REMOVE_DISK_PUT),
    (20, BGP_V6),
    (19, ADD_ROT_ATTESTATION),
    (18, ADD_ATTACHED_SUBNETS),
    (17, TWO_TYPES_OF_DELEGATED_ZVOL),
    (16, MEASUREMENT_PROPER_INVENTORY),
    (15, ADD_TRUST_QUORUM_STATUS),
    (14, MEASUREMENTS),
    (13, ADD_TRUST_QUORUM),
    (12, ADD_SMF_SERVICES_HEALTH_CHECK),
    (11, ADD_DUAL_STACK_EXTERNAL_IP_CONFIG),
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
        query: Query<latest::zone_bundle::ZoneBundleFilter>,
    ) -> Result<
        HttpResponseOk<Vec<latest::zone_bundle::ZoneBundleMetadata>>,
        HttpError,
    >;

    /// List the zone bundles that are available for a running zone.
    #[endpoint {
        method = GET,
        path = "/zones/bundles/{zone_name}",
    }]
    async fn zone_bundle_list(
        rqctx: RequestContext<Self::Context>,
        params: Path<latest::zone_bundle::ZonePathParam>,
    ) -> Result<
        HttpResponseOk<Vec<latest::zone_bundle::ZoneBundleMetadata>>,
        HttpError,
    >;

    /// Fetch the binary content of a single zone bundle.
    #[endpoint {
        method = GET,
        path = "/zones/bundles/{zone_name}/{bundle_id}",
    }]
    async fn zone_bundle_get(
        rqctx: RequestContext<Self::Context>,
        params: Path<latest::zone_bundle::ZoneBundleId>,
    ) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError>;

    /// Delete a zone bundle.
    #[endpoint {
        method = DELETE,
        path = "/zones/bundles/{zone_name}/{bundle_id}",
    }]
    async fn zone_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        params: Path<latest::zone_bundle::ZoneBundleId>,
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
            BTreeMap<Utf8PathBuf, latest::zone_bundle::BundleUtilization>,
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
    ) -> Result<HttpResponseOk<latest::zone_bundle::CleanupContext>, HttpError>;

    /// Update context used by the zone-bundle cleanup task.
    #[endpoint {
        method = PUT,
        path = "/zones/bundle-cleanup/context",
    }]
    async fn zone_bundle_cleanup_context_update(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::zone_bundle::CleanupContextUpdate>,
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
                latest::zone_bundle::CleanupCount,
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
        path_params: Path<latest::support_bundle::SupportBundleListPathParam>,
    ) -> Result<
        HttpResponseOk<Vec<latest::support_bundle::SupportBundleMetadata>>,
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
        path_params: Path<latest::support_bundle::SupportBundlePathParam>,
    ) -> Result<
        HttpResponseCreated<latest::support_bundle::SupportBundleMetadata>,
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
        path_params: Path<latest::support_bundle::SupportBundlePathParam>,
        query_params: Query<
            latest::support_bundle::SupportBundleTransferQueryParams,
        >,
        body: StreamingBody,
    ) -> Result<
        HttpResponseCreated<latest::support_bundle::SupportBundleMetadata>,
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
        path_params: Path<latest::support_bundle::SupportBundlePathParam>,
        query_params: Query<
            latest::support_bundle::SupportBundleFinalizeQueryParams,
        >,
    ) -> Result<
        HttpResponseCreated<latest::support_bundle::SupportBundleMetadata>,
        HttpError,
    >;

    /// Fetch a support bundle from a particular dataset
    #[endpoint {
        method = GET,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download"
    }]
    async fn support_bundle_download(
        rqctx: RequestContext<Self::Context>,
        headers: Header<latest::support_bundle::RangeRequestHeaders>,
        path_params: Path<latest::support_bundle::SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch a file within a support bundle from a particular dataset
    #[endpoint {
        method = GET,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download/{file}"
    }]
    async fn support_bundle_download_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<latest::support_bundle::RangeRequestHeaders>,
        path_params: Path<latest::support_bundle::SupportBundleFilePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch the index (list of files within a support bundle)
    #[endpoint {
        method = GET,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/index"
    }]
    async fn support_bundle_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<latest::support_bundle::RangeRequestHeaders>,
        path_params: Path<latest::support_bundle::SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch metadata about a support bundle from a particular dataset
    #[endpoint {
        method = HEAD,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download"
    }]
    async fn support_bundle_head(
        rqctx: RequestContext<Self::Context>,
        headers: Header<latest::support_bundle::RangeRequestHeaders>,
        path_params: Path<latest::support_bundle::SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch metadata about a file within a support bundle from a particular dataset
    #[endpoint {
        method = HEAD,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/download/{file}"
    }]
    async fn support_bundle_head_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<latest::support_bundle::RangeRequestHeaders>,
        path_params: Path<latest::support_bundle::SupportBundleFilePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Fetch metadata about the list of files within a support bundle
    #[endpoint {
        method = HEAD,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}/index"
    }]
    async fn support_bundle_head_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<latest::support_bundle::RangeRequestHeaders>,
        path_params: Path<latest::support_bundle::SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError>;

    /// Delete a support bundle from a particular dataset
    #[endpoint {
        method = DELETE,
        path = "/support-bundles/{zpool_id}/{dataset_id}/{support_bundle_id}"
    }]
    async fn support_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::support_bundle::SupportBundlePathParam>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/omicron-config",
        versions = VERSION_MEASUREMENTS..,
    }]
    async fn omicron_config_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::inventory::OmicronSledConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        operation_id = "omicron_config_put",
        method = PUT,
        path = "/omicron-config",
        versions =
            VERSION_ADD_DUAL_STACK_EXTERNAL_IP_CONFIG..VERSION_MEASUREMENTS,
    }]
    async fn omicron_config_put_v11(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v11::inventory::OmicronSledConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let body =
            body.try_map(latest::inventory::OmicronSledConfig::try_from)?;
        Self::omicron_config_put(rqctx, body).await
    }

    #[endpoint {
        operation_id = "omicron_config_put",
        method = PUT,
        path = "/omicron-config",
        versions =
            VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES..VERSION_ADD_DUAL_STACK_EXTERNAL_IP_CONFIG,
    }]
    async fn omicron_config_put_v10(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v10::inventory::OmicronSledConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let body = body.try_map(v11::inventory::OmicronSledConfig::try_from)?;
        Self::omicron_config_put_v11(rqctx, body).await
    }

    #[endpoint {
        operation_id = "omicron_config_put",
        method = PUT,
        path = "/omicron-config",
        versions =
            VERSION_ADD_NEXUS_LOCKSTEP_PORT_TO_INVENTORY..VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES,
    }]
    async fn omicron_config_put_v4(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v4::inventory::OmicronSledConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let body = body.try_map(v10::inventory::OmicronSledConfig::try_from)?;
        Self::omicron_config_put_v10(rqctx, body).await
    }

    #[endpoint {
        operation_id = "omicron_config_put",
        method = PUT,
        path = "/omicron-config",
        versions = ..VERSION_ADD_NEXUS_LOCKSTEP_PORT_TO_INVENTORY,
    }]
    async fn omicron_config_put_v1(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v1::inventory::OmicronSledConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        Self::omicron_config_put_v4(rqctx, body.map(Into::into)).await
    }

    #[endpoint {
        operation_id = "sled_role_get",
        method = GET,
        path = "/sled-role",
        versions = ..VERSION_REMOVE_SLED_ROLE,
    }]
    async fn sled_role_get_v1(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::inventory::SledRole>, HttpError>;

    #[endpoint {
        operation_id = "vmm_register",
        method = PUT,
        path = "/vmms/{propolis_id}",
        versions = VERSION_ADD_ATTACHED_SUBNETS..
    }]
    async fn vmm_register(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
        body: TypedBody<latest::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError>;

    #[endpoint {
        operation_id = "vmm_register",
        method = PUT,
        path = "/vmms/{propolis_id}",
        versions =
            VERSION_TWO_TYPES_OF_DELEGATED_ZVOL..VERSION_ADD_ATTACHED_SUBNETS
    }]
    async fn vmm_register_v17(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
        body: TypedBody<v17::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        Self::vmm_register(rqctx, path_params, body.map(Into::into)).await
    }

    #[endpoint {
        operation_id = "vmm_register",
        method = PUT,
        path = "/vmms/{propolis_id}",
        versions = VERSION_ADD_DUAL_STACK_EXTERNAL_IP_CONFIG..VERSION_TWO_TYPES_OF_DELEGATED_ZVOL
    }]
    async fn vmm_register_v11(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
        body: TypedBody<v11::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        Self::vmm_register_v17(rqctx, path_params, body.map(Into::into)).await
    }

    #[endpoint {
        operation_id = "vmm_register",
        method = PUT,
        path = "/vmms/{propolis_id}",
        versions =
            VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES..VERSION_ADD_DUAL_STACK_EXTERNAL_IP_CONFIG
    }]
    async fn vmm_register_v10(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::instance::VmmPathParam>,
        body: TypedBody<v10::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        let body = body.try_map(v11::instance::InstanceEnsureBody::try_from)?;
        Self::vmm_register_v11(rqctx, path_params, body).await
    }

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}",
        operation_id = "vmm_register",
        versions =
            VERSION_DELEGATE_ZVOL_TO_PROPOLIS..VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES
    }]
    async fn vmm_register_v9(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::instance::VmmPathParam>,
        body: TypedBody<v9::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        let body = body.try_map(v10::instance::InstanceEnsureBody::try_from)?;
        Self::vmm_register_v10(rqctx, path_params, body).await
    }

    #[endpoint {
        operation_id = "vmm_register",
        method = PUT,
        path = "/vmms/{propolis_id}",
        versions = VERSION_MULTICAST_SUPPORT..VERSION_DELEGATE_ZVOL_TO_PROPOLIS
    }]
    async fn vmm_register_v7(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::instance::VmmPathParam>,
        body: TypedBody<v7::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        Self::vmm_register_v9(rqctx, path_params, body.map(Into::into)).await
    }

    #[endpoint {
        operation_id = "vmm_register",
        method = PUT,
        path = "/vmms/{propolis_id}",
        versions = ..VERSION_MULTICAST_SUPPORT
    }]
    async fn vmm_register_v1(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::instance::VmmPathParam>,
        body: TypedBody<v1::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        Self::vmm_register_v7(rqctx, path_params, body.map(Into::into)).await
    }

    #[endpoint {
        method = DELETE,
        path = "/vmms/{propolis_id}"
    }]
    async fn vmm_unregister(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
    ) -> Result<
        HttpResponseOk<latest::instance::VmmUnregisterResponse>,
        HttpError,
    >;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}/state",
    }]
    async fn vmm_put_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
        body: TypedBody<latest::instance::VmmPutStateBody>,
    ) -> Result<HttpResponseOk<latest::instance::VmmPutStateResponse>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/vmms/{propolis_id}/state",
    }]
    async fn vmm_get_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}/external-ip",
    }]
    async fn vmm_put_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
        body: TypedBody<latest::instance::InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = DELETE,
        path = "/vmms/{propolis_id}/external-ip",
    }]
    async fn vmm_delete_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
        body: TypedBody<latest::instance::InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}/multicast-group",
        versions = VERSION_MULTICAST_SUPPORT..,
    }]
    async fn vmm_join_multicast_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
        body: TypedBody<latest::instance::InstanceMulticastBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = DELETE,
        path = "/vmms/{propolis_id}/multicast-group",
        versions = VERSION_MULTICAST_SUPPORT..,
    }]
    async fn vmm_leave_multicast_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
        body: TypedBody<latest::instance::InstanceMulticastBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/disks/{disk_id}",
        versions = ..VERSION_REMOVE_DISK_PUT,
    }]
    async fn disk_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::disk::DiskPathParam>,
        body: TypedBody<latest::disk::DiskEnsureBody>,
    ) -> Result<HttpResponseOk<DiskRuntimeState>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/artifacts-config"
    }]
    async fn artifact_config_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::artifact::ArtifactConfig>, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/artifacts-config"
    }]
    async fn artifact_config_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::artifact::ArtifactConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = GET,
        path = "/artifacts"
    }]
    async fn artifact_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::artifact::ArtifactListResponse>, HttpError>;

    #[endpoint {
        method = POST,
        path = "/artifacts/{sha256}/copy-from-depot"
    }]
    async fn artifact_copy_from_depot(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::artifact::ArtifactPathParam>,
        query_params: Query<latest::artifact::ArtifactQueryParam>,
        body: TypedBody<latest::artifact::ArtifactCopyFromDepotBody>,
    ) -> Result<
        HttpResponseAccepted<latest::artifact::ArtifactCopyFromDepotResponse>,
        HttpError,
    >;

    #[endpoint {
        method = PUT,
        path = "/artifacts/{sha256}",
        request_body_max_bytes = UPDATE_ARTIFACT_MAX_BYTES,
    }]
    async fn artifact_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::artifact::ArtifactPathParam>,
        query_params: Query<latest::artifact::ArtifactQueryParam>,
        body: StreamingBody,
    ) -> Result<HttpResponseOk<latest::artifact::ArtifactPutResponse>, HttpError>;

    /// Take a snapshot of a disk that is attached to an instance
    #[endpoint {
        method = POST,
        path = "/vmms/{propolis_id}/disks/{disk_id}/snapshot",
    }]
    async fn vmm_issue_disk_snapshot_request(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<
            latest::instance::VmmIssueDiskSnapshotRequestPathParam,
        >,
        body: TypedBody<latest::instance::VmmIssueDiskSnapshotRequestBody>,
    ) -> Result<
        HttpResponseOk<latest::instance::VmmIssueDiskSnapshotRequestResponse>,
        HttpError,
    >;

    #[endpoint {
        method = PUT,
        path = "/vpc/{vpc_id}/firewall/rules",
        versions = VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES..,
    }]
    async fn vpc_firewall_rules_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VpcPathParam>,
        body: TypedBody<latest::firewall_rules::VpcFirewallRulesEnsureBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        operation_id = "vpc_firewall_rules_put",
        method = PUT,
        path = "/vpc/{vpc_id}/firewall/rules",
        versions = ..VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES,
    }]
    async fn vpc_firewall_rules_put_v1(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::instance::VpcPathParam>,
        body: TypedBody<v9::firewall_rules::VpcFirewallRulesEnsureBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let body = body.try_map(
            latest::firewall_rules::VpcFirewallRulesEnsureBody::try_from,
        )?;
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
        versions = VERSION_BGP_V6..,
    }]
    async fn uplink_ensure(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::uplink::SwitchPorts>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = POST,
        path = "/switch-ports",
        versions = ..VERSION_BGP_V6,
    }]
    async fn uplink_ensure_v1(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v1::uplink::SwitchPorts>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        Self::uplink_ensure(rqctx, body.map(From::from)).await
    }

    /// This API endpoint is only reading the local sled agent's view of the
    /// bootstore. The boostore is a distributed data store that is eventually
    /// consistent. Reads from individual nodes may not represent the latest state.
    #[endpoint {
        method = GET,
        path = "/network-bootstore-config",
        versions = VERSION_BGP_V6..,
    }]
    async fn read_network_bootstore_config_cache(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<latest::early_networking::EarlyNetworkConfig>,
        HttpError,
    >;

    /// This API endpoint is only reading the local sled agent's view of the
    /// bootstore. The boostore is a distributed data store that is eventually
    /// consistent. Reads from individual nodes may not represent the latest state.
    #[endpoint {
        method = GET,
        path = "/network-bootstore-config",
        versions = ..VERSION_BGP_V6,
    }]
    async fn read_network_bootstore_config_cache_v1(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<v1::early_networking::EarlyNetworkConfig>,
        HttpError,
    > {
        let result: v1::early_networking::EarlyNetworkConfig =
            Self::read_network_bootstore_config_cache(rqctx)
                .await?
                .0
                .try_into()
                .map_err(|e| {
                    HttpError::for_bad_request(
                        None,
                        format!("error getting v1 config: {e}"),
                    )
                })?;

        Ok(HttpResponseOk(result))
    }

    #[endpoint {
        method = PUT,
        path = "/network-bootstore-config",
        versions = VERSION_BGP_V6..,
    }]
    async fn write_network_bootstore_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::early_networking::EarlyNetworkConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/network-bootstore-config",
        versions = ..VERSION_BGP_V6,
    }]
    async fn write_network_bootstore_config_v1(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v1::early_networking::EarlyNetworkConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        Self::write_network_bootstore_config(rqctx, body.map(|x| x.into()))
            .await
    }

    /// Add a sled to a rack that was already initialized via RSS
    #[endpoint {
        method = PUT,
        path = "/sleds"
    }]
    async fn sled_add(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::sled::AddSledRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Fetch basic information about this sled
    #[endpoint {
        method = GET,
        path = "/inventory",
        versions = VERSION_ADD_ZPOOL_HEALTH_TO_INVENTORY..,
    }]
    async fn inventory(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::inventory::Inventory>, HttpError>;

    /// Fetch basic information about this sled
    #[endpoint {
        operation_id = "inventory",
        method = GET,
        path = "/inventory",
        versions = VERSION_REMOVE_HEALTH_MONITOR_KEEP_CHECKS..VERSION_ADD_ZPOOL_HEALTH_TO_INVENTORY,
    }]
    async fn inventory_v22(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v22::inventory::Inventory>, HttpError> {
        Self::inventory(rqctx).await.map(|HttpResponseOk(inv)| {
            HttpResponseOk(v22::inventory::Inventory::from(inv))
        })
    }

    /// Fetch basic information about this sled
    #[endpoint {
        operation_id = "inventory",
        method = GET,
        path = "/inventory",
        versions = VERSION_MEASUREMENT_PROPER_INVENTORY..VERSION_REMOVE_HEALTH_MONITOR_KEEP_CHECKS,
    }]
    async fn inventory_v16(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v16::inventory::Inventory>, HttpError> {
        Self::inventory_v22(rqctx).await.map(|HttpResponseOk(inv)| {
            HttpResponseOk(v16::inventory::Inventory::from(inv))
        })
    }

    /// Fetch basic information about this sled
    #[endpoint {
        operation_id = "inventory",
        method = GET,
        path = "/inventory",
        versions = VERSION_MEASUREMENTS..VERSION_MEASUREMENT_PROPER_INVENTORY,
    }]
    async fn inventory_v14(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v14::inventory::Inventory>, HttpError> {
        let HttpResponseOk(inventory) = Self::inventory_v16(rqctx).await?;
        inventory.try_into().map_err(HttpError::from).map(HttpResponseOk)
    }

    /// Fetch basic information about this sled
    #[endpoint {
        operation_id = "inventory",
        method = GET,
        path = "/inventory",
        versions = VERSION_ADD_SMF_SERVICES_HEALTH_CHECK..VERSION_MEASUREMENTS,
    }]
    async fn inventory_v12(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v12::inventory::Inventory>, HttpError> {
        let HttpResponseOk(inventory) = Self::inventory_v14(rqctx).await?;
        inventory.try_into().map_err(HttpError::from).map(HttpResponseOk)
    }

    /// Fetch basic information about this sled
    #[endpoint {
        operation_id = "inventory",
        method = GET,
        path = "/inventory",
        versions = VERSION_ADD_DUAL_STACK_EXTERNAL_IP_CONFIG..VERSION_ADD_SMF_SERVICES_HEALTH_CHECK,
    }]
    async fn inventory_v11(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v11::inventory::Inventory>, HttpError> {
        Self::inventory_v12(rqctx).await.map(|HttpResponseOk(inv)| {
            HttpResponseOk(v11::inventory::Inventory::from(inv))
        })
    }

    /// Fetch basic information about this sled
    #[endpoint {
        operation_id = "inventory",
        method = GET,
        path = "/inventory",
        versions =
            VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES..VERSION_ADD_DUAL_STACK_EXTERNAL_IP_CONFIG,
    }]
    async fn inventory_v10(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v10::inventory::Inventory>, HttpError> {
        let HttpResponseOk(inventory) = Self::inventory_v11(rqctx).await?;
        inventory.try_into().map_err(HttpError::from).map(HttpResponseOk)
    }

    /// Fetch basic information about this sled
    #[endpoint {
        operation_id = "inventory",
        method = GET,
        path = "/inventory",
        versions =
            VERSION_ADD_NEXUS_LOCKSTEP_PORT_TO_INVENTORY..VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES,
    }]
    async fn inventory_v4(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v4::inventory::Inventory>, HttpError> {
        let HttpResponseOk(inventory) = Self::inventory_v10(rqctx).await?;
        inventory.try_into().map_err(HttpError::from).map(HttpResponseOk)
    }

    /// Fetch basic information about this sled
    #[endpoint {
        operation_id = "inventory",
        method = GET,
        path = "/inventory",
        versions = ..VERSION_ADD_NEXUS_LOCKSTEP_PORT_TO_INVENTORY,
    }]
    async fn inventory_v1(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::inventory::Inventory>, HttpError> {
        Self::inventory_v4(rqctx).await.map(|HttpResponseOk(inv)| {
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
    ) -> Result<HttpResponseOk<latest::bootstore::BootstoreStatus>, HttpError>;

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
        path_params: Path<
            latest::diagnostics::SledDiagnosticsLogsDownloadPathParam,
        >,
        query_params: Query<
            latest::diagnostics::SledDiagnosticsLogsDownloadQueryParam,
        >,
    ) -> Result<http::Response<Body>, HttpError>;

    /// This endpoint reports the status of the `destroy_orphaned_datasets`
    /// chicken switch. It will be removed with omicron#6177.
    #[endpoint {
        operation_id = "chicken_switch_destroy_orphaned_datasets_get",
        method = GET,
        path = "/chicken-switch/destroy-orphaned-datasets",
        versions = ..VERSION_REMOVE_DESTROY_ORPHANED_DATASETS_CHICKEN_SWITCH,
    }]
    async fn chicken_switch_destroy_orphaned_datasets_get_v1(
        request_context: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<v1::debug::ChickenSwitchDestroyOrphanedDatasets>,
        HttpError,
    >;

    /// This endpoint sets the `destroy_orphaned_datasets` chicken switch
    /// (allowing sled-agent to delete datasets it believes are orphaned). It
    /// will be removed with omicron#6177.
    #[endpoint {
        operation_id = "chicken_switch_destroy_orphaned_datasets_put",
        method = PUT,
        path = "/chicken-switch/destroy-orphaned-datasets",
        // This should have been removed in
        // `VERSION_REMOVE_DESTROY_ORPHANED_DATASETS_CHICKEN_SWITCH`, but was
        // overlooked. This removes it as of the next version instead.
        versions = ..VERSION_ADD_SWITCH_ZONE_OPERATOR_POLICY,
    }]
    async fn chicken_switch_destroy_orphaned_datasets_put_v1(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<v1::debug::ChickenSwitchDestroyOrphanedDatasets>,
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
    ) -> Result<
        HttpResponseOk<latest::debug::OperatorSwitchZonePolicy>,
        HttpError,
    >;

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
        body: TypedBody<latest::debug::OperatorSwitchZonePolicy>,
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
        body: TypedBody<latest::probes::ProbeSet>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Update the entire set of probe zones on this sled.
    ///
    /// Probe zones are used to debug networking configuration. They look
    /// similar to instances, in that they have an OPTE port on a VPC subnet and
    /// external addresses, but no actual VM.
    #[endpoint {
        operation_id = "probes_put",
        method = PUT,
        path = "/probes",
        versions =
            VERSION_ADD_PROBE_PUT_ENDPOINT..VERSION_ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES,
    }]
    async fn probes_put_v6(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<v6::probes::ProbeSet>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let body = body.try_map(latest::probes::ProbeSet::try_from)?;
        Self::probes_put(request_context, body).await
    }

    /// Create a local storage dataset
    #[endpoint {
        operation_id = "local_storage_dataset_ensure",
        method = POST,
        path = "/local-storage",
        versions = VERSION_TWO_TYPES_OF_DELEGATED_ZVOL..,
    }]
    async fn local_storage_dataset_ensure(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<latest::dataset::LocalStorageDatasetEnsureRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Create a local storage dataset
    #[endpoint {
        operation_id = "local_storage_dataset_ensure",
        method = POST,
        path = "/local-storage/{zpool_id}/{dataset_id}",
        versions = VERSION_DELEGATE_ZVOL_TO_PROPOLIS..VERSION_TWO_TYPES_OF_DELEGATED_ZVOL,
    }]
    async fn local_storage_dataset_ensure_v9(
        request_context: RequestContext<Self::Context>,
        path_params: Path<v9::dataset::LocalStoragePathParam>,
        body: TypedBody<v9::dataset::LocalStorageDatasetEnsureRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let path_params = path_params.into_inner();
        let body = body.into_inner();

        Self::local_storage_dataset_ensure(
            request_context,
            latest::dataset::LocalStorageDatasetEnsureRequest::from(
                path_params.zpool_id,
                path_params.dataset_id,
                body,
            )
            .into(),
        )
        .await
    }

    /// Delete a local storage dataset
    #[endpoint {
        operation_id = "local_storage_dataset_delete",
        method = DELETE,
        path = "/local-storage",
        versions = VERSION_TWO_TYPES_OF_DELEGATED_ZVOL..,
    }]
    async fn local_storage_dataset_delete(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<latest::dataset::LocalStorageDatasetDeleteRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Delete a local storage dataset
    #[endpoint {
        operation_id = "local_storage_dataset_delete",
        method = DELETE,
        path = "/local-storage/{zpool_id}/{dataset_id}",
        versions = VERSION_DELEGATE_ZVOL_TO_PROPOLIS..VERSION_TWO_TYPES_OF_DELEGATED_ZVOL,
    }]
    async fn local_storage_dataset_delete_v9(
        request_context: RequestContext<Self::Context>,
        path_params: Path<v9::dataset::LocalStoragePathParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let path_params = path_params.into_inner();

        Self::local_storage_dataset_delete(
            request_context,
            latest::dataset::LocalStorageDatasetDeleteRequest {
                zpool_id: path_params.zpool_id,
                dataset_id: path_params.dataset_id,
                // This version of the API assumed it would be using the
                // encrypted dataset.
                encrypted_at_rest: true,
            }
            .into(),
        )
        .await
    }

    /// Initiate a trust quorum reconfiguration
    #[endpoint {
        method = POST,
        path = "/trust-quorum/configuration",
        versions = VERSION_ADD_TRUST_QUORUM..,
    }]
    async fn trust_quorum_reconfigure(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<trust_quorum_types::messages::ReconfigureMsg>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Initiate an upgrade from LRTQ
    #[endpoint {
        method = POST,
        path = "/trust-quorum/upgrade",
        versions = VERSION_ADD_TRUST_QUORUM..,
    }]
    async fn trust_quorum_upgrade_from_lrtq(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<trust_quorum_types::messages::LrtqUpgradeMsg>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Commit a trust quorum configuration
    #[endpoint {
        method = PUT,
        path = "/trust-quorum/commit",
        versions = VERSION_ADD_TRUST_QUORUM..,
    }]
    async fn trust_quorum_commit(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<trust_quorum_types::messages::CommitRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Get the coordinator status if this node is coordinating a reconfiguration
    #[endpoint {
        method = GET,
        path = "/trust-quorum/coordinator-status",
        versions = VERSION_ADD_TRUST_QUORUM..,
    }]
    async fn trust_quorum_coordinator_status(
        request_context: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<Option<trust_quorum_types::status::CoordinatorStatus>>,
        HttpError,
    >;

    /// Attempt to prepare and commit a trust quorum configuration
    #[endpoint {
        method = PUT,
        path = "/trust-quorum/prepare-and-commit",
        versions = VERSION_ADD_TRUST_QUORUM..,
    }]
    async fn trust_quorum_prepare_and_commit(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<trust_quorum_types::messages::PrepareAndCommitRequest>,
    ) -> Result<
        HttpResponseOk<trust_quorum_types::status::CommitStatus>,
        HttpError,
    >;

    /// Proxy a commit operation to another trust quorum node
    #[endpoint {
        method = PUT,
        path = "/trust-quorum/proxy/commit",
        versions = VERSION_ADD_TRUST_QUORUM..,
    }]
    async fn trust_quorum_proxy_commit(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<latest::trust_quorum::ProxyCommitRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Proxy a prepare-and-commit operation to another trust quorum node
    #[endpoint {
        method = PUT,
        path = "/trust-quorum/proxy/prepare-and-commit",
        versions = VERSION_ADD_TRUST_QUORUM..,
    }]
    async fn trust_quorum_proxy_prepare_and_commit(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<latest::trust_quorum::ProxyPrepareAndCommitRequest>,
    ) -> Result<
        HttpResponseOk<trust_quorum_types::status::CommitStatus>,
        HttpError,
    >;

    /// Proxy a status request to another trust quorum node
    #[endpoint {
        method = GET,
        path = "/trust-quorum/proxy/status",
        versions = VERSION_ADD_TRUST_QUORUM..,
    }]
    async fn trust_quorum_proxy_status(
        request_context: RequestContext<Self::Context>,
        query_params: Query<sled_hardware_types::BaseboardId>,
    ) -> Result<HttpResponseOk<trust_quorum_types::status::NodeStatus>, HttpError>;

    /// Get the status of this trust quorum node
    #[endpoint {
        method = GET,
        path = "/trust-quorum/status",
        versions = VERSION_ADD_TRUST_QUORUM_STATUS..,
    }]
    async fn trust_quorum_status(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<trust_quorum_types::status::NodeStatus>, HttpError>;

    /// Get the current network config from trust quorum
    #[endpoint {
        method = GET,
        path = "/trust-quorum/network-config",
        versions = VERSION_ADD_TRUST_QUORUM_STATUS..,
    }]
    async fn trust_quorum_network_config_get(
        request_context: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<Option<latest::trust_quorum::TrustQuorumNetworkConfig>>,
        HttpError,
    >;

    /// Update the network config in trust quorum
    #[endpoint {
        method = PUT,
        path = "/trust-quorum/network-config",
        versions = VERSION_ADD_TRUST_QUORUM_STATUS..,
    }]
    async fn trust_quorum_network_config_put(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<latest::trust_quorum::TrustQuorumNetworkConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Update the subnets attached to an instance.
    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}/attached-subnets",
        versions = VERSION_ADD_ATTACHED_SUBNETS..,
    }]
    async fn vmm_put_attached_subnets(
        request_context: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
        body: TypedBody<latest::attached_subnet::AttachedSubnets>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Delete all subnets attached to an instance.
    #[endpoint {
        method = DELETE,
        path = "/vmms/{propolis_id}/attached-subnets",
        versions = VERSION_ADD_ATTACHED_SUBNETS..,
    }]
    async fn vmm_delete_attached_subnets(
        request_context: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Attach a subnet to an instance.
    #[endpoint {
        method = POST,
        path = "/vmms/{propolis_id}/attached-subnets",
        versions = VERSION_ADD_ATTACHED_SUBNETS..,
    }]
    async fn vmm_post_attached_subnet(
        request_context: RequestContext<Self::Context>,
        path_params: Path<latest::instance::VmmPathParam>,
        body: TypedBody<latest::attached_subnet::AttachedSubnet>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Detach a subnet from an instance.
    #[endpoint {
        method = DELETE,
        path = "/vmms/{propolis_id}/attached-subnets/{subnet}",
        versions = VERSION_ADD_ATTACHED_SUBNETS..,
    }]
    async fn vmm_delete_attached_subnet(
        request_context: RequestContext<Self::Context>,
        path_params: Path<latest::attached_subnet::VmmSubnetPathParam>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Return the set of measurments recorded by the RoT.
    #[endpoint {
        method = GET,
        path = "/rot/{rot}/measurement-log",
        versions = VERSION_ADD_ROT_ATTESTATION..,
    }]
    async fn rot_measurement_log(
        request_context: RequestContext<Self::Context>,
        path_params: Path<latest::rot::RotPathParams>,
    ) -> Result<HttpResponseOk<latest::rot::MeasurementLog>, HttpError>;

    /// Return the certificate chain for the attestation signer from the RoT.
    #[endpoint {
        method = GET,
        path = "/rot/{rot}/certificate-chain",
        versions = VERSION_ADD_ROT_ATTESTATION..,
    }]
    async fn rot_certificate_chain(
        request_context: RequestContext<Self::Context>,
        path_params: Path<latest::rot::RotPathParams>,
    ) -> Result<HttpResponseOk<latest::rot::CertificateChain>, HttpError>;

    /// Return attestation over recorded measurments and nonce from the RoT.
    #[endpoint {
        method = POST,
        path = "/rot/{rot}/attest",
        versions = VERSION_ADD_ROT_ATTESTATION..,
    }]
    async fn rot_attest(
        request_context: RequestContext<Self::Context>,
        path_params: Path<latest::rot::RotPathParams>,
        body: TypedBody<latest::rot::Nonce>,
    ) -> Result<HttpResponseOk<latest::rot::Attestation>, HttpError>;
}
