// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request parameters for Sled Agent API v1.
//!
//! This module contains path parameters, query parameters, header parameters,
//! and request body types used by the Sled Agent API.
//!
//! Per RFD 619, high-level request types (params) are defined in the earliest
//! version they appear in. These types are used directly by the API crate with
//! fixed identifiers.

use std::time::Duration;

use omicron_common::api::external::Generation;
use omicron_uuid_kinds::{
    DatasetUuid, PropolisUuid, SupportBundleUuid, ZpoolUuid,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

use super::zone_bundle::PriorityOrder;

/// Path parameters for zone requests.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ZonePathParam {
    /// The name of the zone.
    pub zone_name: String,
}

/// Path parameters for VMM requests.
#[derive(Deserialize, JsonSchema)]
pub struct VmmPathParam {
    pub propolis_id: PropolisUuid,
}

/// Path parameters for Support Bundle list requests.
#[derive(Deserialize, JsonSchema)]
pub struct SupportBundleListPathParam {
    /// The zpool on which this support bundle was provisioned
    pub zpool_id: ZpoolUuid,

    /// The dataset on which this support bundle was provisioned
    pub dataset_id: DatasetUuid,
}

/// Path parameters for Support Bundle requests.
#[derive(Deserialize, JsonSchema)]
pub struct SupportBundlePathParam {
    /// The zpool on which this support bundle was provisioned
    pub zpool_id: ZpoolUuid,

    /// The dataset on which this support bundle was provisioned
    pub dataset_id: DatasetUuid,

    /// The ID of the support bundle itself
    pub support_bundle_id: SupportBundleUuid,
}

/// Path parameters for Support Bundle file requests.
#[derive(Deserialize, JsonSchema)]
pub struct SupportBundleFilePathParam {
    #[serde(flatten)]
    pub parent: SupportBundlePathParam,

    /// The path of the file within the support bundle to query
    pub file: String,
}

/// Path parameters for Disk requests.
#[derive(Deserialize, JsonSchema)]
pub struct DiskPathParam {
    pub disk_id: Uuid,
}

/// Path parameters for Artifact requests.
#[derive(Deserialize, JsonSchema)]
pub struct ArtifactPathParam {
    pub sha256: ArtifactHash,
}

/// Path parameters for VMM disk snapshot requests.
#[derive(Deserialize, JsonSchema)]
pub struct VmmIssueDiskSnapshotRequestPathParam {
    pub propolis_id: PropolisUuid,
    pub disk_id: Uuid,
}

/// Path parameters for VPC requests.
#[derive(Deserialize, JsonSchema)]
pub struct VpcPathParam {
    pub vpc_id: Uuid,
}

/// Path parameters for sled-diagnostics log requests used by support bundles.
// NOTE: The original type had a typo (Parm vs Param). We keep both for compat.
#[derive(Deserialize, JsonSchema)]
pub struct SledDiagnosticsLogsDownloadPathParam {
    /// The zone for which one would like to collect logs for
    pub zone: String,
}

/// Type alias for backward compatibility with the original typo.
pub type SledDiagnosticsLogsDownloadPathParm =
    SledDiagnosticsLogsDownloadPathParam;

/// Query parameters for zone bundle list filtering.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ZoneBundleFilter {
    /// An optional substring used to filter zone bundles.
    pub filter: Option<String>,
}

/// Query parameters for support bundle transfer.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct SupportBundleTransferQueryParams {
    pub offset: u64,
}

/// Query parameters for support bundle finalization.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct SupportBundleFinalizeQueryParams {
    pub hash: ArtifactHash,
}

/// Query parameters for artifact requests.
#[derive(Deserialize, JsonSchema)]
pub struct ArtifactQueryParam {
    pub generation: Generation,
}

/// Query parameters for sled-diagnostics log download requests.
#[derive(Deserialize, JsonSchema)]
pub struct SledDiagnosticsLogsDownloadQueryParam {
    /// The max number of rotated logs to include in the final support bundle
    pub max_rotated: usize,
}

/// Range request headers.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct RangeRequestHeaders {
    /// A request to access a portion of the resource, such as `bytes=0-499`
    ///
    /// See: <https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Range>
    pub range: Option<String>,
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

/// Request body for copying artifacts from a depot.
#[derive(Deserialize, JsonSchema)]
pub struct ArtifactCopyFromDepotBody {
    pub depot_base_url: String,
}

/// Request body for VMM disk snapshot requests.
#[derive(Deserialize, JsonSchema)]
pub struct VmmIssueDiskSnapshotRequestBody {
    pub snapshot_id: Uuid,
}
