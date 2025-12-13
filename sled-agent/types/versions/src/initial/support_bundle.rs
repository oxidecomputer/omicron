// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle types for Sled Agent API v1.

use omicron_uuid_kinds::{DatasetUuid, SupportBundleUuid, ZpoolUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tufaceous_artifact::ArtifactHash;

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

/// Range request headers.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct RangeRequestHeaders {
    /// A request to access a portion of the resource, such as `bytes=0-499`
    ///
    /// See: <https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Range>
    pub range: Option<String>,
}

/// State of a support bundle.
#[derive(Deserialize, Debug, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SupportBundleState {
    Complete,
    Incomplete,
}

/// Metadata about a support bundle.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SupportBundleMetadata {
    pub support_bundle_id: SupportBundleUuid,
    pub state: SupportBundleState,
}
