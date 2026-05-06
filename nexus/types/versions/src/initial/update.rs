// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Update-related types for version INITIAL.

use chrono::{DateTime, Utc};
use omicron_common::api::external::Nullable;
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

/// Wrapper type for TUF root roles to prevent misuse.
///
/// The format of TUF root roles is an implementation detail and Nexus should
/// generally treat them as opaque JSON blobs without inspecting any fields
/// or, especially, adding any data to them. This value should be created only
/// through Serde deserialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct TufSignedRootRole(serde_json::Value);

impl TufSignedRootRole {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_string().into_bytes()
    }
}

// We'd like to use `#[serde(try_from = ..)]` here but it conflicts with
// `#[serde(transparent)]`, which we're using for the Serialize derive.
impl<'de> Deserialize<'de> for TufSignedRootRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        // Verify that this appears to be a valid, self-signed TUF root role.
        let root = tufaceous::edit::Root::deserialize(&value)
            .map_err(D::Error::custom)?;
        match root.verify_self_signed() {
            Ok(()) => Ok(Self(value)),
            Err(err) => Err(D::Error::custom(format!(
                "Unable to verify root role: {}",
                InlineErrorChain::new(&err)
            ))),
        }
    }
}

/// View of a system software target release
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct TargetRelease {
    /// Time this was set as the target release
    pub time_requested: DateTime<Utc>,

    /// The specified release of the rack's system software
    pub version: Version,
}

/// Trusted root role used by the update system to verify update repositories.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct UpdatesTrustRoot {
    /// The UUID of this trusted root role.
    pub id: Uuid,
    /// Time the trusted root role was added.
    pub time_created: DateTime<Utc>,
    /// The trusted root role itself, a JSON document as described by The Update
    /// Framework.
    pub root_role: TufSignedRootRole,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UpdateStatus {
    /// Current target release of the system software
    ///
    /// This may not correspond to the actual system software running
    /// at the time of request; it is instead the release that the system
    /// should be moving towards as a goal state. The system asynchronously
    /// updates software to match this target release.
    ///
    /// Will only be null if a target release has never been set. In that case,
    /// the system is not automatically attempting to manage software versions.
    pub target_release: Nullable<TargetRelease>,

    /// Count of components running each release version
    ///
    /// Keys will be either:
    ///
    /// * Semver-like release version strings
    /// * "install dataset", representing the initial rack software before
    ///   any updates
    /// * "unknown", which means there is no TUF repo uploaded that matches
    ///   the software running on the component)
    pub components_by_release_version: BTreeMap<String, usize>,

    /// Time of most recent update planning activity
    ///
    /// This is intended as a rough indicator of the last time something
    /// happened in the update planner.
    pub time_last_step_planned: DateTime<Utc>,

    /// Whether automatic update is suspended due to manual update activity
    ///
    /// After a manual support procedure that changes the system software,
    /// automatic update activity is suspended to avoid undoing the change. To
    /// resume automatic update, first upload the TUF repository matching the
    /// manually applied update, then set that as the target release.
    pub suspended: bool,
}

/// Metadata about a TUF repository
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct TufRepo {
    /// The hash of the repository
    // This is a slight abuse of `ArtifactHash`, since that's the hash of
    // individual artifacts within the repository. However, we use it here for
    // convenience.
    pub hash: ArtifactHash,

    /// The system version for this repository
    ///
    /// The system version is a top-level version number applied to all the
    /// software in the repository.
    pub system_version: Version,

    /// The file name of the repository, as reported by the client that uploaded
    /// it
    ///
    /// This is intended for debugging. The file name may not match any
    /// particular pattern, and even if it does, it may not be accurate since
    /// it's just what the client reported.
    // (e.g., with wicket, we read the file contents from stdin so we don't know
    // the correct file name).
    pub file_name: String,

    /// Time the repository was uploaded
    pub time_created: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct TufRepoUpload {
    pub repo: TufRepo,
    pub status: TufRepoUploadStatus,
}

/// Whether the uploaded TUF repo already existed or was new and had to be
/// inserted. Part of `TufRepoUpload`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TufRepoUploadStatus {
    /// The repository already existed in the database
    AlreadyExists,

    /// The repository did not exist, and was inserted into the database
    Inserted,
}

/// Parameters for PUT requests for `/v1/system/update/repositories`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UpdatesPutRepositoryParams {
    /// The name of the uploaded file.
    pub file_name: String,
}

/// Parameters for GET requests for `/v1/system/update/repositories`.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct UpdatesGetRepositoryParams {
    /// The version to get.
    pub system_version: Version,
}

/// Parameters for PUT requests to `/v1/system/update/target-release`.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct SetTargetReleaseParams {
    /// Version of the system software to make the target release.
    pub system_version: Version,
}
