// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use nexus_types::external_api::views;
use nexus_types::external_api::views::TufRepoUploadStatus;
use schemars::JsonSchema;
use semver::Version;
use serde::Deserialize;
use serde::Serialize;
use tufaceous_artifact::ArtifactHash;

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

impl From<views::TufRepo> for TufRepo {
    fn from(repo: nexus_types::external_api::views::TufRepo) -> Self {
        Self {
            hash: repo.hash.unwrap_or(ArtifactHash([0; 32])),
            system_version: repo.system_version,
            file_name: repo.file_name.unwrap_or_default(),
            time_created: repo.time_created,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct TufRepoUpload {
    pub repo: TufRepo,
    pub status: TufRepoUploadStatus,
}

impl From<views::TufRepoUpload> for TufRepoUpload {
    fn from(info: nexus_types::external_api::views::TufRepoUpload) -> Self {
        Self { repo: info.repo.into(), status: info.status }
    }
}
