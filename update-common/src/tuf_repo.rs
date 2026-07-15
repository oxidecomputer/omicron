// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use omicron_common::update::ArtifactId;
use schemars::JsonSchema;
use semver::Version;
use serde::Deserialize;
use serde::Serialize;
use tufaceous_artifact::ArtifactHash;

/// A description of an uploaded TUF repository.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct TufRepoDescription {
    /// Information about the repository.
    pub repo: TufRepoMeta,

    /// Information about the artifacts present in the repository.
    pub artifacts: Vec<TufArtifactMeta>,
}

impl TufRepoDescription {
    /// Sorts the artifacts so that descriptions can be compared.
    pub fn sort_artifacts(&mut self) {
        self.artifacts.sort_by(|a, b| a.id.cmp(&b.id));
    }
}

/// Metadata about a TUF repository.
///
/// Found within a `TufRepoDescription`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct TufRepoMeta {
    /// The hash of the repository.
    ///
    /// This is a slight abuse of `ArtifactHash`, since that's the hash of
    /// individual artifacts within the repository. However, we use it here for
    /// convenience.
    pub hash: ArtifactHash,

    /// The version of the targets role.
    pub targets_role_version: u64,

    /// The time until which the repo is valid.
    pub valid_until: DateTime<Utc>,

    /// The system version in artifacts.json.
    pub system_version: Version,

    /// The file name of the repository.
    ///
    /// This is purely used for debugging and may not always be correct (e.g.
    /// with wicket, we read the file contents from stdin so we don't know the
    /// correct file name).
    pub file_name: String,
}

/// Metadata about an individual TUF artifact.
///
/// Found within a `TufRepoDescription`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct TufArtifactMeta {
    /// The artifact ID.
    pub id: ArtifactId,

    /// The hash of the artifact.
    pub hash: ArtifactHash,

    /// The size of the artifact in bytes.
    pub size: u64,

    /// Contents of the `BORD` field of a Hubris archive caboose. Only
    /// applicable to artifacts that are Hubris archives.
    ///
    /// This field should always be `Some(_)` if `sign` is `Some(_)`, but the
    /// opposite is not true (SP images will have a `board` but not a `sign`).
    pub board: Option<String>,

    /// Contents of the `SIGN` field of a Hubris archive caboose, i.e.,
    /// an identifier for the set of valid signing keys. Currently only
    /// applicable to RoT image and bootloader artifacts, where it will
    /// be an LPC55 Root Key Table Hash (RKTH).
    pub sign: Option<Vec<u8>>,
}
