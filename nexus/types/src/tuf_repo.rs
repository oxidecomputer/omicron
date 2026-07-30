// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use semver::Version;
use serde::Deserialize;
use serde::Serialize;
use tufaceous_artifact_v2::ArtifactHash;
use tufaceous_artifact_v2::ArtifactSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TufRepoDescription {
    /// The artifacts described in the repository.
    pub artifacts: ArtifactSet,

    /// The metadata of the repository.
    pub metadata: BTreeMap<String, String>,

    /// The system version in `artifacts-v2.json`.
    pub system_version: Version,

    /// The hash of the repository.
    ///
    /// This is a slight abuse of `ArtifactHash`, since that's the hash of
    /// individual artifacts within the repository. However, we use it here for
    /// convenience.
    pub hash: ArtifactHash,

    /// The file name of the repository.
    ///
    /// This is purely used for debugging and may not always be correct (e.g.
    /// with wicket, we read the file contents from stdin so we don't know the
    /// correct file name).
    pub file_name: String,
}
