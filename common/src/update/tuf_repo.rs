// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use semver::Version;
use serde::Deserialize;
use serde::Serialize;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::Artifacts;

/// A description of an uploaded TUF repository.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct TufRepoDescription {
    /// The artifacts described in the repository.
    pub artifacts: Artifacts,

    /// The system version in `artifacts-v2.json`.
    pub system_version: Version,

    /// The hash of the repository.
    ///
    /// This is a slight abuse of `ArtifactHash`, since that's the hash of
    /// individual artifacts within the repository. However, we use it here for
    /// convenience.
    pub hash: Option<ArtifactHash>,

    /// The file name of the repository.
    ///
    /// This is purely used for debugging and may not always be present or
    /// correct. (In prior versions of Omicron this string was generated if
    /// not present.)
    pub file_name: Option<String>,
}
