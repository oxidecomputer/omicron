// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Type definitions for the MUPdate override (RFD 556).

use std::collections::BTreeSet;

use omicron_uuid_kinds::MupdateOverrideUuid;
use serde::{Deserialize, Serialize};
use tufaceous_artifact::ArtifactHashId;

/// MUPdate override information, typically serialized as JSON (RFD 556).
///
/// When a MUPdate occurs, a file containing this information is created on the
/// install dataset of the system.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct MupdateOverrideInfo {
    /// A UUID that identifies a MUPdate that occurred.
    pub mupdate_uuid: MupdateOverrideUuid,

    /// Artifact hashes written out to the install dataset.
    pub hash_ids: BTreeSet<ArtifactHashId>,
}

impl MupdateOverrideInfo {
    /// The name of the file on the install dataset.
    pub const FILE_NAME: &'static str = "mupdate-override.json";
}
