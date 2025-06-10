// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use omicron_uuid_kinds::MupdateUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tufaceous_artifact::ArtifactHash;

/// Describes the set of Omicron zones written out into an install dataset.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct OmicronZoneManifest {
    /// The UUID of the mupdate which created this manifest. Intended primarily
    /// for checking equality.
    pub mupdate_id: MupdateUuid,

    /// Omicron zone file names and hashes.
    pub zones: IdOrdMap<OmicronZoneFileMetadata>,
}

impl OmicronZoneManifest {
    /// The name of the file.
    pub const FILE_NAME: &str = "zones.json";
}

/// Information about an Omicron zone file written out to the install dataset.
///
/// Part of [`OmicronZoneManifest`].
#[derive(
    Clone,
    Debug,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct OmicronZoneFileMetadata {
    /// The file name.
    pub file_name: String,

    /// The file size.
    pub file_size: u64,

    /// The hash of the file.
    pub hash: ArtifactHash,
}

impl IdOrdItem for OmicronZoneFileMetadata {
    type Key<'a> = &'a str;

    #[inline]
    fn key(&self) -> Self::Key<'_> {
        &self.file_name
    }

    id_upcast!();
}
