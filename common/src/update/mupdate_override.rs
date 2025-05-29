// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Type definitions for the MUPdate override (RFD 556).

use std::collections::BTreeSet;

use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use omicron_uuid_kinds::MupdateOverrideUuid;
use serde::{Deserialize, Serialize};
use tufaceous_artifact::{ArtifactHash, ArtifactHashId};

/// MUPdate override information, typically serialized as JSON (RFD 556).
///
/// When a MUPdate occurs, a file containing this information is created on the
/// install dataset of the system.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct MupdateOverrideInfo {
    /// A UUID that identifies a MUPdate that occurred.
    pub mupdate_uuid: MupdateOverrideUuid,

    /// Artifact hashes written out to the install dataset.
    ///
    /// Currently includes the host phase 2 and composite control plane
    /// artifacts. Information about individual zones is included in
    /// [`Self::zones`].
    pub hash_ids: BTreeSet<ArtifactHashId>,

    /// Control plane zone file names and hashes.
    pub zones: IdOrdMap<MupdateOverrideZone>,
}

impl MupdateOverrideInfo {
    /// The name of the file on the install dataset.
    pub const FILE_NAME: &'static str = "mupdate-override.json";
}

/// Control plane zone information written out to the install dataset.
///
/// Part of [`MupdateOverrideInfo`].
#[derive(
    Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize,
)]
pub struct MupdateOverrideZone {
    /// The file name.
    pub file_name: String,

    /// The file size.
    pub file_size: u64,

    /// The hash of the file.
    pub hash: ArtifactHash,
}

impl IdOrdItem for MupdateOverrideZone {
    type Key<'a> = &'a str;

    #[inline]
    fn key(&self) -> Self::Key<'_> {
        &self.file_name
    }

    id_upcast!();
}
