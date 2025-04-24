// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Type definitions for the MUPdate override (RFD 556).

use omicron_uuid_kinds::MupdateOverrideUuid;
use serde::{Deserialize, Serialize};

/// A MUPdate override JSON schema (RFD 556).
///
/// When a MUPdate occurs, a file containing this information is created on the
/// install dataset of the system.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MupdateOverrideJson {
    /// A UUID that identifies a MUPdate that occurred.
    pub mupdate_uuid: MupdateOverrideUuid,
}

impl MupdateOverrideJson {
    /// The name of the file on the install dataset.
    pub const FILE_NAME: &'static str = "mupdate-override.json";
}
