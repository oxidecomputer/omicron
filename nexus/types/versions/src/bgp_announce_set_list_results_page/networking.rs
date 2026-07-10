// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00;
use api_identity::ObjectIdentity;
use omicron_common::api::external::{IdentityMetadata, ObjectIdentity};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Represents a BGP announce set by id. The id can be used with other API calls
/// to view and manage the announce set.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq,
)]
pub struct BgpAnnounceSet {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

impl From<v2025_11_20_00::networking::BgpAnnounceSet> for BgpAnnounceSet {
    fn from(old: v2025_11_20_00::networking::BgpAnnounceSet) -> Self {
        Self { identity: old.identity }
    }
}

impl From<BgpAnnounceSet> for v2025_11_20_00::networking::BgpAnnounceSet {
    fn from(new: BgpAnnounceSet) -> Self {
        Self { identity: new.identity }
    }
}
