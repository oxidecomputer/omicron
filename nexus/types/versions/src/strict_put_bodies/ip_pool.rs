// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP pool types for version STRICT_PUT_BODIES.

use crate::v2026_06_23_00::identity::IdentityMetadataUpdateParams;
use omicron_common::api::external::IdentityMetadataUpdateParamsLax;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Updateable properties of an `IpPool`
///
/// A `PUT` replaces the resource, so `name` and `description` must both be
/// present.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// Convert the newer body into the older one (see the note on `ProjectUpdate`'s
// conversion).
impl From<IpPoolUpdate> for crate::v2025_11_20_00::ip_pool::IpPoolUpdate {
    fn from(new: IpPoolUpdate) -> Self {
        Self {
            identity: IdentityMetadataUpdateParamsLax {
                name: Some(new.identity.name),
                description: Some(new.identity.description),
            },
        }
    }
}
