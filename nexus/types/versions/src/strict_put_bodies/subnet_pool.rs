// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subnet pool types for version STRICT_PUT_BODIES.

use crate::v2026_06_23_00::identity::IdentityMetadataUpdateParams;
use omicron_common::api::external::IdentityMetadataUpdateParamsLax;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Updateable properties of a `SubnetPool`
///
/// A `PUT` replaces the resource, so `name` and `description` must both be
/// present.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// Convert the newer body into the older one (see the note on `ProjectUpdate`'s
// conversion).
impl From<SubnetPoolUpdate>
    for crate::v2026_01_16_01::subnet_pool::SubnetPoolUpdate
{
    fn from(new: SubnetPoolUpdate) -> Self {
        Self {
            identity: IdentityMetadataUpdateParamsLax {
                name: Some(new.identity.name),
                description: Some(new.identity.description),
            },
        }
    }
}
