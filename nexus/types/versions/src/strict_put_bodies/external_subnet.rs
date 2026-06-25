// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External subnet types for version STRICT_PUT_BODIES.

use crate::v2026_06_23_00::identity::IdentityMetadataUpdateParams;
use omicron_common::api::external::IdentityMetadataUpdateParamsLax;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Updateable properties of an `ExternalSubnet`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ExternalSubnetUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// Convert the newer body into the older one (see the note on `ProjectUpdate`'s
// conversion).
impl From<ExternalSubnetUpdate>
    for crate::v2026_01_16_01::external_subnet::ExternalSubnetUpdate
{
    fn from(new: ExternalSubnetUpdate) -> Self {
        Self {
            identity: IdentityMetadataUpdateParamsLax {
                name: Some(new.identity.name),
                description: Some(new.identity.description),
            },
        }
    }
}
