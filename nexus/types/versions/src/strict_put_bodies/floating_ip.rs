// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Floating IP types for version STRICT_PUT_BODIES.

use crate::v2026_06_23_00::identity::IdentityMetadataUpdateParams;
use omicron_common::api::external::IdentityMetadataUpdateParamsLax;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Updateable properties of a `FloatingIp`
///
/// A `PUT` replaces the resource, so `name` and `description` must both be
/// present.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// Convert the newer body into the older one, which is the type the Nexus app
// layer takes. Each required field just becomes a present `Option`. This only
// works in this direction: the older body can leave out a field that the newer
// one requires, so there's no way to convert back.
impl From<FloatingIpUpdate>
    for crate::v2025_11_20_00::floating_ip::FloatingIpUpdate
{
    fn from(new: FloatingIpUpdate) -> Self {
        Self {
            identity: IdentityMetadataUpdateParamsLax {
                name: Some(new.identity.name),
                description: Some(new.identity.description),
            },
        }
    }
}
