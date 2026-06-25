// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Project types for version STRICT_PUT_BODIES.

use crate::v2026_06_23_00::identity::IdentityMetadataUpdateParamsStrict;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Updateable properties of a `Project`
///
/// A `PUT` replaces the resource, so every field is required: `name` and
/// `description` must both be present.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParamsStrict,
}

// Convert the newer body into the older one, which is the type the Nexus app
// layer takes. Each required field just becomes a present `Option`. This only
// works in this direction: the older body can leave out a field that the newer
// one requires, so there's no way to convert back.
impl From<ProjectUpdate> for crate::v2025_11_20_00::project::ProjectUpdate {
    fn from(new: ProjectUpdate) -> Self {
        Self {
            identity: IdentityMetadataUpdateParams {
                name: Some(new.identity.name),
                description: Some(new.identity.description),
            },
        }
    }
}
