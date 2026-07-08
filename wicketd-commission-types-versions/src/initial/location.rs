// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::Serialize;
use sled_hardware_types::Baseboard;
use wicket_common::inventory::SpIdentifier;

/// All the fields of this response are optional, because it's possible we don't
/// know any of them (yet) if MGS has not yet finished discovering its location
/// or (ever) if we're running in a dev environment that doesn't support
/// MGS-location / baseboard mapping.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GetLocationResponse {
    /// The identity of our sled (where wicketd is running).
    pub sled_id: Option<SpIdentifier>,
    /// The baseboard of our sled (where wicketd is running).
    pub sled_baseboard: Option<Baseboard>,
    /// The baseboard of the switch our sled is physically connected to.
    pub switch_baseboard: Option<Baseboard>,
    /// The identity of the switch our sled is physically connected to.
    pub switch_id: Option<SpIdentifier>,
}
