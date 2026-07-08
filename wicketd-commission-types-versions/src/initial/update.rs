// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::Deserialize;
use wicket_common::inventory::SpIdentifier;
use wicket_common::rack_update::{ClearUpdateStateOptions, StartUpdateOptions};

#[derive(Clone, Debug, JsonSchema, Deserialize)]
pub struct StartUpdateParams {
    /// The SP identifiers to start the update with. Must be non-empty.
    pub targets: BTreeSet<SpIdentifier>,

    /// Options for the update.
    pub options: StartUpdateOptions,
}

#[derive(Clone, Debug, JsonSchema, Deserialize)]
pub struct ClearUpdateStateParams {
    /// The SP identifiers to clear the update state for. Must be non-empty.
    pub targets: BTreeSet<SpIdentifier>,

    /// Options for clearing update state
    pub options: ClearUpdateStateOptions,
}
