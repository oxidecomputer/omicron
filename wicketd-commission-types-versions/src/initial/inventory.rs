// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use wicket_common::inventory::{RackV1Inventory, SpIdentifier};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct GetInventoryParams {
    /// Refresh the state of these SPs from MGS prior to returning (instead of
    /// returning cached data).
    pub force_refresh: Vec<SpIdentifier>,
}

/// The response to a `get_inventory` call: the inventory known to wicketd, or a
/// notification that data is unavailable.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum GetInventoryResponse {
    Response { inventory: RackV1Inventory },
    Unavailable,
}
