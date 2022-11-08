// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack inventory for display by wicket

use gateway_client::types::{SpComponentInfo, SpInfo};
use schemars::JsonSchema;
use serde::Serialize;

/// The current state of the v1 Rack as known to wicketd
#[derive(Default, Debug, Serialize, JsonSchema)]
pub struct RackV1Inventory {
    pub sps: Vec<(SpInfo, Vec<SpComponentInfo>)>,
}
