// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_uuid_kinds::RackUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The rack ID of the rack in which this management gateway is located.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct RackId {
    pub rack_id: RackUuid,
}
