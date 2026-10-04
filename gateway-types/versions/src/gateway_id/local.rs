// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_uuid_kinds::RackUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The identity of this management gateway service.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct GatewayIdentity {
    /// The rack UUID of the rack in which this management gateway is located.
    ///
    /// All service processors contacted through this gateway can be assumed
    /// to be located in this rack.
    pub rack_id: RackUuid,
    /// The unique UUID of this management gateway service process.
    pub gateway_id: Uuid,
}
