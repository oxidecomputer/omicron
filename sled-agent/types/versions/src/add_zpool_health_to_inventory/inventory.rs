// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use illumos_utils::zpool::ZpoolHealth;
use omicron_common::api::external::ByteCount;
use omicron_uuid_kinds::ZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// TODO-K: Add the inventory type here as well and modify it to use this type

/// Identifies information about zpools managed by the control plane
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InventoryZpool {
    pub id: ZpoolUuid,
    pub total_size: ByteCount,
    pub health: ZpoolHealth,
}
