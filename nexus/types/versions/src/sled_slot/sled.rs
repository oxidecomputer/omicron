// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::asset::AssetIdentityMetadata;
use crate::v2025_11_20_00::hardware::Baseboard;
use crate::v2025_11_20_00::sled::{SledPolicy, SledState};
use omicron_common::api::external::ByteCount;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// An operator's view of a Sled.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Sled {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    pub baseboard: Baseboard,
    /// The rack to which this Sled is currently attached
    pub rack_id: Uuid,
    /// The physical slot in the rack where this sled is currently located, or
    /// null if its location is not known at this time.
    pub slot: Option<u8>,
    /// The operator-defined policy of a sled.
    pub policy: SledPolicy,
    /// The current state of the sled.
    pub state: SledState,
    /// The number of hardware threads which can execute on this sled
    pub usable_hardware_threads: u32,
    /// Amount of RAM which may be used by the Sled's OS
    pub usable_physical_ram: ByteCount,
}

impl From<Sled> for v2025_11_20_00::sled::Sled {
    fn from(new: Sled) -> Self {
        let Sled {
            identity,
            baseboard,
            rack_id,
            policy,
            state,
            usable_hardware_threads,
            usable_physical_ram,
            ..
        } = new;
        Self {
            identity,
            baseboard,
            rack_id,
            policy,
            state,
            usable_hardware_threads,
            usable_physical_ram,
        }
    }
}
