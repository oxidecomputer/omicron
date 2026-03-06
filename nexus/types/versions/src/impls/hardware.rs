// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for hardware types.

use crate::latest::hardware::UninitializedSledId;
use sled_hardware_types::BaseboardId;

impl From<UninitializedSledId> for BaseboardId {
    fn from(value: UninitializedSledId) -> Self {
        BaseboardId { part_number: value.part, serial_number: value.serial }
    }
}
