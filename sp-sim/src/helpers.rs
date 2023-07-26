// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_messages::{RotSlotId, SpError};

pub(crate) fn rot_slot_id_to_u16(slot_id: RotSlotId) -> u16 {
    match slot_id {
        RotSlotId::A => 0,
        RotSlotId::B => 1,
    }
}

pub(crate) fn rot_slot_id_from_u16(slot_id: u16) -> Result<RotSlotId, SpError> {
    match slot_id {
        0 => Ok(RotSlotId::A),
        1 => Ok(RotSlotId::B),
        _ => Err(SpError::InvalidSlotForComponent),
    }
}
