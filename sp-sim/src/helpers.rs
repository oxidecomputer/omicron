// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_messages::{Fwid, RotSlotId, RotStateV2, RotStateV3, SpError};

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

pub(crate) fn rot_state_v2(v3: RotStateV3) -> RotStateV2 {
    let Fwid::Sha3_256(slot_a_sha3_256_digest) = v3.slot_a_fwid;
    let Fwid::Sha3_256(slot_b_sha3_256_digest) = v3.slot_b_fwid;
    RotStateV2 {
        active: v3.active,
        persistent_boot_preference: v3.persistent_boot_preference,
        pending_persistent_boot_preference: v3
            .pending_persistent_boot_preference,
        transient_boot_preference: v3.transient_boot_preference,
        slot_a_sha3_256_digest: Some(slot_a_sha3_256_digest),
        slot_b_sha3_256_digest: Some(slot_b_sha3_256_digest),
    }
}
