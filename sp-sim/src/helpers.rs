// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::update::BaseboardKind;
use gateway_messages::{
    CfpaPage, Fwid, RotBootInfo, RotRequest, RotResponse, RotSlotId,
    RotStateV2, RotStateV3, SpError, UpdateError,
};
use std::io::Write;

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

pub(crate) fn rot_boot_info(
    rot_state: RotStateV3,
    old_rot_state: bool,
    version: u8,
) -> Result<RotBootInfo, SpError> {
    if old_rot_state {
        return Err(SpError::RequestUnsupportedForSp);
    }
    match version {
        0 => Err(SpError::Update(UpdateError::VersionNotSupported)),
        1 => Ok(RotBootInfo::V2(rot_state_v2(rot_state))),
        _ => Ok(RotBootInfo::V3(rot_state)),
    }
}

pub(crate) fn read_dummy_rot_page(
    board: BaseboardKind,
    request: RotRequest,
    buf: &mut [u8],
) -> Result<RotResponse, SpError> {
    let board = match board {
        BaseboardKind::Gimlet => "gimlet",
        BaseboardKind::Sidecar => "sidecar",
    };
    let page = match request {
        RotRequest::ReadCmpa => "cmpa",
        RotRequest::ReadCfpa(CfpaPage::Active) => "cfpa-active",
        RotRequest::ReadCfpa(CfpaPage::Inactive) => "cfpa-inactive",
        RotRequest::ReadCfpa(CfpaPage::Scratch) => "cfpa-scratch",
    };
    // the `Write` implementation for `&mut [u8]` mutates the slice to advance
    // the start index past the bytes written, leaving `rest` as the remainder
    // of the page to zero.
    let mut rest = buf;
    write!(rest, "{board}-{page}")
        .expect("`buf` is a whole RoT page, which fits any dummy page name");
    rest.fill(0);
    Ok(RotResponse::Ok)
}
