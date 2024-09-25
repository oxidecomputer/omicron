// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Decode a ProfileInfo struct.

use crate::native::io;
use crate::native::packets::server::ProfileInfo;
use bytes::Buf as _;

pub fn decode(src: &mut &[u8]) -> Option<ProfileInfo> {
    let Some(n_rows) = io::varuint::decode(src) else {
        return None;
    };
    let Some(n_blocks) = io::varuint::decode(src) else {
        return None;
    };
    let Some(n_bytes) = io::varuint::decode(src) else {
        return None;
    };
    if !src.has_remaining() {
        return None;
    }
    let applied_limit = src.get_u8() == 1;
    let Some(rows_before_limit) = io::varuint::decode(src) else {
        return None;
    };
    if !src.has_remaining() {
        return None;
    }
    let calculated_rows_before_limit = src.get_u8() == 1;
    Some(ProfileInfo {
        n_rows,
        n_blocks,
        n_bytes,
        applied_limit,
        rows_before_limit,
        calculated_rows_before_limit,
    })
}
