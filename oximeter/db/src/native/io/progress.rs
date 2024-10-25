// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Decode progress packets from the server.

use crate::native::io;
use crate::native::packets::server::Progress;
use std::time::Duration;

/// Decode a progress packet from the server, if possible.
pub fn decode(src: &mut &[u8]) -> Option<Progress> {
    let Some(rows_read) = io::varuint::decode(src) else {
        return None;
    };
    let Some(bytes_read) = io::varuint::decode(src) else {
        return None;
    };
    let Some(total_rows_to_read) = io::varuint::decode(src) else {
        return None;
    };
    let Some(total_bytes_to_read) = io::varuint::decode(src) else {
        return None;
    };
    let Some(rows_written) = io::varuint::decode(src) else {
        return None;
    };
    let Some(bytes_written) = io::varuint::decode(src) else {
        return None;
    };
    let Some(query_time_nanos) = io::varuint::decode(src) else {
        return None;
    };
    let query_time = Duration::from_nanos(query_time_nanos);
    Some(Progress {
        rows_read,
        bytes_read,
        total_rows_to_read,
        total_bytes_to_read,
        rows_written,
        bytes_written,
        query_time,
    })
}
