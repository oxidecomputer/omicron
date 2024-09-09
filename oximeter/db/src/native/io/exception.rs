// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Decode server exception packets.

use crate::native::{io, packets::server::Exception, Error};
use bytes::Buf as _;

/// Decode a list of Exception packets from the server, if possible.
pub fn decode(src: &mut &[u8]) -> Result<Option<Vec<Exception>>, Error> {
    // Loop until we are certain there are no more "nested" exceptions, which
    // are just appended back-to-back.
    let mut out = Vec::new();
    loop {
        if src.remaining() < std::mem::size_of::<i32>() {
            return Ok(None);
        }
        let code = src.get_i32_le();
        let Some(name) = io::string::decode(src)? else {
            return Ok(None);
        };
        let Some(message) = io::string::decode(src)? else {
            return Ok(None);
        };
        let Some(stack_trace) = io::string::decode(src)? else {
            return Ok(None);
        };
        if !src.has_remaining() {
            return Ok(None);
        };
        let nested = src.get_u8() == 1;
        let exc = Exception { code, name, message, stack_trace, nested };
        out.push(exc);
        if !nested {
            return Ok(Some(out));
        }
    }
}
