// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Variable UInt64 encoding.
//!
//! ClickHouse uses variable-length encodings for u64s in many places. For
//! example, all strings are prefixed with their length encoded this way. Many
//! other packets also encode u64s like this, though not all of them.
//!
//! This encoding is compact, but pretty annoying to work with. It's not
//! possible to know how big any packet containing this data is without just
//! iterating over the values (especially since ClickHouse does not append a
//! fixed-size length header to its messages).

use bytes::{Buf, BufMut};

/// Encode a u64 as a variable-length integer, returning the number of bytes
/// written.
///
/// The variable-length encoding packs a u64 into bytes. The low 7 bits are used
/// for data, and the last bit is used to signal the presence of more data
/// bytes.
pub fn encode(mut x: u64, mut buf: impl BufMut) -> usize {
    let mut count = 0;
    while x > 0x7F {
        buf.put_u8((x as u8 & 0x7F) | 0x80);
        x >>= 7;
        count += 1;
    }
    buf.put_u8(x as u8 & 0x7F);
    count + 1
}

/// Decode a u64 in variable-length encoding, if possible.
pub fn decode<T: Buf>(buf: &mut T) -> Option<u64> {
    // As we decode bytes, we'll insert them into the right position in this
    // output.
    let mut out: u64 = 0;

    // The largest int is all `0xff`s. We'll take 1 bit from each of those for
    // the continuation bit, so we have 7 bits per byte for those initial bytes.
    // That means we need ceil(64 / 7) bytes to encode all of them, which is 10.
    const MAX_LEN: usize = 10;
    for i in 0..MAX_LEN {
        if !buf.has_remaining() {
            return None;
        }
        let x = buf.get_u8();
        out |= u64::from(x & 0x7F) << (7 * i);
        if (x & 0x80) == 0 {
            return Some(out);
        }
    }
    unreachable!();
}

#[cfg(test)]
#[test]
fn test_encode_decode_varuint() {
    use bytes::BytesMut;

    for x in [
        0u64,
        u64::from(u8::MAX) - 1,
        u64::from(u8::MAX),
        u64::from(u8::MAX) + 1,
        u64::from(u16::MAX) - 1,
        u64::from(u16::MAX),
        u64::from(u16::MAX) + 1,
        u64::from(u32::MAX) - 1,
        u64::from(u32::MAX),
        u64::from(u32::MAX) + 1,
        u64::MAX - 1,
        u64::MAX,
    ] {
        let mut buf = BytesMut::with_capacity(64);
        encode(x, &mut buf);
        let out = decode(&mut buf).unwrap();
        assert_eq!(x, out);
    }
}
