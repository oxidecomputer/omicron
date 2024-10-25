// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Encoding ClickHouse UTF-8 strings.
//!
//! ClickHouse encodes all strings as a tuple, consisting of its length followed
//! by the string's UTF8 bytes. Note that the length is always encoded using the
//! variable-length usigned integer encoding.

use super::varuint;
use crate::native::Error;
use bytes::Buf;
use bytes::BufMut;

/// Encode a string into the ClickHouse format.
pub fn encode(s: impl AsRef<str>, mut buf: impl BufMut) {
    let s = s.as_ref();
    varuint::encode(s.len() as u64, &mut buf);
    buf.put(s.as_bytes());
}

/// Decode a string in ClickHouse format, if possible.
pub fn decode<T: Buf>(buf: &mut T) -> Result<Option<String>, Error> {
    let Some(len) = varuint::decode(buf).map(|x| x as usize) else {
        return Ok(None);
    };
    if buf.remaining() < len {
        return Ok(None);
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes).map(Option::Some).map_err(|_| Error::NonUtf8String)
}

#[cfg(test)]
mod test {
    use super::decode;
    use super::encode;
    use bytes::BytesMut;

    #[test]
    fn test_encode_hello_world() {
        const EXPECTED_BYTES: &[u8] = &[
            0x0d, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x77, 0x6f, 0x72,
            0x6c, 0x64, 0x21,
        ];
        const SRC: &str = "Hello, world!";
        let mut buf = BytesMut::with_capacity(64);
        encode(SRC, &mut buf);
        assert_eq!(EXPECTED_BYTES, buf.split());
    }

    #[test]
    fn test_encode_decode() {
        const SRC: &str = "a string";
        let mut buf = BytesMut::with_capacity(64);
        encode(SRC, &mut buf);
        let out = decode(&mut buf).unwrap().unwrap();
        assert_eq!(out, SRC);
    }
}
