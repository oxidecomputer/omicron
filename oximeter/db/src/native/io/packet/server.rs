// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2025 Oxide Computer Company

//! Decode packets from the ClickHouse server.

use crate::native::Error;
use crate::native::io;
use crate::native::packets::server::Hello;
use crate::native::packets::server::Packet;
use crate::native::packets::server::PasswordComplexityRule;
use crate::native::probes;
use bytes::Buf as _;
use bytes::BytesMut;
use std::net::IpAddr;

/// A decoder for packets from the ClickHouse server.
#[derive(Debug)]
pub struct Decoder {
    /// IP address of the server we're decoding packets from.
    ///
    /// This is used only as an argument USDT probes, not decoding itself.
    pub addr: IpAddr,
}

impl Decoder {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self { addr: std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST) }
    }

    /// Decode a Hello packet from the server, if possible.
    fn decode_hello(src: &mut &[u8]) -> Result<Option<Hello>, Error> {
        let Some(name) = io::string::decode(src)? else {
            return Ok(None);
        };
        let Some(version_major) = io::varuint::decode(src) else {
            return Ok(None);
        };
        let Some(version_minor) = io::varuint::decode(src) else {
            return Ok(None);
        };
        let Some(revision) = io::varuint::decode(src) else {
            return Ok(None);
        };
        let Some(tz) = io::string::decode(src)? else {
            return Ok(None);
        };
        let Some(display_name) = io::string::decode(src)? else {
            return Ok(None);
        };
        let Some(version_patch) = io::varuint::decode(src) else {
            return Ok(None);
        };
        let Some(n_rules) = io::varuint::decode(src) else {
            return Ok(None);
        };

        // We don't expect any rules, but the revision we advertise supports
        // them, so we may get some from the server.
        let mut password_complexity_rules = Vec::with_capacity(n_rules as _);
        for _ in 0..n_rules {
            let Some(pattern) = io::string::decode(src)? else {
                return Ok(None);
            };
            let Some(exception) = io::string::decode(src)? else {
                return Ok(None);
            };
            password_complexity_rules
                .push(PasswordComplexityRule { pattern, exception });
        }

        // The interserver secret / nonce is not variable-length, just a
        // straight u64 as bytes.
        let Some((mut bytes, remainder)) = src.split_at_checked(8) else {
            return Ok(None);
        };
        let interserver_secret = bytes.get_u64_le();
        *src = remainder;
        Ok(Some(Hello {
            name,
            version_major,
            version_minor,
            revision,
            tz,
            display_name,
            version_patch,
            password_complexity_rules,
            interserver_secret,
        }))
    }
}

impl tokio_util::codec::Decoder for Decoder {
    type Error = Error;
    type Item = Packet;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // Next, we always try to decode the current frame from a slice into our
        // buffer. This is a mutable slice, though it refers to immutable bytes.
        // As we process the packet, we always chew through the prefix of this
        // view. If we succeed, we then shorten `src` to match what we
        // processed. But if we don't have enough data, we simply drop this and
        // try again on the next call.
        let mut buf = src.as_ref();

        // Grab the kind of packet, and then dispatch to the right decoder.
        let kind = buf[0];
        buf = &buf[1..];
        let packet = match kind {
            Packet::HELLO => {
                match Self::decode_hello(&mut buf) {
                    Ok(Some(hello)) => Packet::Hello(hello),
                    Ok(None) => {
                        // We did not have a full packet, so just return. We'll
                        // append new bytes from the I/O resource and try again
                        // next time.
                        return Ok(None);
                    }
                    Err(e) => {
                        // We failed to correctly decode the packet.
                        //
                        // We may need to do something else in the future, but
                        // for now, just throw away the buffer. We have no idea
                        // how much data we need to lop off. We should
                        // realistically RST the whole TCP connection, most
                        // likely.
                        probes::invalid__packet!(|| (
                            self.addr.to_string(),
                            "Hello",
                            src.len()
                        ));
                        src.clear();
                        return Err(e);
                    }
                }
            }
            Packet::DATA => match io::block::decode(&mut buf) {
                Ok(Some(block)) => Packet::Data(block),
                Ok(None) => return Ok(None),
                Err(e) => {
                    probes::invalid__packet!(|| (
                        self.addr.to_string(),
                        "Data",
                        src.len()
                    ));
                    src.clear();
                    return Err(e);
                }
            },
            Packet::EXCEPTION => match io::exception::decode(&mut buf) {
                Ok(Some(exceptions)) => Packet::Exception(exceptions),
                Ok(None) => return Ok(None),
                Err(e) => {
                    probes::invalid__packet!(|| (
                        self.addr.to_string(),
                        "Exception",
                        src.len()
                    ));
                    src.clear();
                    return Err(e);
                }
            },
            Packet::PROGRESS => {
                let Some(progress) = io::progress::decode(&mut buf) else {
                    return Ok(None);
                };
                Packet::Progress(progress)
            }
            Packet::PONG => Packet::Pong,
            Packet::END_OF_STREAM => Packet::EndOfStream,
            Packet::PROFILE_INFO => {
                let Some(info) = io::profile_info::decode(&mut buf) else {
                    return Ok(None);
                };
                Packet::ProfileInfo(info)
            }
            Packet::TABLE_COLUMNS => {
                match io::table_columns::decode(&mut buf) {
                    Ok(Some(columns)) => Packet::TableColumns(columns),
                    Ok(None) => return Ok(None),
                    Err(e) => {
                        probes::invalid__packet!(|| (
                            self.addr.to_string(),
                            "TableColumns",
                            src.len()
                        ));
                        src.clear();
                        return Err(e);
                    }
                }
            }
            Packet::PROFILE_EVENTS => match io::block::decode(&mut buf) {
                // Profile events are encoded as a data block.
                Ok(Some(block)) => Packet::ProfileEvents(block),
                Ok(None) => return Ok(None),
                Err(e) => {
                    probes::invalid__packet!(|| (
                        self.addr.to_string(),
                        "ProfileEvents",
                        src.len()
                    ));
                    src.clear();
                    return Err(e);
                }
            },
            _ => {
                // We don't know anything about the packet, drop it.
                //
                // Again, we probably need to handle this more gracefully, but
                // it's not clear how in the absence of a header with the packet
                // size.
                probes::unrecognized__server__packet!(|| (
                    self.addr.to_string(),
                    u64::from(kind),
                    src.len()
                ));
                src.clear();
                return Err(Error::UnrecognizedServerPacket(kind));
            }
        };

        // Now that we've decoded the full frame, chop off the bytes we actually
        // consumed during that process, to be sure we start at the next frame
        // when we're called again.
        probes::packet__received!(|| (
            self.addr.to_string(),
            packet.kind(),
            &packet
        ));
        let n_consumed = src.len() - buf.len();
        src.advance(n_consumed);
        Ok(Some(packet))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::native::packets::server::Exception;
    use crate::native::packets::server::Progress;
    use crate::native::packets::server::REVISION;
    use bytes::BufMut as _;
    use tokio_util::codec::Decoder as _;

    // Server Hello packet, captured from CLI using tcpdump
    const HELLO_PACKET: &[u8] = b"\
        \x00\x0aClickHouse\x17\x08\xc1\xa9\x03\x13\
        America/Los_Angeles\x05flint\x07\
        \x00\
        \xf3\x9c\x51\x7d\x2b\xca\x31\x78";

    // Server Pong packet. Contains just the type byte, nothing else.
    const PONG_PACKET: &[u8] = &[Packet::PONG];

    // Server exception packet. Contains:
    //
    // - code (i32)
    // - name (string)
    // - message (string)
    // - stack strace (string)
    // - nested (bool), which is true if there are appended exceptions.
    const EXCEPTION_PACKET: &[u8] = b"\
        \x02\
        \x01\x00\x00\x00\
        \x03foo\
        \x09exception\
        \x0bstack trace\
        \x00\
    ";

    // Server progress packet.
    const PROGRESS_PACKET: &[u8] =
        &[0x03, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];

    fn expected_server_hello() -> Hello {
        Hello {
            name: String::from("ClickHouse"),
            version_major: 0x17,
            version_minor: 0x08,
            revision: REVISION,
            tz: String::from("America/Los_Angeles"),
            display_name: String::from("flint"),
            version_patch: 0x07,
            password_complexity_rules: vec![],
            interserver_secret: 0x7831ca2b7d519cf3,
        }
    }

    fn expected_exception() -> Exception {
        Exception {
            code: 1,
            name: "foo".into(),
            message: "exception".into(),
            stack_trace: "stack trace".into(),
            nested: false,
        }
    }

    #[test]
    fn test_decode_full_hello() {
        let mut decoder = Decoder::for_test();
        let mut bytes = BytesMut::from(HELLO_PACKET);
        let packet = decoder
            .decode(&mut bytes)
            .expect("Should be able to decode a full server hello packet");
        let Some(Packet::Hello(hello)) = &packet else {
            panic!("Should have decoded a hello packet, found: {packet:?}");
        };
        let expected = expected_server_hello();
        assert_eq!(hello, &expected, "Failed to correctly decode server Hello");
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_decode_partial_hello() {
        let mut decoder = Decoder::for_test();
        let mut prefix = BytesMut::from(&HELLO_PACKET[..10]);
        let mut suffix = BytesMut::from(&HELLO_PACKET[10..]);

        let packet = decoder
            .decode(&mut prefix)
            .expect("Should not emit error when decoding valid partial packet");
        assert_eq!(
            packet, None,
            "Should have returned None when decoding partial packet"
        );
        assert_eq!(prefix.len(), 10);

        decoder.decode(&mut suffix).expect_err(
            "Should return an error when decoding from the middle of a frame",
        );
        assert!(suffix.is_empty(), "Should truncate the input buffer on error");
    }

    #[test]
    fn test_decode_hello_packet_and_a_half() {
        let mut decoder = Decoder::for_test();
        let mut bytes = BytesMut::from(HELLO_PACKET);
        bytes.extend(&HELLO_PACKET[..10]);
        let packet = decoder
            .decode(&mut bytes)
            .expect("Should be able to decode a full server hello packet");
        let Some(Packet::Hello(hello)) = &packet else {
            panic!("Should have decoded a hello packet, found: {packet:?}");
        };
        let expected = expected_server_hello();
        assert_eq!(hello, &expected, "Failed to correctly decode server Hello");
        assert_eq!(bytes.len(), 10, "Should have left the remaining bytes");
    }

    #[test]
    fn test_decode_multiple_hello_packets() {
        let mut decoder = Decoder::for_test();
        let mut bytes = BytesMut::from(HELLO_PACKET);
        bytes.extend(HELLO_PACKET);
        bytes.extend(&HELLO_PACKET[..10]);
        for i in 0..2 {
            let packet = decoder
                .decode(&mut bytes)
                .expect("Should be able to decode a full server hello packet");
            let Some(Packet::Hello(hello)) = &packet else {
                panic!("Should have decoded a hello packet, found: {packet:?}");
            };
            let expected = expected_server_hello();
            assert_eq!(
                hello, &expected,
                "Failed to correctly decode server Hello number {i}"
            );
        }
        assert_eq!(bytes.len(), 10, "Should have left the remaining bytes");
    }

    #[test]
    fn test_decode_pong() {
        let mut decoder = Decoder::for_test();
        let mut bytes = BytesMut::from(PONG_PACKET);
        let packet = decoder
            .decode(&mut bytes)
            .expect("Should be able to decode a full server pong packet");
        let Some(Packet::Pong) = &packet else {
            panic!("Should have decoded a pong packet, found: {packet:?}");
        };
    }

    #[test]
    fn test_decode_single_exception() {
        let mut decoder = Decoder::for_test();
        let mut bytes = BytesMut::from(EXCEPTION_PACKET);
        let packet = decoder
            .decode(&mut bytes)
            .expect("Should be able to decode full server exception packet");
        let Some(Packet::Exception(exceptions)) = &packet else {
            panic!("Should have decoded an exception packet, found {packet:?}");
        };
        assert_eq!(exceptions.len(), 1);
        assert_eq!(exceptions[0], expected_exception());
    }

    #[test]
    fn test_decode_nested_exceptions() {
        let mut decoder = Decoder::for_test();

        // Modify the first exception so that it seems nested.
        let mut bytes =
            BytesMut::from(&EXCEPTION_PACKET[..EXCEPTION_PACKET.len() - 1]);
        bytes.put_u8(1);

        // Now append the second exception, which is not nested. Strip the first
        // byte which is the packet type.
        bytes.extend(&EXCEPTION_PACKET[1..]);
        let packet = decoder
            .decode(&mut bytes)
            .expect("Should be able to decode full server exception packet");
        let Some(Packet::Exception(exceptions)) = &packet else {
            panic!("Should have decoded an exception packet, found {packet:?}");
        };
        let second = expected_exception();
        let first = Exception { nested: true, ..second.clone() };

        assert_eq!(exceptions.len(), 2);
        assert_eq!(exceptions[0], first);
        assert_eq!(exceptions[1], second);
    }

    #[test]
    fn test_decode_progress_packet() {
        let mut decoder = Decoder::for_test();
        let mut bytes = BytesMut::from(PROGRESS_PACKET);
        let packet = decoder
            .decode(&mut bytes)
            .expect("Should be able to decode full server progress packet");
        let Some(Packet::Progress(progress)) = &packet else {
            panic!("Should have decoded a progress packet, found {packet:?}");
        };
        assert_eq!(
            progress,
            &Progress {
                rows_read: 1,
                bytes_read: 2,
                total_rows_to_read: 3,
                total_bytes_to_read: 4,
                rows_written: 5,
                bytes_written: 6,
                query_time: Duration::from_nanos(7),
            }
        );
    }
}
