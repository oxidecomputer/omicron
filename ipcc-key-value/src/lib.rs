// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Utilities for key/value pairs passed from the control plane to the SP
//! (through MGS) to the host (through the host/SP uart) via IPCC.

use serde::Deserialize;
use serde::Serialize;

/// Supported keys.
///
/// The numeric values of these keys should match the definitions in the SP
/// `host-sp-comms` task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Key {
    /// Always responds `"pong"`.
    Ping = 0,
    /// The value should be an encoded [`InstallinatorImageId`].
    InstallinatorImageId = 1,
}

/// Description of the images `installinator` needs to fetch from a peer on the
/// bootstrap network during sled recovery.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct InstallinatorImageId {
    /// SHA-256 hash of the host phase 2 image to fetch.
    pub host_phase_2: [u8; 32],
    /// SHA-256 hash of the control plane image to fetch.
    pub control_plane: [u8; 32],
}

// The existence of these methods is a _little_ dubious: `InstallinatorImageId`
// is `pub` and implements `Serialize + Deserialize`, so any caller could choose
// to serialize/deserialize an instance with any serde serializer/deserializer
// already. However, in our case MGS will be serializing and installinator will
// be deserializing, so I think it makes sense that both of those can call these
// functions to keep the choice of encoding in one place.
impl InstallinatorImageId {
    pub fn serialize(&self) -> Vec<u8> {
        use ciborium::ser::Error;

        let mut out = Vec::new();

        // Serializing is infallible:
        //
        // a) `Vec`'s `io::Write` implementation is infallible, so we can't get
        //    `Error::Io(_)`
        // b) We know `InstallinatorImageId` can be represented and serialized
        //    by ciborium, so we can't get `Error::Value(_)`.
        //
        // so we mark both of those cases as `unreachable!()`.
        match ciborium::ser::into_writer(self, &mut out) {
            Ok(()) => out,
            Err(Error::Io(err)) => {
                unreachable!("i/o error appending to Vec: {err}");
            }
            Err(Error::Value(err)) => {
                unreachable!("failed to serialize an image ID: {err}");
            }
        }
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        use ciborium::de::Error;

        // ciborium deserialization errors aren't super useful; their
        // `Display` impl just forwards to `Debug` formatting, and we don't
        // expect callers to be able to meaningfully handle the distinction
        // between these cases. We'll squish all of these cases down to just an
        // error string.
        match ciborium::de::from_reader(data) {
            Ok(value) => Ok(value),
            Err(Error::Io(err)) => {
                unreachable!("i/o error reading from slice: {err}")
            }
            Err(Error::Syntax(offset)) => {
                Err(format!("syntax error at offset {offset}"))
            }
            Err(Error::Semantic(Some(offset), description)) => {
                Err(format!("semantic error: {description} (offset {offset})"))
            }
            Err(Error::Semantic(None, description)) => {
                Err(format!("semantic error: {description} (offset unknown)"))
            }
            Err(Error::RecursionLimitExceeded) => {
                Err("recursion limit exceeded".to_string())
            }
        }
    }
}

// TODO Add ioctl wrappers? `installinator` is the only client for
// `Key::InstallinatorImageId`, but we might grow other keys for other clients,
// at which point we probably want all the ioctl wrapping to happen in one
// place.

#[cfg(test)]
mod tests {
    use super::*;
    use test_strategy::proptest;

    #[proptest]
    fn installinator_image_id_round_trip(image_id: InstallinatorImageId) {
        let serialized = image_id.serialize();
        assert_eq!(
            InstallinatorImageId::deserialize(&serialized),
            Ok(image_id)
        );
    }

    #[proptest]
    fn serialized_max_size(image_id: InstallinatorImageId) {
        // Double-check that ciborium is encoding this how we expect. Our
        // serialized size should be:
        //
        // 1. 1 byte to identify a map
        // 2. 1 byte + strlen for each key
        // 3. 2 bytes for each 32-long array
        // 4. 1 or 2 bytes per `u8` in each array, where the number depends on
        //    the value of the byte
        //
        // The absolute cap assuming 2 bytes per `u8` in step 4 is therefore:
        const MAX_SIZE: usize = 1 // map
            + 1 + "host_phase_2".len() // key
            + 1 + "control_plane".len() // key
            + 2 * ( // 2 arrays
                2 + // "32-long array"
                32 * 2 // 32 bytes, 1-2 bytes each
            );

        let serialized = image_id.serialize();
        assert!(serialized.len() <= MAX_SIZE);
    }

    #[test]
    fn deserialize_fixed_value() {
        // Encoding an `InstallinatorImageId` at https://cbor.me with the
        // host_phase_2 hash [1, 2, ..., 32] and the control_plane hash [33, 34,
        // ..., 64]:
        const SERIALIZED: &[u8] = &[
            0xA2, // map(2)
            0x6C, // text(12)
            0x68, 0x6F, 0x73, 0x74, 0x5F, 0x70, 0x68, 0x61, 0x73, 0x65, 0x5F,
            0x32, // "host_phase_2"
            0x98, 0x20, // array(32)
            0x01, // unsigned(1)
            0x02, // unsigned(2)
            0x03, // unsigned(3)
            0x04, // unsigned(4)
            0x05, // unsigned(5)
            0x06, // unsigned(6)
            0x07, // unsigned(7)
            0x08, // unsigned(8)
            0x09, // unsigned(9)
            0x0A, // unsigned(10)
            0x0B, // unsigned(11)
            0x0C, // unsigned(12)
            0x0D, // unsigned(13)
            0x0E, // unsigned(14)
            0x0F, // unsigned(15)
            0x10, // unsigned(16)
            0x11, // unsigned(17)
            0x12, // unsigned(18)
            0x13, // unsigned(19)
            0x14, // unsigned(20)
            0x15, // unsigned(21)
            0x16, // unsigned(22)
            0x17, // unsigned(23)
            0x18, 0x18, // unsigned(24)
            0x18, 0x19, // unsigned(25)
            0x18, 0x1A, // unsigned(26)
            0x18, 0x1B, // unsigned(27)
            0x18, 0x1C, // unsigned(28)
            0x18, 0x1D, // unsigned(29)
            0x18, 0x1E, // unsigned(30)
            0x18, 0x1F, // unsigned(31)
            0x18, 0x20, // unsigned(32)
            0x6D, // text(13)
            0x63, 0x6F, 0x6E, 0x74, 0x72, 0x6F, 0x6C, 0x5F, 0x70, 0x6C, 0x61,
            0x6E, 0x65, // "control_plane"
            0x98, 0x20, // array(32)
            0x18, 0x21, // unsigned(33)
            0x18, 0x22, // unsigned(34)
            0x18, 0x23, // unsigned(35)
            0x18, 0x24, // unsigned(36)
            0x18, 0x25, // unsigned(37)
            0x18, 0x26, // unsigned(38)
            0x18, 0x27, // unsigned(39)
            0x18, 0x28, // unsigned(40)
            0x18, 0x29, // unsigned(41)
            0x18, 0x2A, // unsigned(42)
            0x18, 0x2B, // unsigned(43)
            0x18, 0x2C, // unsigned(44)
            0x18, 0x2D, // unsigned(45)
            0x18, 0x2E, // unsigned(46)
            0x18, 0x2F, // unsigned(47)
            0x18, 0x30, // unsigned(48)
            0x18, 0x31, // unsigned(49)
            0x18, 0x32, // unsigned(50)
            0x18, 0x33, // unsigned(51)
            0x18, 0x34, // unsigned(52)
            0x18, 0x35, // unsigned(53)
            0x18, 0x36, // unsigned(54)
            0x18, 0x37, // unsigned(55)
            0x18, 0x38, // unsigned(56)
            0x18, 0x39, // unsigned(57)
            0x18, 0x3A, // unsigned(58)
            0x18, 0x3B, // unsigned(59)
            0x18, 0x3C, // unsigned(60)
            0x18, 0x3D, // unsigned(61)
            0x18, 0x3E, // unsigned(62)
            0x18, 0x3F, // unsigned(63)
            0x18, 0x40, // unsigned(64)
        ];

        let expected = InstallinatorImageId {
            host_phase_2: [
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
            ],
            control_plane: [
                33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
                49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
            ],
        };

        assert_eq!(InstallinatorImageId::deserialize(SERIALIZED), Ok(expected));
    }
}
