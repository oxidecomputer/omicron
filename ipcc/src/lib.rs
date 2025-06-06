// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! An interface to libipcc (inter-processor communications channel) which
//! currently supports looking up values stored in the SP by key. These
//! values are variously static, passed from the control plane to the SP
//! (through MGS) or set from userland via libipcc.

use libipcc::{IpccError, IpccHandle};
use omicron_uuid_kinds::MupdateUuid;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use tufaceous_artifact::ArtifactHash;

#[cfg(test)]
use proptest::arbitrary::any;
#[cfg(test)]
use proptest::strategy::Strategy;

/// Supported keys.
///
/// The numeric values of these keys should match the definitions in the SP
/// `host-sp-comms` task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Key {
    /// Always responds `"pong"`.
    Ping = IpccKey::Ping as u8,
    /// The value should be an encoded [`InstallinatorImageId`].
    InstallinatorImageId = IpccKey::InstallinatorImageId as u8,
}

/// Description of the images `installinator` needs to fetch from a peer on the
/// bootstrap network during sled recovery.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub struct InstallinatorImageId {
    /// UUID identifying this update.
    ///
    /// Installinator can send progress and completion messages to all peers it
    /// finds, and any peer that cares about progress can use this ID to match
    /// the progress message with a running recovery.
    #[cfg_attr(test, strategy(any::<[u8; 16]>().prop_map(MupdateUuid::from_bytes)))]
    pub update_id: MupdateUuid,
    /// SHA-256 hash of the host phase 2 image to fetch.
    pub host_phase_2: ArtifactHash,
    /// SHA-256 hash of the control plane image to fetch.
    pub control_plane: ArtifactHash,
}

impl InstallinatorImageId {
    /// The size in bytes of an `InstallinatorImageId` serialized into CBOR via
    /// [`InstallinatorImageId::serialize()`].
    #[allow(dead_code)] // Used by tests and platform-specific modules
    const CBOR_SERIALIZED_SIZE: usize = 1 // map
            + 1 + "update_id".len() // key
            + 1 + "host_phase_2".len() // key
            + 1 + "control_plane".len() // key
            + 1 + 16 // UUID byte array
            + 2*(2 + 32); // 2 32-long byte arrays
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
                Err(format!("i/o error reading from slice: {err}"))
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

#[derive(Debug, Error)]
pub enum InstallinatorImageIdError {
    #[error(transparent)]
    Ipcc(#[from] IpccError),
    #[error("deserializing installinator image ID failed: {0}")]
    DeserializationFailed(String),
}

/// These are the IPCC keys we can look up.
/// NB: These keys match the definitions found in libipcc (RFD 316) and should
/// match the values in `[ipcc::Key]` one-to-one.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
#[repr(u8)]
enum IpccKey {
    Ping = 0,
    InstallinatorImageId = 1,
    Inventory = 2,
    System = 3,
    Dtrace = 4,
}

/// Interface to the inter-processor communications channel.
/// For more information see rfd 316.
pub struct Ipcc {
    handle: IpccHandle,
}

impl Ipcc {
    /// Creates a new `Ipcc` instance.
    pub fn new() -> Result<Self, IpccError> {
        let handle = IpccHandle::new()?;
        Ok(Self { handle })
    }

    /// Returns the current `InstallinatorImageId`.
    pub fn installinator_image_id(
        &self,
    ) -> Result<InstallinatorImageId, InstallinatorImageIdError> {
        let mut buf = [0; InstallinatorImageId::CBOR_SERIALIZED_SIZE];
        let n = self
            .handle
            .key_lookup(IpccKey::InstallinatorImageId as u8, &mut buf)?;
        let id = InstallinatorImageId::deserialize(&buf[..n])
            .map_err(InstallinatorImageIdError::DeserializationFailed)?;
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_uuid_kinds::MupdateUuid;
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
    fn serialized_size(image_id: InstallinatorImageId) {
        let serialized = image_id.serialize();
        assert!(serialized.len() == InstallinatorImageId::CBOR_SERIALIZED_SIZE);
    }

    #[test]
    fn deserialize_fixed_value() {
        // Encoding an `InstallinatorImageId` at https://cbor.me with the
        // host_phase_2 hash [1, 2, ..., 32] and the control_plane hash [33, 34,
        // ..., 64]:
        const SERIALIZED: &[u8] = &[
            0xA3, // map(3)
            0x69, // text(9)
            0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x5f, 0x69,
            0x64, // "update_id"
            0x50, // bytes(16),
            0x41, // byte(65)
            0x42, // byte(66)
            0x43, // byte(67)
            0x44, // byte(68)
            0x45, // byte(69)
            0x46, // byte(70)
            0x47, // byte(71)
            0x48, // byte(72)
            0x49, // byte(73)
            0x4a, // byte(74)
            0x4b, // byte(75)
            0x4c, // byte(76)
            0x4d, // byte(77)
            0x4e, // byte(78)
            0x4f, // byte(79)
            0x50, // byte(80)
            0x6C, // text(12)
            0x68, 0x6F, 0x73, 0x74, 0x5F, 0x70, 0x68, 0x61, 0x73, 0x65, 0x5F,
            0x32, // "host_phase_2"
            0x58, 0x20, // bytes(32)
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
            0x18, // unsigned(24)
            0x19, // unsigned(25)
            0x1A, // unsigned(26)
            0x1B, // unsigned(27)
            0x1C, // unsigned(28)
            0x1D, // unsigned(29)
            0x1E, // unsigned(30)
            0x1F, // unsigned(31)
            0x20, // unsigned(32)
            0x6D, // text(13)
            0x63, 0x6F, 0x6E, 0x74, 0x72, 0x6F, 0x6C, 0x5F, 0x70, 0x6C, 0x61,
            0x6E, 0x65, // "control_plane"
            0x58, 0x20, // bytes(32)
            0x21, // unsigned(33)
            0x22, // unsigned(34)
            0x23, // unsigned(35)
            0x24, // unsigned(36)
            0x25, // unsigned(37)
            0x26, // unsigned(38)
            0x27, // unsigned(39)
            0x28, // unsigned(40)
            0x29, // unsigned(41)
            0x2A, // unsigned(42)
            0x2B, // unsigned(43)
            0x2C, // unsigned(44)
            0x2D, // unsigned(45)
            0x2E, // unsigned(46)
            0x2F, // unsigned(47)
            0x30, // unsigned(48)
            0x31, // unsigned(49)
            0x32, // unsigned(50)
            0x33, // unsigned(51)
            0x34, // unsigned(52)
            0x35, // unsigned(53)
            0x36, // unsigned(54)
            0x37, // unsigned(55)
            0x38, // unsigned(56)
            0x39, // unsigned(57)
            0x3A, // unsigned(58)
            0x3B, // unsigned(59)
            0x3C, // unsigned(60)
            0x3D, // unsigned(61)
            0x3E, // unsigned(62)
            0x3F, // unsigned(63)
            0x40, // unsigned(64)
        ];

        let expected = InstallinatorImageId {
            update_id: MupdateUuid::from_bytes([
                65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
            ]),
            host_phase_2: ArtifactHash([
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
            ]),
            control_plane: ArtifactHash([
                33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
                49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
            ]),
        };

        assert_eq!(InstallinatorImageId::deserialize(SERIALIZED), Ok(expected));
    }
}
