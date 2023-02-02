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
// TODO this may need more or different fields
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct InstallinatorImageId {
    /// SHA-256 hash of the host OS image to fetch.
    pub host_os: [u8; 32],
    /// SHA-256 hash of the control plane image to fetch.
    pub control_plane: [u8; 32],
}

// TODO the existence of these methods is a _little_ dubious:
// `InstallinatorImageId` is `pub` and implements `Serialize + Deserialize`, so
// any caller could choose to serialize/deserialize an instance with any
// serde serializer/deserializer already. However, in our case MGS will be
// serializing and installinator will be deserializing, so I think it makes
// sense that both of those can call these functions to keep the choice of
// encoding in one place.
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
