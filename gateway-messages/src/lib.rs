// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![cfg_attr(not(test), no_std)]

pub mod sp_impl;

use bitflags::bitflags;
use core::{fmt, str};
use serde::{Deserialize, Serialize};

pub use hubpack::error::Error as HubpackError;
pub use hubpack::{deserialize, serialize, SerializedSize};

pub mod version {
    pub const V1: u32 = 1;
}

// TODO: Ignition messages have a `target` for identification; what do the
// other messages need?

/// Messages from a gateway to an SP.
#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub struct Request {
    pub version: u32,
    pub request_id: u32,
    pub kind: RequestKind,
}

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub enum RequestKind {
    Ping,
    // TODO do we want to be able to request IgnitionState for all targets in
    // one message?
    IgnitionState { target: u8 },
    IgnitionCommand { target: u8, command: IgnitionCommand },
    SpState,
    SerialConsoleWrite(SerialConsole),
}

// TODO: Not all SPs are capable of crafting all these response kinds, but the
// way we're using hubpack requires everyone to allocate Response::MAX_SIZE. Is
// that okay, or should we break this up more?
#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub enum ResponseKind {
    Pong,
    IgnitionState(IgnitionState),
    IgnitionCommandAck,
    SpState(SpState),
    SerialConsoleWriteAck,
    Error(ResponseError),
}

// TODO how is this reported? Same/different for components?
pub type SerialNumber = [u8; 16];

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub struct SpState {
    pub serial_number: SerialNumber,
}

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub enum ResponseError {
    /// The [`RequestKind`] is not supported by the receiving SP; e.g., asking an
    /// SP without an attached ignition controller for ignition state.
    RequestUnsupportedForSp,
    /// The [`RequestKind`] is not supported by the receiving component of the
    /// SP; e.g., asking for the serial console of a component that does not
    /// have one.
    RequestUnsupportedForComponent,
    /// The specified ignition target does not exist.
    IgnitionTargetDoesNotExist(u8),
}

/// Messages from an SP to a gateway. Includes both responses to [`Request`]s as
/// well as SP-initiated messages like serial console output.
#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub struct SpMessage {
    pub version: u32,
    pub kind: SpMessageKind,
}

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub enum SpMessageKind {
    // TODO: Is only sending the new state sufficient?
    // IgnitionChange { target: u8, new_state: IgnitionState },
    /// Response to a [`Request`] from MGS.
    Response { request_id: u32, kind: ResponseKind },

    /// Data traveling from an SP-attached component (in practice, a CPU) on the
    /// component's serial console.
    SerialConsole(SerialConsole),
}

#[derive(
    Debug, Clone, Copy, PartialEq, SerializedSize, Serialize, Deserialize,
)]
pub struct IgnitionState {
    pub id: u16,
    pub flags: IgnitionFlags,
}

bitflags! {
    #[derive(SerializedSize, Serialize, Deserialize)]
    pub struct IgnitionFlags: u8 {
        // RFD 142, 5.2.4 status bits
        const POWER = 0b0000_0001;
        const CTRL_DETECT_0 = 0b0000_0010;
        const CTRL_DETECT_1 = 0b0000_0100;
        // const RESERVED_3 = 0b0000_1000;

        // RFD 142, 5.2.3 fault signals
        const FLT_A3 = 0b0001_0000;
        const FLT_A2 = 0b0010_0000;
        const FLT_ROT = 0b0100_0000;
        const FLT_SP = 0b1000_0000;
    }
}

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub enum IgnitionCommand {
    PowerOn,
    PowerOff,
}

/// Identifier for a single component managed by an SP.
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, SerializedSize, Serialize, Deserialize,
)]
pub struct SpComponent {
    /// The ID of the component.
    ///
    /// TODO This may need some thought. Currently we expect this to contain
    /// up to `MAX_ID_LENGTH` nonzero utf8 bytes followed by nul bytes as
    /// padding.
    ///
    /// An `SpComponent` can be created via its `TryFrom<&str>` implementation,
    /// which appends the appropriate padding.
    pub id: [u8; Self::MAX_ID_LENGTH],
}

impl SpComponent {
    /// Maximum number of bytes for a component ID.
    pub const MAX_ID_LENGTH: usize = 16;

    /// Interpret the component name as a human-readable string.
    ///
    /// Our current expectation of component names is that this should never
    /// fail (i.e., we're always storing component names as human-readable
    /// strings), but because we reconstitute components from network messages
    /// we still need to check.
    pub fn as_str(&self) -> Option<&str> {
        let n =
            self.id.iter().position(|&c| c == 0).unwrap_or(Self::MAX_ID_LENGTH);
        str::from_utf8(&self.id[..n]).ok()
    }
}

impl fmt::Debug for SpComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("SpComponent");
        if let Some(s) = self.as_str() {
            debug.field("id", &s);
        } else {
            debug.field("id", &self.id);
        }
        debug.finish()
    }
}

/// Error type returned from `TryFrom<&str> for SpComponent` if the provided ID
/// is too long.
pub struct SpComponentIdTooLong;

impl TryFrom<&str> for SpComponent {
    type Error = SpComponentIdTooLong;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() > Self::MAX_ID_LENGTH {
            return Err(SpComponentIdTooLong);
        }

        let mut component = SpComponent { id: [0; Self::MAX_ID_LENGTH] };

        // should we sanity check that `value` doesn't contain any nul bytes?
        // seems like overkill; probably fine to omit
        component.id[..value.len()].copy_from_slice(value.as_bytes());

        Ok(component)
    }
}

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub struct SerialConsole {
    /// Source component with an attached serial console.
    pub component: SpComponent,

    /// Offset of this chunk of data relative to all console data this
    /// source has sent since it booted. The receiver can determine if it's
    /// missed data and reconstruct out-of-order packets based on this value
    /// plus `len`.
    pub offset: u64,

    /// Number of bytes in `data`.
    pub len: u8,

    /// TODO: What's a reasonable chunk size? Or do we want some variability
    /// here (subject to hubpack limitations or outside-of-hubpack encoding)?
    ///
    /// Another minor annoyance - serde doesn't support arbitrary array sizes
    /// and only implements up to [T; 32], so we'd need a wrapper of some kind
    /// to go higher. See https://github.com/serde-rs/serde/issues/1937
    pub data: [u8; Self::MAX_DATA_PER_PACKET],
}

impl SerialConsole {
    pub const MAX_DATA_PER_PACKET: usize = 32;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_serial_console() {
        let line = "hello world\n";
        let mut console = SerialConsole {
            component: SpComponent { id: *b"0000111122223333" },
            offset: 12345,
            len: line.len() as u8,
            data: [0xff; 32],
        };
        console.data[..line.len()].copy_from_slice(line.as_bytes());

        let mut serialized = [0; SerialConsole::MAX_SIZE];
        let n = serialize(&mut serialized, &console).unwrap();

        let (deserialized, _) =
            deserialize::<SerialConsole>(&serialized[..n]).unwrap();
        assert_eq!(deserialized.len, console.len);
        assert_eq!(deserialized.data, console.data);
    }

    #[test]
    fn serial_console_data_length_fits_in_u8() {
        // this is just a sanity check that if we bump `MAX_DATA_PER_PACKET`
        // above 256 we also need to change the type of `SerialConsole::len`
        assert!(SerialConsole::MAX_DATA_PER_PACKET <= usize::from(u8::MAX));
    }
}
