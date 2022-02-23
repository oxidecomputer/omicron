// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod sp_impl;

use bitflags::bitflags;
use serde::{Deserialize, Serialize};

pub use hubpack::{deserialize, serialize, SerializedSize};
pub use hubpack::error::Error as HubpackError;

// TODO: Ignition messages have a `target: u8` for identification; what do the
// other messages need?

/// Messages from a gateway to an SP.
#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub struct Request {
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
}

/// Messages from an SP to a gateway, specifically responding to a [Request].
#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub struct Response {
    pub request_id: u32,
    pub kind: ResponseKind,
}

// TODO: Not all SPs are capable of crafting all these response kinds, but the
// way we're using hubpack requires everyone to allocate Response::MAX_SIZE. Is
// that okay, or should we break this up more?
#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub enum ResponseKind {
    Pong,
    // TODO repeat `target` for these, or is our `request_id` sufficient?
    IgnitionState(IgnitionState),
    IgnitionCommandAck,
    Error(ResponseError),
}

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub enum ResponseError {
    /// The [RequestKind] is not supported by the receiving SP; e.g., asking an
    /// SP without an attached ignition controller for ignition state.
    RequestUnsupported,
}

/// Messages from an SP to a gateway, prompted by the SP itself; e.g., ignition
/// state change or serial console output.
#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub struct SpMessage {
    // TODO: expections on this field? E.g. for serial console, does it imply
    // ordering, or can we live with "wait to send more console data until we
    // get an ack" with just a single outstanding message?
    pub msg_id: u32,
    pub kind: SpMessageKind,
}

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub enum SpMessageKind {
    // TODO: Is only sending the new state sufficient?
    IgnitionChange { target: u8, new_state: IgnitionState },
    SerialConsole(SerialConsole),
}

/// Acks send from a gateway to an SP in response to an [SpMessage].
// TODO: Is this the right way to handle serial console streaming? SP sends
// `SerialConsole` with a chunk of data, waits for an ack, then sends the next?
// How to handle overflow on the SP if acks are slow or nonexistant?
#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub struct SpMessageAck {
    pub msg_id: u32,
}

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
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
    // Nop
}

#[derive(
    Debug, Clone, Copy, Default, SerializedSize, Serialize, Deserialize,
)]
pub struct SerialConsole {
    len: u8,
    // TODO: What's a reasonable chunk size? Or do we want some variability
    // here (subject to hubpack limitations or outside-of-hubpack encoding)?
    //
    // Another minor annoyance - serde doesn't support arbitrary array sizes,
    // and only implements up to [T; 32], so we'd need a wrapper of some kind to
    // go higher. See https://github.com/serde-rs/serde/issues/1937
    data: [u8; 32],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_serial_console() {
        let line = "hello world\n";
        let mut console = SerialConsole::default();
        console.len = line.len() as u8;
        console.data[..line.len()].copy_from_slice(line.as_bytes());

        let mut serialized = [0; SerialConsole::MAX_SIZE];
        let n = serialize(&mut serialized, &console).unwrap();

        let (deserialized, _) =
            deserialize::<SerialConsole>(&serialized[..n]).unwrap();
        assert_eq!(deserialized.len, console.len);
        assert_eq!(deserialized.data, console.data);
    }
}
