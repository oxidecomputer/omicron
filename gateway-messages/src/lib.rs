// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![cfg_attr(all(not(test), not(feature = "std")), no_std)]

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
#[derive(Debug, Clone, SerializedSize, Serialize, Deserialize)]
pub struct Request {
    pub version: u32,
    pub request_id: u32,
    pub kind: RequestKind,
}

#[derive(Debug, Clone, SerializedSize, Serialize, Deserialize)]
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

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResponseError::RequestUnsupportedForSp => {
                write!(f, "unsupported request for this SP")
            }
            ResponseError::RequestUnsupportedForComponent => {
                write!(f, "unsupported request for this SP component")
            }
            ResponseError::IgnitionTargetDoesNotExist(target) => {
                write!(f, "nonexistent ignition target {}", target)
            }
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for ResponseError {}

/// Messages from an SP to a gateway. Includes both responses to [`Request`]s as
/// well as SP-initiated messages like serial console output.
#[derive(Debug, Clone, SerializedSize, Serialize, Deserialize)]
pub struct SpMessage {
    pub version: u32,
    pub kind: SpMessageKind,
}

#[derive(Debug, Clone, SerializedSize, Serialize, Deserialize)]
pub enum SpMessageKind {
    // TODO: Is only sending the new state sufficient?
    // IgnitionChange { target: u8, new_state: IgnitionState },
    /// Response to a [`Request`] from MGS.
    Response { request_id: u32, result: Result<ResponseKind, ResponseError> },

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

// We could derive `Copy`, but `data` is large-ish so we want callers to think
// abount cloning.
#[derive(Clone, SerializedSize)]
pub struct SerialConsole {
    /// Source component with an attached serial console.
    pub component: SpComponent,

    /// Offset of this chunk of data relative to all console data this
    /// source has sent since it booted. The receiver can determine if it's
    /// missed data and reconstruct out-of-order packets based on this value
    /// plus `len`.
    pub offset: u64,

    /// Number of bytes in `data`.
    pub len: u16,

    /// Actual serial console data.
    pub data: [u8; Self::MAX_DATA_PER_PACKET],
}

impl fmt::Debug for SerialConsole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("SerialConsole");
        debug.field("component", &self.component);
        debug.field("offset", &self.offset);
        debug.field("len", &self.len);
        let data = &self.data
            [..usize::min(usize::from(self.len), Self::MAX_DATA_PER_PACKET)];
        if let Ok(s) = str::from_utf8(data) {
            debug.field("data", &s);
        } else {
            debug.field("data", &data);
        }
        debug.finish()
    }
}

impl SerialConsole {
    /// TODO: What do we want our max size to be? We only actually encode the
    /// amount of data present, so this determines the max packet size and the
    /// size of `SerialConsole` in memory.
    ///
    /// Note: This does not include the header overhead! If we want to know the
    /// exact max packet size, we should derive a value here that includes the
    /// header size (`sizeof(SpComponent) + sizeof(u64) + sizeof(u16)`).
    pub const MAX_DATA_PER_PACKET: usize = 128;
}

mod serial_console_serde {
    use super::*;
    use serde::de::{Error, Visitor};
    use serde::ser::SerializeTuple;

    #[derive(Debug, Deserialize, Serialize)]
    struct Header {
        component: SpComponent,
        offset: u64,
        len: u16,
    }

    impl Serialize for SerialConsole {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let header = Header {
                component: self.component,
                offset: self.offset,
                len: self.len,
            };

            let len = usize::from(self.len);
            let mut tup = serializer.serialize_tuple(1 + len)?;
            tup.serialize_element(&header)?;
            for b in &self.data[..len] {
                tup.serialize_element(b)?;
            }
            tup.end()
        }
    }

    impl<'de> Deserialize<'de> for SerialConsole {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct TupleVisitor;

            impl<'de> Visitor<'de> for TupleVisitor {
                type Value = SerialConsole;

                fn expecting(
                    &self,
                    formatter: &mut std::fmt::Formatter,
                ) -> std::fmt::Result {
                    write!(formatter, "a serial console packet")
                }

                fn visit_seq<A>(
                    self,
                    mut seq: A,
                ) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::SeqAccess<'de>,
                {
                    let header: Header = match seq.next_element()? {
                        Some(header) => header,
                        None => {
                            return Err(A::Error::custom(
                                "missing packet header",
                            ))
                        }
                    };
                    let mut out = SerialConsole {
                        component: header.component,
                        offset: header.offset,
                        len: header.len,
                        data: [0; SerialConsole::MAX_DATA_PER_PACKET],
                    };
                    let len = usize::from(out.len);
                    if len > SerialConsole::MAX_DATA_PER_PACKET {
                        return Err(A::Error::custom("packet length too long"));
                    }
                    for b in &mut out.data[..len] {
                        *b = match seq.next_element()? {
                            Some(b) => b,
                            None => {
                                return Err(A::Error::custom(
                                    "invalid packet length",
                                ))
                            }
                        };
                    }
                    Ok(out)
                }
            }

            deserializer
                .deserialize_tuple(1 + Self::MAX_DATA_PER_PACKET, TupleVisitor)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_serial_console() {
        let line = b"hello world\n";
        let mut console = SerialConsole {
            component: SpComponent { id: *b"0000111122223333" },
            offset: 12345,
            len: line.len() as u16,
            data: [0xff; SerialConsole::MAX_DATA_PER_PACKET],
        };
        console.data[..line.len()].copy_from_slice(line);

        let mut serialized = [0; SerialConsole::MAX_SIZE];
        let n = serialize(&mut serialized, &console).unwrap();

        // serialized size should be limited to actual line length, not
        // the size of `console.data` (`MAX_DATA_PER_PACKET`)
        assert_eq!(
            n,
            SpComponent::MAX_SIZE + u64::MAX_SIZE + u16::MAX_SIZE + line.len()
        );

        let (deserialized, _) =
            deserialize::<SerialConsole>(&serialized[..n]).unwrap();
        assert_eq!(deserialized.len, console.len);
        assert_eq!(&deserialized.data[..line.len()], line);
    }

    #[test]
    fn serial_console_data_length_fits_in_u16() {
        // this is just a sanity check that if we bump `MAX_DATA_PER_PACKET`
        // above 65535 we also need to change the type of `SerialConsole::len`
        assert!(SerialConsole::MAX_DATA_PER_PACKET <= usize::from(u16::MAX));
    }
}
