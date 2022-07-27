// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![cfg_attr(all(not(test), not(feature = "std")), no_std)]

pub mod sp_impl;
mod variable_packet;

use bitflags::bitflags;
use core::fmt;
use core::str;
use serde::Deserialize;
use serde::Serialize;
use serde_repr::Deserialize_repr;
use serde_repr::Serialize_repr;

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
    Discover,
    // TODO do we want to be able to request IgnitionState for all targets in
    // one message?
    IgnitionState { target: u8 },
    BulkIgnitionState,
    IgnitionCommand { target: u8, command: IgnitionCommand },
    SpState,
    SerialConsoleWrite(SerialConsole),
}

/// Identifier for one of of an SP's KSZ8463 management-network-facing ports.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize_repr,
    Deserialize_repr,
    SerializedSize,
)]
#[repr(u8)]
pub enum SpPort {
    One = 1,
    Two = 2,
}

// TODO: Not all SPs are capable of crafting all these response kinds, but the
// way we're using hubpack requires everyone to allocate Response::MAX_SIZE. Is
// that okay, or should we break this up more?
#[derive(Debug, Clone, SerializedSize, Serialize, Deserialize)]
pub enum ResponseKind {
    Discover(DiscoverResponse),
    IgnitionState(IgnitionState),
    BulkIgnitionState(BulkIgnitionState),
    IgnitionCommandAck,
    SpState(SpState),
    SerialConsoleWriteAck,
}

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub struct DiscoverResponse {
    /// Which SP port received the `Discover` request.
    pub sp_port: SpPort,
}

// TODO how is this reported? Same/different for components?
pub type SerialNumber = [u8; 16];

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub struct SpState {
    pub serial_number: SerialNumber,
}

#[derive(Debug, Clone, Copy, SerializedSize, Serialize, Deserialize)]
pub enum ResponseError {
    /// The SP is busy; retry the request mometarily.
    ///
    /// E.g., the request requires communicating on a USART whose FIFO is
    /// currently full.
    Busy,
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
            ResponseError::Busy => {
                write!(f, "SP busy")
            }
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
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    SerializedSize,
    Serialize,
    Deserialize,
)]
pub struct IgnitionState {
    pub id: u16,
    pub flags: IgnitionFlags,
}

impl IgnitionState {
    pub fn is_powered_on(self) -> bool {
        self.flags.intersects(IgnitionFlags::POWER)
    }
}

bitflags! {
    #[derive(Default, SerializedSize, Serialize, Deserialize)]
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

#[derive(Clone, PartialEq, SerializedSize)]
pub struct BulkIgnitionState {
    /// Number of ignition targets present in `targets`.
    pub num_targets: u16,
    /// Ignition state for each target.
    ///
    /// TODO The ignition target is implicitly the array index; is that
    /// reasonable or should we specify target indices explicitly?
    pub targets: [IgnitionState; Self::MAX_IGNITION_TARGETS],
}

impl fmt::Debug for BulkIgnitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("BulkIgnitionState");
        debug.field("num_targets", &self.num_targets);
        let targets = &self.targets[..usize::from(self.num_targets)];
        debug.field("targets", &targets);
        debug.finish()
    }
}

impl BulkIgnitionState {
    // TODO We need to decide how to set max sizes for packets that may contain
    // a variable amount of data. There are (at least) three concerns:
    //
    // 1. It determines a max packet size; we need to make sure this stays under
    //    whatever limit is in place on the management network.
    // 2. It determines the size of the relevant structs/enums (and
    //    corresponding serialization/deserialization buffers). This is almost
    //    certainly irrelevant for MGS, but is very relevant for SPs.
    // 3. What are the implications on versioning of changing the size? It
    //    doesn't actually affect the packet format on the wire, but a receiver
    //    with a lower compiled-in max size will reject packets it receives with
    //    more data than its max size.
    //
    // plus one note: these max sizes do not include the header overhead for the
    // packets; that needs to be accounted for (particularly for point 1 above).
    //
    // Another question specific to `BulkIgnitionState`: Will we always send
    // "max number of targets in the rack" states, even if some slots are
    // unpopulated? Maybe this message shouldn't be variable at all. For now we
    // leave it like it is; it's certainly "variable" in the sense that our
    // simulated racks for tests have fewer than 36 targets.
    pub const MAX_IGNITION_TARGETS: usize = 36;
}

mod bulk_ignition_state_serde {
    use super::variable_packet::VariablePacket;
    use super::*;

    #[derive(Debug, Deserialize, Serialize)]
    pub(crate) struct Header {
        num_targets: u16,
    }

    impl VariablePacket for BulkIgnitionState {
        type Header = Header;
        type Element = IgnitionState;

        const MAX_ELEMENTS: usize = Self::MAX_IGNITION_TARGETS;
        const DESERIALIZE_NAME: &'static str = "bulk ignition state packet";

        fn header(&self) -> Self::Header {
            Header { num_targets: self.num_targets }
        }

        fn num_elements(&self) -> u16 {
            self.num_targets
        }

        fn elements(&self) -> &[Self::Element] {
            &self.targets
        }

        fn elements_mut(&mut self) -> &mut [Self::Element] {
            &mut self.targets
        }

        fn from_header(header: Self::Header) -> Self {
            Self {
                num_targets: header.num_targets,
                targets: [IgnitionState::default();
                    BulkIgnitionState::MAX_IGNITION_TARGETS],
            }
        }
    }

    impl Serialize for BulkIgnitionState {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            VariablePacket::serialize(self, serializer)
        }
    }

    impl<'de> Deserialize<'de> for BulkIgnitionState {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            VariablePacket::deserialize(deserializer)
        }
    }
}

#[derive(
    Debug, Clone, Copy, SerializedSize, Serialize, Deserialize, PartialEq,
)]
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
#[derive(Debug)]
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
        let data = &self.data[..usize::from(self.len)];
        if let Ok(s) = str::from_utf8(data) {
            debug.field("data", &s);
        } else {
            debug.field("data", &data);
        }
        debug.finish()
    }
}

impl SerialConsole {
    // TODO: See discussion on `BulkIgnitionState::MAX_IGNITION_TARGETS` for
    // concerns about setting this limit.
    //
    // A concern specific to `SerialConsole`: What should we do (if anything) to
    // account for something like "user `cat`s a large file, which is now
    // streaming across the management network"? A couple possibilities:
    //
    // 1. One packet per line, and truncate any lines longer than
    //    `MAX_DATA_PER_PACKET` (seems like this could be _very_ annoying if a
    //    user bumped into it without realizing it).
    // 2. Rate limiting (enforced where?)
    pub const MAX_DATA_PER_PACKET: usize = 128;
}

mod serial_console_serde {
    use super::variable_packet::VariablePacket;
    use super::*;

    #[derive(Debug, Deserialize, Serialize)]
    pub(crate) struct Header {
        component: SpComponent,
        offset: u64,
        len: u16,
    }

    impl VariablePacket for SerialConsole {
        type Header = Header;
        type Element = u8;

        const MAX_ELEMENTS: usize = Self::MAX_DATA_PER_PACKET;
        const DESERIALIZE_NAME: &'static str = "serial console packet";

        fn header(&self) -> Self::Header {
            Header {
                component: self.component,
                offset: self.offset,
                len: self.len,
            }
        }

        fn num_elements(&self) -> u16 {
            self.len
        }

        fn elements(&self) -> &[Self::Element] {
            &self.data
        }

        fn elements_mut(&mut self) -> &mut [Self::Element] {
            &mut self.data
        }

        fn from_header(header: Self::Header) -> Self {
            Self {
                component: header.component,
                offset: header.offset,
                len: header.len,
                data: [0; Self::MAX_DATA_PER_PACKET],
            }
        }
    }

    impl Serialize for SerialConsole {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            VariablePacket::serialize(self, serializer)
        }
    }

    impl<'de> Deserialize<'de> for SerialConsole {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            VariablePacket::deserialize(deserializer)
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
