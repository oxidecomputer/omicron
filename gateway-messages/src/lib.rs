// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![cfg_attr(all(not(test), not(feature = "std")), no_std)]

pub mod sp_impl;

use bitflags::bitflags;
use core::fmt;
use core::mem;
use core::str;
use serde::Deserialize;
use serde::Serialize;
use serde_repr::Deserialize_repr;
use serde_repr::Serialize_repr;
use static_assertions::const_assert;

pub use hubpack::error::Error as HubpackError;
pub use hubpack::{deserialize, serialize, SerializedSize};

/// Maximum size in bytes for a serialized message.
pub const MAX_SERIALIZED_SIZE: usize = 1024;

pub mod version {
    pub const V1: u32 = 1;
}

// TODO: Ignition messages have a `target` for identification; what do the
// other messages need?

/// Messages from a gateway to an SP.
#[derive(
    Debug, Clone, SerializedSize, Serialize, Deserialize, PartialEq, Eq,
)]
pub struct Request {
    pub version: u32,
    pub request_id: u32,
    pub kind: RequestKind,
}

#[derive(
    Debug, Clone, SerializedSize, Serialize, Deserialize, PartialEq, Eq,
)]
pub enum RequestKind {
    Discover,
    // TODO do we want to be able to request IgnitionState for all targets in
    // one message?
    IgnitionState {
        target: u8,
    },
    BulkIgnitionState,
    IgnitionCommand {
        target: u8,
        command: IgnitionCommand,
    },
    SpState,
    SerialConsoleAttach(SpComponent),
    /// `SerialConsoleWrite` always includes trailing raw data.
    SerialConsoleWrite {
        /// Offset of the first byte of this packet, starting from 0 when this
        /// serial console session was attached.
        offset: u64,
    },
    SerialConsoleDetach,
    UpdateStart(UpdateStart),
    /// `UpdateChunk` always includes trailing raw data.
    UpdateChunk(UpdateChunk),
    SysResetPrepare,
    SysResetTrigger,
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

#[derive(Debug, Clone, SerializedSize, Serialize, Deserialize)]
pub enum ResponseKind {
    Discover(DiscoverResponse),
    IgnitionState(IgnitionState),
    BulkIgnitionState(BulkIgnitionState),
    IgnitionCommandAck,
    SpState(SpState),
    UpdateStartAck,
    UpdateChunkAck,
    SerialConsoleAttachAck,
    SerialConsoleWriteAck { furthest_ingested_offset: u64 },
    SerialConsoleDetachAck,
    SysResetPrepareAck,
    // There is intentionally no `SysResetTriggerAck` response; the expected
    // "resposne" to `SysResetTrigger` is an SP reset, which won't allow for
    // acks to be sent.
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
    pub version: u32,
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
    /// Cannot write to the serial console because it is not attached.
    SerialConsoleNotAttached,
    /// Cannot attach to the serial console because another MGS instance is
    /// already attached.
    SerialConsoleAlreadyAttached,
    /// An update is already in progress with the specified amount of data
    /// already provided. MGS should resume the update at that offset.
    UpdateInProgress { bytes_received: u32 },
    /// Received an invalid update chunk; the in-progress update must be
    /// abandoned and restarted.
    InvalidUpdateChunk,
    /// An update operation failed with the associated code.
    UpdateFailed(u32),
    /// Received a `SysResetTrigger` request without first receiving a
    /// `SysResetPrepare` request. This can be used to detect a successful
    /// reset.
    SysResetTriggerWithoutPrepare,
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
            ResponseError::SerialConsoleNotAttached => {
                write!(f, "serial console is not attached")
            }
            ResponseError::SerialConsoleAlreadyAttached => {
                write!(f, "serial console already attached")
            }
            ResponseError::UpdateInProgress { bytes_received } => {
                write!(f, "update still in progress ({bytes_received} bytes received so far)")
            }
            ResponseError::InvalidUpdateChunk => {
                write!(f, "invalid update chunk")
            }
            ResponseError::UpdateFailed(code) => {
                write!(f, "update failed (code {})", code)
            }
            &ResponseError::SysResetTriggerWithoutPrepare => {
                write!(f, "sys reset trigger requested without a preceding sys reset prepare")
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
    ///
    /// Note that SP -> MGS serial console messages are currently _not_
    /// acknowledged or retried; they are purely "fire and forget" from the SP's
    /// point of view. Once it sends data in a packet, it discards it from its
    /// local buffer.
    SerialConsole {
        component: SpComponent,
        /// Offset of the first byte in this packet's data starting from 0 when
        /// the serial console session was attached.
        offset: u64,
    },
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    SerializedSize,
    Serialize,
    Deserialize,
)]
pub struct UpdateStart {
    pub total_size: u32,
    // TODO auth info? checksum/digest?
    // TODO should we inline the first chunk?
}

#[derive(
    Debug, Clone, PartialEq, Eq, SerializedSize, Serialize, Deserialize,
)]
pub struct UpdateChunk {
    /// Offset in bytes of this chunk from the beginning of the update data.
    pub offset: u32,
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
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

#[derive(Debug, Clone, PartialEq, SerializedSize, Serialize, Deserialize)]
pub struct BulkIgnitionState {
    /// Ignition state for each target.
    ///
    /// TODO The ignition target is implicitly the array index; is that
    /// reasonable or should we specify target indices explicitly?
    #[serde(with = "serde_big_array::BigArray")]
    pub targets: [IgnitionState; Self::MAX_IGNITION_TARGETS],
}

impl BulkIgnitionState {
    // TODO-cleanup Is it okay to hard code this number to what we know the
    // value is for the initial rack? For now assuming yes, and any changes in
    // future products could use a different message.
    pub const MAX_IGNITION_TARGETS: usize = 36;
}

#[derive(
    Debug, Clone, Copy, SerializedSize, Serialize, Deserialize, PartialEq, Eq,
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

    /// The `sp3` host CPU.
    pub const SP3_HOST_CPU: Self =
        Self { id: *b"sp3\0\0\0\0\0\0\0\0\0\0\0\0\0" };

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

/// Sealed trait restricting the types that can be passed to
/// [`serialize_with_trailing_data()`].
pub trait GatewayMessage: SerializedSize + Serialize + private::Sealed {}
mod private {
    pub trait Sealed {}
}
impl GatewayMessage for Request {}
impl GatewayMessage for SpMessage {}
impl private::Sealed for Request {}
impl private::Sealed for SpMessage {}

// `GatewayMessage` imlementers can be followed by binary data; we want the
// majority of our packet to be available for that data. Statically check that
// our serialized message headers haven't gotten too large. The specific value
// here is arbitrary; if this check starts failing, it's probably fine to reduce
// it some. The check is here to force us to think about it.
const_assert!(MAX_SERIALIZED_SIZE - Request::MAX_SIZE > 700);
const_assert!(MAX_SERIALIZED_SIZE - SpMessage::MAX_SIZE > 700);

/// Returns `(serialized_size, data_bytes_written)` where `serialized_size` is
/// the message size written to `out` and `data_bytes_written` is the number of
/// bytes included in `out` from `data_slices`.
///
/// `data_slices` is provided as multiple slices to allow for data structures
/// like `heapless::Deque` (which presents its contents as two slices). If
/// multiple slices are present in `data_slices`, `data_bytes_written` will be
/// at most the sum of all their lengths. Bytes will be appended from the slices
/// in order.
pub fn serialize_with_trailing_data<T>(
    out: &mut [u8; MAX_SERIALIZED_SIZE],
    header: &T,
    data_slices: &[&[u8]],
) -> (usize, usize)
where
    T: GatewayMessage,
{
    // We know `T` is either `Request` or `SpMessage`, both of which we know
    // statically (confirmed by `const_assert`s above) are significantly smaller
    // than `MAX_SERIALIZED_SIZE`. They cannot fail to serialize for any reason
    // other than an undersized buffer, so we can unwrap here.
    let n = hubpack::serialize(out, header).unwrap();
    let out = &mut out[n..];

    // Split `out` into a 2-byte length prefix and the remainder of the buffer.
    let (length_prefix, mut out) = out.split_at_mut(mem::size_of::<u16>());

    let mut nwritten = 0;
    for &data in data_slices {
        // How much of this slice can we fit in `out`?
        let to_write = usize::min(out.len(), data.len());
        out[..to_write].copy_from_slice(&data[..to_write]);
        nwritten += to_write;
        out = &mut out[to_write..];
        if out.is_empty() {
            break;
        }
    }

    // Fill in the length prefix with the amount of data we copied into `out`.
    length_prefix.copy_from_slice(&(nwritten as u16).to_le_bytes());

    (n + mem::size_of::<u16>() + nwritten, nwritten)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_with_trailing_data() {
        let mut out = [0; MAX_SERIALIZED_SIZE];
        let header =
            Request { version: 1, request_id: 2, kind: RequestKind::Discover };
        let data_vecs = &[
            vec![0; 256],
            vec![1; 256],
            vec![2; 256],
            vec![3; 256],
            vec![4; 256],
            vec![5; 256],
        ];
        let data_slices =
            data_vecs.iter().map(|v| v.as_slice()).collect::<Vec<_>>();

        let (out_len, nwritten) =
            serialize_with_trailing_data(&mut out, &header, &data_slices);

        // We should have filled `out` entirely; `data_vecs` contains more data
        // than fits in `MAX_SERIALIZED_SIZE`.
        assert_eq!(out_len, MAX_SERIALIZED_SIZE);

        let (deserialized_header, remainder) =
            deserialize::<Request>(&out).unwrap();

        let remainder = sp_impl::unpack_trailing_data(remainder).unwrap();

        assert_eq!(header, deserialized_header);
        assert_eq!(remainder.len(), nwritten);

        for (i, chunk) in remainder.chunks(256).enumerate() {
            assert_eq!(chunk, &data_vecs[i][..chunk.len()]);
        }
    }
}
