// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Behavior implemented by both real and simulated SPs.

use crate::version;
use crate::BulkIgnitionState;
use crate::DiscoverResponse;
use crate::IgnitionCommand;
use crate::IgnitionState;
use crate::Request;
use crate::RequestKind;
use crate::ResponseError;
use crate::ResponseKind;
use crate::SpComponent;
use crate::SpMessage;
use crate::SpMessageKind;
use crate::SpPort;
use crate::SpState;
use crate::UpdateChunk;
use crate::UpdateId;
use crate::UpdatePrepare;
use crate::UpdateStatus;
use core::convert::Infallible;
use core::mem;

#[cfg(feature = "std")]
use std::net::SocketAddrV6;

#[cfg(not(feature = "std"))]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct SocketAddrV6 {
    pub ip: smoltcp::wire::Ipv6Address,
    pub port: u16,
}

pub trait SpHandler {
    fn discover(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<DiscoverResponse, ResponseError>;

    fn ignition_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: u8,
    ) -> Result<IgnitionState, ResponseError>;

    fn bulk_ignition_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<BulkIgnitionState, ResponseError>;

    fn ignition_command(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        target: u8,
        command: IgnitionCommand,
    ) -> Result<(), ResponseError>;

    fn sp_state(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<SpState, ResponseError>;

    fn update_prepare(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        update: UpdatePrepare,
    ) -> Result<(), ResponseError>;

    fn update_chunk(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        chunk: UpdateChunk,
        data: &[u8],
    ) -> Result<(), ResponseError>;

    fn update_status(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
    ) -> Result<Option<UpdateStatus>, ResponseError>;

    fn update_abort(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
        id: UpdateId,
    ) -> Result<(), ResponseError>;

    fn serial_console_attach(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        component: SpComponent,
    ) -> Result<(), ResponseError>;

    /// The returned u64 should be the offset we want to receive in the next
    /// call to `serial_console_write()`; i.e., the furthest offset we've
    /// ingested (either by writing to the console or by buffering to write it).
    fn serial_console_write(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        offset: u64,
        data: &[u8],
    ) -> Result<u64, ResponseError>;

    fn serial_console_detach(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), ResponseError>;

    fn reset_prepare(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), ResponseError>;

    // On success, this method cannot return (it should perform a reset).
    fn reset_trigger(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<Infallible, ResponseError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Incoming data packet is larger than the largest [`Request`].
    DataTooLarge,
    /// Incoming data packet had leftover trailing data.
    LeftoverData,
    /// Message version is unsupported.
    UnsupportedVersion(u32),
    /// Deserializing the packet into a [`Request`] failed.
    DeserializationFailed(hubpack::error::Error),
}

impl From<hubpack::error::Error> for Error {
    fn from(err: hubpack::error::Error) -> Self {
        Self::DeserializationFailed(err)
    }
}

/// Unpack the 2-byte length-prefixed trailing data that comes after some
/// packets (e.g., update chunks, serial console).
pub fn unpack_trailing_data(data: &[u8]) -> hubpack::error::Result<&[u8]> {
    if data.len() < mem::size_of::<u16>() {
        return Err(hubpack::error::Error::Truncated);
    }
    let (prefix, data) = data.split_at(mem::size_of::<u16>());
    let len = u16::from_le_bytes([prefix[0], prefix[1]]);
    if data.len() != usize::from(len) {
        return Err(hubpack::error::Error::Invalid);
    }
    Ok(data)
}

/// Handle a single incoming message.
///
/// The incoming message is described by `sender` (the remote address of the
/// sender), `port` (the local port the message arived on), and `data` (the raw
/// message). It will be deserialized, and the appropriate method will be called
/// on `handler` to craft a response. The response will then be serialized into
/// `out`, and returned `Ok(n)` value specifies length of the serialized
/// response.
pub fn handle_message<H: SpHandler>(
    sender: SocketAddrV6,
    port: SpPort,
    data: &[u8],
    handler: &mut H,
    out: &mut [u8; crate::MAX_SERIALIZED_SIZE],
) -> Result<usize, Error> {
    // parse request, with sanity checks on sizes
    if data.len() > crate::MAX_SERIALIZED_SIZE {
        return Err(Error::DataTooLarge);
    }
    let (request, leftover) = hubpack::deserialize::<Request>(data)?;

    // `version` is intentionally the first 4 bytes of the packet; we could
    // check it before trying to deserialize?
    if request.version != version::V1 {
        return Err(Error::UnsupportedVersion(request.version));
    }

    // Do we expect any trailing raw data? Only for specific kinds of messages;
    // if we get any for other messages, bail out.
    let trailing_data = match &request.kind {
        RequestKind::UpdateChunk(_)
        | RequestKind::SerialConsoleWrite { .. } => {
            unpack_trailing_data(leftover)?
        }
        _ => {
            if !leftover.is_empty() {
                return Err(Error::LeftoverData);
            }
            &[]
        }
    };

    // call out to handler to provide response
    let result = match request.kind {
        RequestKind::Discover => {
            handler.discover(sender, port).map(ResponseKind::Discover)
        }
        RequestKind::IgnitionState { target } => handler
            .ignition_state(sender, port, target)
            .map(ResponseKind::IgnitionState),
        RequestKind::BulkIgnitionState => handler
            .bulk_ignition_state(sender, port)
            .map(ResponseKind::BulkIgnitionState),
        RequestKind::IgnitionCommand { target, command } => handler
            .ignition_command(sender, port, target, command)
            .map(|()| ResponseKind::IgnitionCommandAck),
        RequestKind::SpState => {
            handler.sp_state(sender, port).map(ResponseKind::SpState)
        }
        RequestKind::UpdatePrepare(update) => handler
            .update_prepare(sender, port, update)
            .map(|()| ResponseKind::UpdatePrepareAck),
        RequestKind::UpdateChunk(chunk) => handler
            .update_chunk(sender, port, chunk, trailing_data)
            .map(|()| ResponseKind::UpdateChunkAck),
        RequestKind::UpdateStatus(component) => handler
            .update_status(sender, port, component)
            .map(ResponseKind::UpdateStatus),
        RequestKind::UpdateAbort { component, id } => handler
            .update_abort(sender, port, component, id)
            .map(|()| ResponseKind::UpdateAbortAck),
        RequestKind::SerialConsoleAttach(component) => handler
            .serial_console_attach(sender, port, component)
            .map(|()| ResponseKind::SerialConsoleAttachAck),
        RequestKind::SerialConsoleWrite { offset } => handler
            .serial_console_write(sender, port, offset, trailing_data)
            .map(|n| ResponseKind::SerialConsoleWriteAck {
                furthest_ingested_offset: n,
            }),
        RequestKind::SerialConsoleDetach => handler
            .serial_console_detach(sender, port)
            .map(|()| ResponseKind::SerialConsoleDetachAck),
        RequestKind::SysResetPrepare => handler
            .reset_prepare(sender, port)
            .map(|()| ResponseKind::SysResetPrepareAck),
        RequestKind::SysResetTrigger => {
            handler.reset_trigger(sender, port).map(|infallible| {
                // A bit of type system magic here; `sys_reset_trigger`'s
                // success type (`Infallible`) cannot be instantiated. We can
                // provide an empty match to teach the type system that an
                // `Infallible` (which can't exist) can be converted to a
                // `ResponseKind` (or any other type!).
                match infallible {}
            })
        }
    };

    // we control `SpMessage` and know all cases can successfully serialize
    // into `self.buf`
    let response = SpMessage {
        version: version::V1,
        kind: SpMessageKind::Response {
            request_id: request.request_id,
            result,
        },
    };

    // We know `response` is well-formed and fits into `out` (since it's
    // statically sized for `SpMessage`), so we can unwrap serialization.
    let n = match hubpack::serialize(&mut out[..], &response) {
        Ok(n) => n,
        Err(_) => panic!(),
    };

    Ok(n)
}
