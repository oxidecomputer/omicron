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
use crate::SerialConsole;
use crate::SpComponent;
use crate::SpMessage;
use crate::SpMessageKind;
use crate::SpPort;
use crate::SpState;
use crate::UpdateChunk;
use crate::UpdateStart;
use core::convert::Infallible;
use hubpack::SerializedSize;

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

    fn update_start(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        update: UpdateStart,
    ) -> Result<(), ResponseError>;

    fn update_chunk(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        chunk: UpdateChunk,
    ) -> Result<(), ResponseError>;

    // TODO Should we return "number of bytes written" here, or is it sufficient
    // to say "all or none"? Would be nice for the caller to not have to resend
    // UDP chunks; can SP ensure it writes all data locally?
    fn serial_console_write(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
        packet: SerialConsole,
    ) -> Result<(), ResponseError>;

    fn sys_reset_prepare(
        &mut self,
        sender: SocketAddrV6,
        port: SpPort,
    ) -> Result<(), ResponseError>;

    // On success, this method cannot return (it should perform a reset).
    fn sys_reset_trigger(
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

#[derive(Debug)]
pub struct SerialConsolePacketizer {
    component: SpComponent,
    offset: u64,
}

impl SerialConsolePacketizer {
    pub fn new(component: SpComponent) -> Self {
        Self { component, offset: 0 }
    }

    pub fn packetize<'a, 'b>(
        &'a mut self,
        data: &'b [u8],
    ) -> SerialConsolePackets<'a, 'b> {
        SerialConsolePackets { parent: self, data }
    }

    /// Extract the first packet from `data`, returning that packet and any
    /// remaining data (which may be empty).
    ///
    /// Panics if `data` is empty.
    pub fn first_packet<'a>(
        &mut self,
        data: &'a [u8],
    ) -> (SerialConsole, &'a [u8]) {
        if data.is_empty() {
            panic!();
        }

        let (this_packet, remaining) = data.split_at(usize::min(
            data.len(),
            SerialConsole::MAX_DATA_PER_PACKET,
        ));

        let mut packet = SerialConsole {
            component: self.component,
            offset: self.offset,
            len: this_packet.len() as u16,
            data: [0; SerialConsole::MAX_DATA_PER_PACKET],
        };
        packet.data[..this_packet.len()].copy_from_slice(this_packet);

        self.offset += this_packet.len() as u64;

        (packet, remaining)
    }

    // TODO this function exists only to allow callers to inject artifical gaps
    // in the data they're sending; should we gate it behind a cargo feature?
    pub fn danger_emulate_dropped_packets(&mut self, bytes_to_skip: u64) {
        self.offset += bytes_to_skip;
    }
}

#[derive(Debug)]
pub struct SerialConsolePackets<'a, 'b> {
    parent: &'a mut SerialConsolePacketizer,
    data: &'b [u8],
}

impl Iterator for SerialConsolePackets<'_, '_> {
    type Item = SerialConsole;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }

        let (packet, remaining) = self.parent.first_packet(self.data);
        self.data = remaining;

        Some(packet)
    }
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
    out: &mut [u8; SpMessage::MAX_SIZE],
) -> Result<usize, Error> {
    // parse request, with sanity checks on sizes
    if data.len() > Request::MAX_SIZE {
        return Err(Error::DataTooLarge);
    }
    let (request, leftover) = hubpack::deserialize::<Request>(data)?;
    if !leftover.is_empty() {
        return Err(Error::LeftoverData);
    }

    // `version` is intentionally the first 4 bytes of the packet; we could
    // check it before trying to deserialize?
    if request.version != version::V1 {
        return Err(Error::UnsupportedVersion(request.version));
    }

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
        RequestKind::UpdateStart(update) => handler
            .update_start(sender, port, update)
            .map(|()| ResponseKind::UpdateStartAck),
        RequestKind::UpdateChunk(chunk) => handler
            .update_chunk(sender, port, chunk)
            .map(|()| ResponseKind::UpdateChunkAck),
        RequestKind::SerialConsoleWrite(packet) => handler
            .serial_console_write(sender, port, packet)
            .map(|()| ResponseKind::SerialConsoleWriteAck),
        RequestKind::SysResetPrepare => handler
            .sys_reset_prepare(sender, port)
            .map(|()| ResponseKind::SysResetPrepareAck),
        RequestKind::SysResetTrigger => {
            handler.sys_reset_trigger(sender, port).map(|infallible| {
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
