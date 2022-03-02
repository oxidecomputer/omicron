// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Behavior implemented by both real and simulated SPs.

use crate::{
    version, IgnitionCommand, Request, RequestKind, ResponseKind,
    SerialConsole, SpComponent, SpMessage, SpMessageKind,
};
use hubpack::SerializedSize;

pub trait SpHandler {
    fn ping(&mut self) -> ResponseKind;

    fn ignition_state(&mut self, target: u8) -> ResponseKind;

    fn ignition_command(
        &mut self,
        target: u8,
        command: IgnitionCommand,
    ) -> ResponseKind;
}

#[derive(Debug)]
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

impl SerialConsolePackets<'_, '_> {
    /// Get the next packet of serial console data, if one exists.
    ///
    /// Returns `Some(data)` with a packet to be sent if any data remains, or
    /// `None` if all data has been previously returned.
    pub fn next_packet<'a>(
        &mut self,
        out: &'a mut [u8; SpMessage::MAX_SIZE],
    ) -> Option<&'a [u8]> {
        if self.data.is_empty() {
            return None;
        }

        let (this_packet, remaining) = self.data.split_at(usize::min(
            self.data.len(),
            SerialConsole::MAX_DATA_PER_PACKET,
        ));

        let mut message = SerialConsole {
            component: self.parent.component,
            offset: self.parent.offset,
            len: this_packet.len() as u8,
            data: [0; SerialConsole::MAX_DATA_PER_PACKET],
        };
        message.data[..this_packet.len()].copy_from_slice(this_packet);
        let message = SpMessage {
            version: version::V1,
            kind: SpMessageKind::SerialConsole(message),
        };

        // We know `out` is big enough for any `SpMessage`, so no need to bubble
        // up an error here.
        let n = match hubpack::serialize(&mut out[..], &message) {
            Ok(n) => n,
            Err(_) => panic!(),
        };

        self.data = remaining;
        self.parent.offset += this_packet.len() as u64;

        Some(&out[..n])
    }
}

#[derive(Debug)]
pub struct SpServer<Handler> {
    buf: [u8; SpMessage::MAX_SIZE],
    handler: Handler,
}

impl<Handler> SpServer<Handler>
where
    Handler: SpHandler,
{
    pub fn new(handler: Handler) -> Self {
        Self { buf: [0; SpMessage::MAX_SIZE], handler }
    }

    pub fn handler(&self) -> &Handler {
        &self.handler
    }

    pub fn handler_mut(&mut self) -> &mut Handler {
        &mut self.handler
    }

    /// Handler for incoming UDP requests.
    ///
    /// `data` should be a UDP packet that has arrived for the current SP. It
    /// will be parsed (into a [`Request`]), the appropriate method will be
    /// called on the underlying message handler, and a serialized response will
    /// be returned, which the caller should send back to the requester.
    pub fn dispatch(&mut self, data: &[u8]) -> Result<&[u8], Error> {
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
        let response_kind = match request.kind {
            RequestKind::Ping => self.handler.ping(),
            RequestKind::IgnitionState { target } => {
                self.handler.ignition_state(target)
            }
            RequestKind::IgnitionCommand { target, command } => {
                self.handler.ignition_command(target, command)
            }
        };

        // we control `SpMessage` and know all cases can successfully serialize
        // into `self.buf`
        let response = SpMessage {
            version: version::V1,
            kind: SpMessageKind::Response {
                request_id: request.request_id,
                kind: response_kind,
            },
        };
        let n = match hubpack::serialize(&mut self.buf, &response) {
            Ok(n) => n,
            Err(_) => panic!(),
        };

        // Do we want some mechanism for remembering `n` if our caller wants to
        // resend this packet, which would have to happen before calling this
        // method again? For now (and maybe forever), force them to just call us
        // again, and we'll reserialize.
        Ok(&self.buf[..n])
    }
}
