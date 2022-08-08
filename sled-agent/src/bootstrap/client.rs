// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to a Sled Agent's Bootstrap API.

use super::params::version;
use super::params::Request;
use super::params::RequestEnvelope;
use super::params::SledAgentRequest;
use super::trust_quorum::ShareDistribution;
use super::views::SledAgentResponse;
use crate::bootstrap::views::Response;
use crate::bootstrap::views::ResponseEnvelope;
use crate::sp::SpHandle;
use crate::sp::SprocketsRole;
use slog::Logger;
use sprockets_host::Ed25519Certificate;
use std::borrow::Cow;
use std::io;
use std::net::SocketAddrV6;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use vsss_rs::Share;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Could not connect to {addr}: {err}")]
    Connect { addr: SocketAddrV6, err: io::Error },

    #[error("Could not establish sprockets session: {0}")]
    SprocketsSession(String),

    #[error("Failed serializing request: {0}")]
    Serialize(serde_json::Error),

    #[error("Failed writing request length prefix: {0}")]
    WriteLengthPrefix(io::Error),

    #[error("Failed writing request: {0}")]
    WriteRequest(io::Error),

    #[error("Failed flushing request: {0}")]
    FlushRequest(io::Error),

    #[error("Failed reading response length prefix: {0}")]
    ReadLengthPrefix(io::Error),

    #[error("Received bogus response length: {0}")]
    BadResponseLength(u32),

    #[error("Failed reading response: {0}")]
    ReadResponse(io::Error),

    #[error("Failed deserializing response: {0}")]
    Deserialize(serde_json::Error),

    #[error("Unsupported version: {0}")]
    UnsupportedVersion(u32),

    #[error("Request failed: {0}")]
    ServerFailure(String),

    #[error("Bogus response from server (expected {expected} but received {received})")]
    InvalidResponse { expected: &'static str, received: &'static str },
}

pub(crate) struct Client<'a> {
    addr: SocketAddrV6,
    sp: &'a Option<SpHandle>,
    trust_quorum_members: &'a [Ed25519Certificate],
    log: Logger,
}

impl<'a> Client<'a> {
    pub(crate) fn new(
        addr: SocketAddrV6,
        sp: &'a Option<SpHandle>,
        trust_quorum_members: &'a [Ed25519Certificate],
        log: Logger,
    ) -> Self {
        Self { addr, sp, trust_quorum_members, log }
    }

    pub(crate) fn addr(&self) -> SocketAddrV6 {
        self.addr
    }

    pub(crate) async fn start_sled(
        &self,
        request: &SledAgentRequest,
        trust_quorum_share: Option<ShareDistribution>,
    ) -> Result<SledAgentResponse, Error> {
        let request = Request::SledAgentRequest(
            Cow::Borrowed(request),
            trust_quorum_share.map(Into::into),
        );

        match self.request_response(request).await? {
            Response::SledAgentResponse(response) => Ok(response),
            Response::ShareResponse(_) => Err(Error::InvalidResponse {
                expected: "SledAgentResponse",
                received: "ShareResponse",
            }),
        }
    }

    pub(crate) async fn request_share(&self) -> Result<Share, Error> {
        let request = Request::ShareRequest;

        match self.request_response(request).await? {
            Response::ShareResponse(response) => Ok(response),
            Response::SledAgentResponse(_) => Err(Error::InvalidResponse {
                expected: "ShareResponse",
                received: "SledAgentResponse",
            }),
        }
    }

    async fn request_response(
        &self,
        request: Request<'_>,
    ) -> Result<Response, Error> {
        // Bound to avoid allocating an unreasonable amount of memory from a
        // bogus length prefix from a server. We authenticate servers via
        // sprockets before allocating based on the length prefix they send, so
        // it should be fine to be a little sloppy here and just pick something
        // far larger than we ever expect to see.
        const MAX_RESPONSE_LEN: u32 = 16 << 20;

        // Establish connection and sprockets connection (if possible).
        let stream = TcpStream::connect(self.addr)
            .await
            .map_err(|err| Error::Connect { addr: self.addr, err })?;

        let mut stream = crate::sp::maybe_wrap_stream(
            stream,
            self.sp,
            SprocketsRole::Client,
            Some(self.trust_quorum_members),
            &self.log,
        )
        .await
        .map_err(|err| Error::SprocketsSession(err.to_string()))?;

        // Build and serialize our request.
        let envelope = RequestEnvelope { version: version::V1, request };

        // "danger" note: `buf` contains a raw trust quorum share; we must not
        // log or otherwise persist it! We only write it to `stream`.
        let buf =
            envelope.danger_serialize_as_json().map_err(Error::Serialize)?;
        let request_length = u32::try_from(buf.len())
            .expect("serialized bootstrap-agent request length overflowed u32");

        // Write our request with a length prefix.
        stream
            .write_u32(request_length)
            .await
            .map_err(Error::WriteLengthPrefix)?;
        stream.write_all(&buf).await.map_err(Error::WriteRequest)?;
        stream.flush().await.map_err(Error::FlushRequest)?;

        // Read the response, length prefix first.
        let response_length =
            stream.read_u32().await.map_err(Error::ReadLengthPrefix)?;
        // Sanity check / guard against malformed lengths
        if response_length > MAX_RESPONSE_LEN {
            return Err(Error::BadResponseLength(response_length));
        }

        let mut buf = vec![0; response_length as usize];
        stream.read_exact(&mut buf).await.map_err(Error::ReadResponse)?;

        // Deserialize and handle the response.
        let envelope: ResponseEnvelope =
            serde_json::from_slice(&buf).map_err(Error::Deserialize)?;

        match envelope.version {
            version::V1 => (),
            other => return Err(Error::UnsupportedVersion(other)),
        }

        envelope.response.map_err(Error::ServerFailure)
    }
}
