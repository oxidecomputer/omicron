// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to a Sled Agent's Bootstrap API.

use super::params::Request;
use super::params::RequestEnvelope;
use super::params::version;
use super::views::SledAgentResponse;
use crate::bootstrap::views::Response;
use crate::bootstrap::views::ResponseEnvelope;
use sled_agent_types::sled::StartSledAgentRequest;
use slog::Logger;
use sprockets_tls::client::Client as SprocketsClient;
use sprockets_tls::keys::SprocketsConfig;
use std::borrow::Cow;
use std::io;
use std::net::SocketAddrV6;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Could not connect to {addr}: {err}")]
    Connect { addr: SocketAddrV6, err: sprockets_tls::Error },

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

    #[error(
        "Bogus response from server (expected {expected} but received {received})"
    )]
    InvalidResponse { expected: &'static str, received: &'static str },
}

/// A TCP client used to connect to bootstrap agents for rack initialization
///
/// TODO: This will transition to a sprockets channel in the fullness of time.
/// In all likelyhood, the sprockets channels will actually be managed by
/// the bootstore which will proxy requests and resposnes as needed for the
/// bootstrap agent.
pub(crate) struct Client {
    addr: SocketAddrV6,
    log: Logger,
    sprockets_conf: SprocketsConfig,
}

impl Client {
    pub(crate) fn new(
        addr: SocketAddrV6,
        sprockets_conf: SprocketsConfig,
        log: Logger,
    ) -> Self {
        Self { addr, sprockets_conf, log }
    }

    /// Start sled agent by sending an initialization request determined from
    /// RSS input. This client is on the same scrimlet as RSS, and is talking
    /// over TCP to all other bootstrap agents.
    pub(crate) async fn start_sled_agent(
        &self,
        request: &StartSledAgentRequest,
    ) -> Result<SledAgentResponse, Error> {
        let request = Request::StartSledAgentRequest(Cow::Borrowed(request));

        match self.request_response(request).await? {
            Response::SledAgentResponse(response) => Ok(response),
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

        let log = self.log.new(o!("component" => "SledAgentSprocketsClient"));
        // Establish connection and sprockets connection (if possible).
        // The sprockets client loads the associated root certificates at this point.
        let stream = SprocketsClient::connect(
            self.sprockets_conf.clone(),
            self.addr,
            log.clone(),
        )
        .await
        .map_err(|err| Error::Connect { addr: self.addr, err })?;

        let mut stream = Box::new(tokio::io::BufStream::new(stream));

        // Build and serialize our request.
        let envelope = RequestEnvelope { version: version::V1, request };
        let buf = serde_json::to_vec(&envelope).map_err(Error::Serialize)?;
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
