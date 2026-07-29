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
use sled_agent_measurements::{MeasurementError, MeasurementsHandle};
use sled_agent_types::sled::StartSledAgentRequest;
use slog::Logger;
use slog_error_chain::SlogInlineError;
use sprockets_tls;
use sprockets_tls::keys::SprocketsConfig;
use std::borrow::Cow;
use std::io;
use std::net::SocketAddrV6;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Debug, Error, SlogInlineError)]
pub enum SprocketsClientError {
    #[error("Could not connect to {addr}")]
    Connect {
        addr: SocketAddrV6,
        #[source]
        err: sprockets_tls::Error,
    },

    #[error("Failed serializing request")]
    Serialize(#[source] serde_json::Error),

    #[error("Failed writing request length prefix")]
    WriteLengthPrefix(#[source] io::Error),

    #[error("Failed writing request")]
    WriteRequest(#[source] io::Error),

    #[error("Failed flushing request")]
    FlushRequest(#[source] io::Error),

    #[error("Failed reading response length prefix")]
    ReadLengthPrefix(#[source] io::Error),

    #[error("Received bogus response length: {0}")]
    BadResponseLength(u32),

    #[error("Failed reading response")]
    ReadResponse(#[source] io::Error),

    #[error("Failed deserializing response")]
    Deserialize(#[source] serde_json::Error),

    #[error("Unsupported version: {0}")]
    UnsupportedVersion(u32),

    #[error("Request failed: {0}")]
    ServerFailure(String),

    #[error(
        "Bogus response from server (expected {expected} but received {received})"
    )]
    InvalidResponse { expected: &'static str, received: &'static str },
    #[error("Reference measurements error")]
    MeasurementError(#[source] MeasurementError),
}

/// A sprockets client wrapper used to connect to bootstrap agents for rack
/// initialization
pub(crate) struct SprocketsClient {
    addr: SocketAddrV6,
    log: Logger,
    sprockets_conf: SprocketsConfig,
    measurements: Arc<MeasurementsHandle>,
}

impl SprocketsClient {
    pub(crate) fn new(
        addr: SocketAddrV6,
        sprockets_conf: SprocketsConfig,
        measurements: Arc<MeasurementsHandle>,
        log: Logger,
    ) -> Self {
        Self { addr, sprockets_conf, log, measurements }
    }

    /// Start sled agent by sending an initialization request determined from
    /// RSS input. This client is on the same scrimlet as RSS, and is talking
    /// over TCP to all other bootstrap agents.
    pub(crate) async fn start_sled_agent(
        &self,
        request: &StartSledAgentRequest,
    ) -> Result<SledAgentResponse, SprocketsClientError> {
        let stream = self.connect().await?;
        Self::start_sled_agent_with_stream(stream, request).await
    }

    pub(crate) async fn start_sled_agent_with_stream(
        stream: sprockets_tls::Stream<TcpStream>,
        request: &StartSledAgentRequest,
    ) -> Result<SledAgentResponse, SprocketsClientError> {
        let request = Request::StartSledAgentRequest(Cow::Borrowed(request));
        match Self::request_response(stream, request).await? {
            Response::SledAgentResponse(response) => Ok(response),
        }
    }

    pub(crate) async fn connect(
        &self,
    ) -> Result<sprockets_tls::Stream<TcpStream>, SprocketsClientError> {
        let log =
            self.log.new(o!("component" => "BootstrapAgentSprocketsClient"));
        // Establish sprockets connection (if possible).
        // The sprockets client loads the associated root certificates at this point.
        let corpus = self
            .measurements
            .current_measurements()
            .map_err(SprocketsClientError::MeasurementError)?;

        sprockets_tls::client::Client::connect(
            self.sprockets_conf.clone(),
            self.addr,
            corpus,
            log.clone(),
        )
        .await
        .map_err(|err| SprocketsClientError::Connect { addr: self.addr, err })
    }

    async fn request_response(
        stream: sprockets_tls::Stream<TcpStream>,
        request: Request<'_>,
    ) -> Result<Response, SprocketsClientError> {
        // Bound to avoid allocating an unreasonable amount of memory from a
        // bogus length prefix from a server. We authenticate servers via
        // sprockets before allocating based on the length prefix they send, so
        // it should be fine to be a little sloppy here and just pick something
        // far larger than we ever expect to see.
        const MAX_RESPONSE_LEN: u32 = 16 << 20;

        let mut stream = Box::new(tokio::io::BufStream::new(stream));

        // Build and serialize our request.
        let envelope = RequestEnvelope { version: version::V1, request };
        let buf = serde_json::to_vec(&envelope)
            .map_err(SprocketsClientError::Serialize)?;
        let request_length = u32::try_from(buf.len())
            .expect("serialized bootstrap-agent request length overflowed u32");

        // Write our request with a length prefix.
        stream
            .write_u32(request_length)
            .await
            .map_err(SprocketsClientError::WriteLengthPrefix)?;
        stream
            .write_all(&buf)
            .await
            .map_err(SprocketsClientError::WriteRequest)?;
        stream.flush().await.map_err(SprocketsClientError::FlushRequest)?;

        // Read the response, length prefix first.
        let response_length = stream
            .read_u32()
            .await
            .map_err(SprocketsClientError::ReadLengthPrefix)?;
        // Sanity check / guard against malformed lengths
        if response_length > MAX_RESPONSE_LEN {
            return Err(SprocketsClientError::BadResponseLength(
                response_length,
            ));
        }

        let mut buf = vec![0; response_length as usize];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(SprocketsClientError::ReadResponse)?;

        // Deserialize and handle the response.
        let envelope: ResponseEnvelope = serde_json::from_slice(&buf)
            .map_err(SprocketsClientError::Deserialize)?;

        match envelope.version {
            version::V1 => (),
            other => {
                return Err(SprocketsClientError::UnsupportedVersion(other));
            }
        }

        envelope.response.map_err(SprocketsClientError::ServerFailure)
    }
}
