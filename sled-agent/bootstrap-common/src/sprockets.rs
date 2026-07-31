// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making requests to a Sled Agent's Bootstrap API.

use serde::{Deserialize, Serialize};
use sled_agent_measurements::{MeasurementError, MeasurementsHandle};
use sled_agent_types::sled::StartSledAgentRequest;
use slog::{Logger, o};
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
use uuid::Uuid;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Request<'a> {
    /// Send configuration information for launching a Sled Agent.
    StartSledAgentRequest(Cow<'a, StartSledAgentRequest>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequestEnvelope<'a> {
    pub version: u32,
    pub request: Request<'a>,
}

pub mod version {
    pub const V1: u32 = 1;
}

/// Describes the Sled Agent running on the device.
#[derive(Serialize, Deserialize, PartialEq)]
pub struct SledAgentResponse {
    pub id: Uuid,
}

#[derive(Serialize, Deserialize, PartialEq)]
// Note: We intentionally do not derive `Debug` on this type, to avoid
// accidentally debug-logging the secret share.
pub enum Response {
    SledAgentResponse(SledAgentResponse),
}

#[derive(Serialize, Deserialize, PartialEq)]
pub struct ResponseEnvelope {
    pub version: u32,
    pub response: Result<Response, String>,
}

/// A sprockets client wrapper used to connect to bootstrap agents for rack
/// initialization
pub struct SprocketsClient {
    addr: SocketAddrV6,
    log: Logger,
    sprockets_conf: SprocketsConfig,
    measurements: Arc<MeasurementsHandle>,
}

impl SprocketsClient {
    pub fn new(
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
    pub async fn start_sled_agent(
        &self,
        request: &StartSledAgentRequest,
    ) -> Result<SledAgentResponse, SprocketsClientError> {
        let stream = self.connect().await?;
        Self::start_sled_agent_with_stream(stream, request).await
    }

    pub async fn start_sled_agent_with_stream(
        stream: sprockets_tls::Stream<TcpStream>,
        request: &StartSledAgentRequest,
    ) -> Result<SledAgentResponse, SprocketsClientError> {
        let request = Request::StartSledAgentRequest(Cow::Borrowed(request));
        match Self::request_response(stream, request).await? {
            Response::SledAgentResponse(response) => Ok(response),
        }
    }

    pub async fn connect(
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

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use omicron_common::address::Ipv6Subnet;
    use omicron_uuid_kinds::RackUuid;
    use omicron_uuid_kinds::SledUuid;
    use sled_agent_types::sled::StartSledAgentRequestBody;

    use super::*;

    #[test]
    fn json_serialization_round_trips() {
        let envelope = RequestEnvelope {
            version: 1,
            request: Request::StartSledAgentRequest(Cow::Owned(
                StartSledAgentRequest {
                    generation: 0,
                    schema_version: 1,
                    body: StartSledAgentRequestBody {
                        id: SledUuid::new_v4(),
                        rack_id: RackUuid::new_v4(),
                        use_trust_quorum: false,
                        is_lrtq_learner: false,
                        subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
                    },
                },
            )),
        };

        let serialized = serde_json::to_vec(&envelope).unwrap();
        let deserialized: RequestEnvelope =
            serde_json::from_slice(serialized.as_slice()).unwrap();

        assert!(envelope == deserialized, "serialization round trip failed");
    }
}
