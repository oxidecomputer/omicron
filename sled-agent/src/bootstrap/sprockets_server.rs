// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Server for sprockets-secured requests over the bootstrap network.

use crate::bootstrap::params::version;
use crate::bootstrap::params::Request;
use crate::bootstrap::params::RequestEnvelope;
use crate::bootstrap::views::Response;
use crate::bootstrap::views::ResponseEnvelope;
use slog::Logger;
use std::io;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufStream;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use super::agent::Agent;

pub(super) struct SprocketsServer {
    listener: TcpListener,
    bootstrap_agent: Arc<Agent>,
    log: Logger,
}

impl SprocketsServer {
    pub(super) async fn bind(
        bootstrap_agent: Arc<Agent>,
        base_log: &Logger,
    ) -> io::Result<Self> {
        let bind_addr = bootstrap_agent.rack_init_address();
        let listener = TcpListener::bind(bind_addr).await?;
        let log =
            base_log.new(o!("component" => "BootstrapAgentSprocketsServer"));
        info!(log, "Started listening"; "local_addr" => %bind_addr);
        Ok(Self { listener, bootstrap_agent, log })
    }

    /// Run the sprockets server.
    ///
    /// This method should be `tokio::spawn()`'d. It can safely be cancelled
    /// to shut it down: its only `.await` point is on
    /// `TcpListener::accept()`, which is cancel-safe. Note that cancelling this
    /// server does not necessarily cancel any outstanding requests that it has
    /// already received (and which may still be executing).
    pub(super) async fn run(self) {
        loop {
            let (stream, remote_addr) = match self.listener.accept().await {
                Ok(conn) => conn,
                Err(err) => {
                    error!(self.log, "accept() failed"; "err" => #%err);
                    continue;
                }
            };

            let log = self.log.new(o!("remote_addr" => remote_addr));
            info!(log, "Accepted connection");

            let bootstrap_agent = self.bootstrap_agent.clone();
            tokio::spawn(async move {
                match handle_start_sled_agent_request(
                    stream,
                    bootstrap_agent,
                    &log,
                )
                .await
                {
                    Ok(()) => info!(log, "Connection closed"),
                    Err(err) => warn!(log, "Connection failed"; "err" => err),
                }
            });
        }
    }
}

async fn handle_start_sled_agent_request(
    stream: TcpStream,
    bootstrap_agent: Arc<Agent>,
    log: &Logger,
) -> Result<(), String> {
    let mut stream = Box::new(BufStream::new(stream));

    let response = match read_request(&mut stream).await? {
        Request::StartSledAgentRequest(request) => {
            // The call to `request_sled_agent` should be idempotent if the
            // request was the same.
            bootstrap_agent
                .request_sled_agent(&request)
                .await
                .map(|response| Response::SledAgentResponse(response))
                .map_err(|err| {
                    warn!(
                        log, "Request to initialize sled agent failed";
                        "request" => ?request,
                        "err" => %err,
                    );
                    format!("Failed to initialize sled agent: {err}")
                })
        }
    };

    write_response(&mut stream, response).await
}

async fn read_request(
    stream: &mut Box<BufStream<TcpStream>>,
) -> Result<Request<'static>, String> {
    // Bound to avoid allocating an unreasonable amount of memory from a bogus
    // length prefix from a client. We authenticate clients via sprockets before
    // allocating based on the length prefix they send, so it should be fine to
    // be a little sloppy here and just pick something far larger than we ever
    // expect to see.
    const MAX_REQUEST_LEN: u32 = 128 << 20;

    // Read request, length prefix first.
    let request_length = stream
        .read_u32()
        .await
        .map_err(|err| format!("Failed to read length prefix: {err}"))?;

    // Sanity check / guard against malformed lengths
    if request_length > MAX_REQUEST_LEN {
        return Err(format!(
            "Rejecting incoming message with enormous length {request_length}"
        ));
    }

    let mut buf = vec![0; request_length as usize];
    stream.read_exact(&mut buf).await.map_err(|err| {
        format!("Failed to read message of length {request_length}: {err}")
    })?;

    // Deserialize request.
    let envelope: RequestEnvelope<'static> = serde_json::from_slice(&buf)
        .map_err(|err| {
            format!("Failed to deserialize request envelope: {err}")
        })?;

    // Currently we only have one version, so there's nothing to do in this
    // match, but we leave it here as a breadcrumb for future changes.
    match envelope.version {
        version::V1 => (),
        other => return Err(format!("Unsupported version: {other}")),
    }

    Ok(envelope.request)
}

async fn write_response(
    stream: &mut Box<BufStream<TcpStream>>,
    response: Result<Response, String>,
) -> Result<(), String> {
    // Build and serialize response.
    let envelope = ResponseEnvelope { version: version::V1, response };
    let buf = serde_json::to_vec(&envelope)
        .map_err(|err| format!("Failed to serialize response: {err}"))?;

    // Write response, length prefix first.
    let response_length = u32::try_from(buf.len())
        .expect("serialized bootstrap-agent response length overflowed u32");

    stream.write_u32(response_length).await.map_err(|err| {
        format!("Failed to write response length prefix: {err}")
    })?;
    stream
        .write_all(&buf)
        .await
        .map_err(|err| format!("Failed to write response body: {err}"))?;
    stream
        .flush()
        .await
        .map_err(|err| format!("Failed to flush response body: {err}"))?;

    Ok(())
}
