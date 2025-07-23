// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Server for sprockets-secured requests over the bootstrap network.

use crate::bootstrap::params::Request;
use crate::bootstrap::params::RequestEnvelope;
use crate::bootstrap::params::version;
use crate::bootstrap::views::Response;
use crate::bootstrap::views::ResponseEnvelope;
use crate::bootstrap::views::SledAgentResponse;
use camino::Utf8PathBuf;
use sled_agent_types::sled::StartSledAgentRequest;
use slog::Logger;
use sprockets_tls::Stream;
use sprockets_tls::keys::SprocketsConfig;
use sprockets_tls::server::Server;
use std::io;
use std::net::SocketAddrV6;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufStream;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

type TxRequestsChannel = mpsc::Sender<(
    StartSledAgentRequest,
    oneshot::Sender<Result<SledAgentResponse, String>>,
)>;

pub(super) struct SprocketsServer {
    listener: Server,
    tx_requests: TxRequestsChannel,
    log: Logger,
}

impl SprocketsServer {
    pub(super) async fn bind(
        bind_addr: SocketAddrV6,
        tx_requests: TxRequestsChannel,
        sprockets_conf: SprocketsConfig,
        base_log: &Logger,
    ) -> io::Result<Self> {
        let log = base_log.new(o!("component" => "SledAgentSprocketsServer"));

        // The root certificates associated with sprockets are loaded at
        // server creation time
        let listener =
            Server::new(sprockets_conf, bind_addr, log.clone()).await.unwrap();
        info!(log, "Started listening"; "local_addr" => %bind_addr);
        Ok(Self { listener, tx_requests, log })
    }

    /// Run the sprockets server.
    ///
    /// This method should be `tokio::spawn()`'d. It can safely be cancelled
    /// to shut it down: its only `.await` point is on a sprocket listener,
    /// which is cancel-safe. Note that cancelling this
    /// server does not necessarily cancel any outstanding requests that it has
    /// already received (and which may still be executing).
    pub(super) async fn run(mut self, corpus: Vec<Utf8PathBuf>) {
        loop {
            // Sprockets actually _uses_ the key here!
            let (stream, remote_addr) =
                match self.listener.accept(&corpus).await {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!(self.log, "accept() failed"; "err" => #%err);
                        continue;
                    }
                };

            let log = self.log.new(o!("remote_addr" => remote_addr));
            info!(
                log,
                "Accepted connection from peer {:?}",
                stream.peer_platform_id()
            );

            let tx_requests = self.tx_requests.clone();
            tokio::spawn(async move {
                match handle_start_sled_agent_request(stream, tx_requests, &log)
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
    stream: Stream<TcpStream>,
    tx_requests: TxRequestsChannel,
    log: &Logger,
) -> Result<(), String> {
    let mut stream = Box::new(BufStream::new(stream));

    let response = match read_request(&mut stream).await? {
        Request::StartSledAgentRequest(request) => {
            let (response_tx, response_rx) = oneshot::channel();

            match tx_requests.send((request.into_owned(), response_tx)).await {
                Ok(()) => match response_rx.await {
                    Ok(Ok(response)) => {
                        Ok(Response::SledAgentResponse(response))
                    }
                    Ok(Err(message)) => {
                        error!(
                            log,
                            "Request to initialize sled-agent failed";
                            "err" => &message,
                        );
                        Err(message)
                    }
                    Err(_) => {
                        error!(
                            log,
                            "Request to initialize sled-agent failed";
                            "err" => "internal response channel closed",
                        );
                        Err("Failed to initialize sled-agent: \
                             internal response channel closed"
                            .to_string())
                    }
                },
                Err(_) => {
                    error!(
                        log,
                        "Request to initialize sled-agent failed";
                        "err" => "internal request channel closed",
                    );
                    Err("Failed to initialize sled-agent: \
                         internal request channel closed"
                        .to_string())
                }
            }
        }
    };

    write_response(&mut stream, response).await
}

async fn read_request(
    stream: &mut Box<BufStream<Stream<TcpStream>>>,
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
    stream: &mut Box<BufStream<Stream<TcpStream>>>,
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
