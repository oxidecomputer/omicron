// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Server API for bootstrap-related functionality.

use super::agent::Agent;
use super::agent::CancelReason;
use super::config::Config;
use super::params::version;
use super::params::Request;
use super::params::RequestEnvelope;
use super::views::Response;
use super::views::ResponseEnvelope;
use crate::bootstrap::http_entrypoints::api as http_api;
use crate::bootstrap::maghemite;
use crate::config::Config as SledConfig;
use cancel_safe_futures::coop_cancel;
use omicron_common::FileKv;
use sled_hardware::underlay;
use slog::Drain;
use slog::Logger;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufStream;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

/// Wraps a [Agent] object, and provides helper methods for exposing it
/// via an HTTP interface and a tcp server used for rack initialization.
pub struct Server {
    log: Logger,
    bootstrap_agent: Arc<Agent>,
    rack_init_server_handle: JoinHandle<Result<CancelReason, String>>,
    canceler: coop_cancel::Canceler<CancelReason>,
    _http_server: dropshot::HttpServer<Arc<Agent>>,
}

impl Server {
    pub async fn start(
        config: Config,
        sled_config: SledConfig,
    ) -> Result<Self, String> {
        let (drain, registration) = slog_dtrace::with_drain(
            config.log.to_logger("SledAgent").map_err(|message| {
                format!("initializing logger: {}", message)
            })?,
        );
        let log = slog::Logger::root(drain.fuse(), slog::o!(FileKv));
        if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
            let msg = format!("Failed to register DTrace probes: {}", e);
            error!(log, "{}", msg);
            return Err(msg);
        } else {
            debug!(log, "registered DTrace probes");
        }

        // Find address objects to pass to maghemite.
        let mg_addr_objs = underlay::find_nics().map_err(|err| {
            format!("Failed to find address objects for maghemite: {err}")
        })?;
        if mg_addr_objs.is_empty() {
            return Err(
                "underlay::find_nics() returned 0 address objects".to_string()
            );
        }

        info!(log, "Starting mg-ddm service");
        maghemite::enable_mg_ddm_service(log.clone(), mg_addr_objs.clone())
            .await
            .map_err(|err| format!("Failed to start mg-ddm: {err}"))?;

        let (canceler, cancel_receiver) =
            cancel_safe_futures::coop_cancel::new_pair();

        info!(log, "setting up bootstrap agent server");
        let bootstrap_agent = Agent::new(
            log.clone(),
            config.clone(),
            sled_config,
            canceler.clone(),
        )
        .await
        .map_err(|e| e.to_string())?;
        info!(log, "bootstrap agent finished initialization successfully");
        let bootstrap_agent = Arc::new(bootstrap_agent);

        let mut dropshot_config = dropshot::ConfigDropshot::default();
        dropshot_config.request_body_max_bytes = 1024 * 1024;
        dropshot_config.bind_address =
            SocketAddr::V6(bootstrap_agent.http_address());
        let dropshot_log =
            log.new(o!("component" => "dropshot (BootstrapAgent)"));
        let http_server = dropshot::HttpServerStarter::new(
            &dropshot_config,
            http_api(),
            bootstrap_agent.clone(),
            &dropshot_log,
        )
        .map_err(|error| format!("initializing server: {}", error))?
        .start();

        let inner_log =
            log.new(o!("component" => "rack init server (BootstrapAgent)"));
        let rack_init_server_handle = Inner::start_rack_init_server(
            Arc::clone(&bootstrap_agent),
            cancel_receiver,
            inner_log,
        )
        .await?;

        let log = log.new(o!("component" => "bootstrap::servere"));
        let server = Server {
            log,
            bootstrap_agent,
            rack_init_server_handle,
            canceler,
            _http_server: http_server,
        };
        Ok(server)
    }

    pub fn agent(&self) -> &Arc<Agent> {
        &self.bootstrap_agent
    }

    pub async fn wait_for_finish(self) -> Result<CancelReason, String> {
        // Note that self.rack_init_server_handle does not exit unless either:
        // * accept() errors out, or
        // * the abort receiver is called.
        match self.rack_init_server_handle.await {
            Ok(result) => match result {
                Ok(cancel_reason) => {
                    info!(self.log, "Rack init server cancelled"; "cancel_reason" => %cancel_reason);
                    Ok(cancel_reason)
                }
                Err(error) => {
                    let error =
                        format!("Rack init server tokio task failed: {error}");
                    error!(self.log, "{error}");
                    Err(error)
                }
            },
            Err(err) => {
                // We previously checked err.is_cancelled() after aborting in
                // Self::close. However, after we switched to cooperative
                // cancellation, this should never happen in practice. That's
                // because we never call `rack_init_server_handle.abort()`. But
                // in case it does happen for some reason, log it.
                if err.is_cancelled() {
                    warn!(self.log, "Join on rack_init_server_handle unexpectedly cancelled");
                }
                let error =
                    format!("Rack init server tokio task failed: {err}");
                error!(self.log, "{error}");
                Err(error)
            }
        }
    }

    pub async fn close(self) -> Result<CancelReason, String> {
        self.canceler
            .cancel(CancelReason::CloseCalled)
            .expect("receiver held open by self");
        self.wait_for_finish().await
    }
}

struct Inner {
    listener: TcpListener,
    bootstrap_agent: Arc<Agent>,
    log: Logger,
}

impl Inner {
    async fn start_rack_init_server(
        bootstrap_agent: Arc<Agent>,
        cancel_receiver: coop_cancel::Receiver<CancelReason>,
        log: Logger,
    ) -> Result<JoinHandle<Result<CancelReason, String>>, String> {
        let bind_address = bootstrap_agent.rack_init_address();

        let listener =
            TcpListener::bind(bind_address).await.map_err(|err| {
                format!("could not bind to {bind_address}: {err}")
            })?;
        info!(log, "Started listening"; "local_addr" => %bind_address);
        let inner = Inner { listener, bootstrap_agent, log };

        Ok(tokio::spawn(inner.run(cancel_receiver)))
    }

    async fn run(
        self,
        mut cancel_receiver: coop_cancel::Receiver<CancelReason>,
    ) -> Result<CancelReason, String> {
        loop {
            let (stream, remote_addr) = tokio::select! {
                result = self.listener.accept() => {
                    result.map_err(|err| {
                        format!("accept() on already-bound socket failed: {err}")
                    })?
                }
                Some(kind) = cancel_receiver.recv() => {
                    info!(self.log, "Aborting server"; "kind" => ?kind);
                    return Ok(kind);
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
    let mut stream = Box::new(tokio::io::BufStream::new(stream));

    let response = match read_request(&mut stream).await? {
        Request::StartSledAgentRequest(request) => {
            // The call to `request_sled_agent` should be idempotent if the request
            // was the same.
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

/// Runs the OpenAPI generator, emitting the spec to stdout.
pub fn run_openapi() -> Result<(), String> {
    http_api()
        .openapi("Oxide Bootstrap Agent API", "0.0.1")
        .description("API for interacting with individual sleds")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}
