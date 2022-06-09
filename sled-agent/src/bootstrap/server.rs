// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Server API for bootstrap-related functionality.

use super::agent::Agent;
use super::config::Config;
use super::params::version;
use super::params::Request;
use super::params::RequestEnvelope;
use super::views::Response;
use super::views::ResponseEnvelope;
use crate::config::Config as SledConfig;
use crate::sp::SpHandle;
use crate::sp::SprocketsRole;
use slog::Drain;
use slog::Logger;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

/// Wraps a [Agent] object, and provides helper methods for exposing it
/// via an HTTP interface.
pub struct Server {
    bootstrap_agent: Arc<Agent>,
    inner: JoinHandle<Result<(), String>>,
}

impl Server {
    pub async fn start(
        address: Ipv6Addr,
        config: Config,
        sled_config: SledConfig,
    ) -> Result<Self, String> {
        let (drain, registration) = slog_dtrace::with_drain(
            config.log.to_logger("SledAgent").map_err(|message| {
                format!("initializing logger: {}", message)
            })?,
        );
        let log = slog::Logger::root(drain.fuse(), slog::o!());
        if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
            let msg = format!("Failed to register DTrace probes: {}", e);
            error!(log, "{}", msg);
            return Err(msg);
        } else {
            debug!(log, "registered DTrace probes");
        }

        info!(log, "detecting (real or simulated) SP");
        let sp = SpHandle::detect(&config.sp_config, &sled_config, &log)
            .await
            .map_err(|err| format!("Failed to detect local SP: {err}"))?;

        info!(log, "setting up bootstrap agent server");
        let bootstrap_agent = Arc::new(
            Agent::new(log.clone(), sled_config, address, sp.clone())
                .await
                .map_err(|e| e.to_string())?,
        );

        let ba = Arc::clone(&bootstrap_agent);
        let ba_log = log.new(o!("component" => "BootstrapAgentServer"));
        let inner =
            Inner::start(config.bind_address, sp.clone(), ba, ba_log).await?;

        let server = Server { bootstrap_agent, inner };

        // Initialize the bootstrap agent *after* the server has started.
        // This ordering allows the bootstrap agent to communicate with
        // other bootstrap agents on the rack during the initialization
        // process.
        if let Err(e) = server.bootstrap_agent.initialize(&config).await {
            server.inner.abort();
            return Err(e.to_string());
        }

        Ok(server)
    }

    pub async fn wait_for_finish(self) -> Result<(), String> {
        match self.inner.await {
            Ok(result) => result,
            Err(err) => {
                if err.is_cancelled() {
                    // We control cancellation of `inner`, which only happens if
                    // we intentionally abort it in `close()`; that should not
                    // result in an error here.
                    Ok(())
                } else {
                    Err(format!("Join on server tokio task failed: {err}"))
                }
            }
        }
    }

    pub async fn close(self) -> Result<(), String> {
        self.inner.abort();
        self.wait_for_finish().await
    }
}

struct Inner {
    listener: TcpListener,
    sp: Option<SpHandle>,
    bootstrap_agent: Arc<Agent>,
    log: Logger,
}

impl Inner {
    async fn start(
        bind_address: SocketAddrV6,
        sp: Option<SpHandle>,
        bootstrap_agent: Arc<Agent>,
        log: Logger,
    ) -> Result<JoinHandle<Result<(), String>>, String> {
        let listener =
            TcpListener::bind(bind_address).await.map_err(|err| {
                format!("could not bind to {bind_address}: {err}")
            })?;
        info!(log, "Started listening"; "local_addr" => %bind_address);
        let inner = Inner { listener, sp, bootstrap_agent, log };
        Ok(tokio::spawn(inner.run()))
    }

    // Run our sprockets server. The only `.await` point is when `accept()`ing
    // on our bound socket, so we can be cleanly shut down by our caller
    // `.abort()`ing our task, which will not affect any already-accepted
    // sockets (which are spawned onto detached tokio tasks).
    async fn run(self) -> Result<(), String> {
        loop {
            let (stream, remote_addr) =
                self.listener.accept().await.map_err(|err| {
                    format!("accept() on already-bound socket failed: {err}")
                })?;

            let log = self.log.new(o!("remote_addr" => remote_addr));
            info!(log, "Accepted connection");

            let sp = self.sp.clone();
            let ba = Arc::clone(&self.bootstrap_agent);
            tokio::spawn(async move {
                match serve_single_request(stream, sp, ba, &log).await {
                    Ok(()) => info!(log, "Connection closed"),
                    Err(err) => warn!(log, "Connection failed"; "err" => err),
                }
            });
        }
    }
}

async fn serve_single_request(
    stream: TcpStream,
    sp: Option<SpHandle>,
    bootstrap_agent: Arc<Agent>,
    log: &Logger,
) -> Result<(), String> {
    // Bound to avoid allocating an unreasonable amount of memory from a bogus
    // length prefix from a client. We authenticate clients via sprockets before
    // allocating based on the length prefix they send, so it should be fine to
    // be a little sloppy here and just pick something far larger than we ever
    // expect to see.
    const MAX_REQUEST_LEN: u32 = 128 << 20;

    // Establish sprockets session (if we have an SP).
    let mut stream =
        crate::sp::maybe_wrap_stream(stream, &sp, SprocketsRole::Server, log)
            .await
            .map_err(|err| {
                format!("Failed to establish sprockets session: {err}")
            })?;

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
    let envelope: RequestEnvelope =
        serde_json::from_slice(&buf).map_err(|err| {
            format!("Failed to deserialize request envelope: {err}")
        })?;

    // Currently we only have one version, so there's nothing to do in this
    // match, but we leave it here as a breadcrumb for future changes.
    match envelope.version {
        version::V1 => (),
        other => return Err(format!("Unsupported version: {other}")),
    }

    // Handle request.
    let response = match envelope.request {
        Request::SledAgentRequest(request) => {
            match bootstrap_agent.request_agent(&*request).await {
                Ok(response) => Ok(Response::SledAgentResponse(response)),
                Err(err) => {
                    warn!(log, "Sled agent request failed"; "err" => %err);
                    Err(format!("Sled agent request failed: {err}"))
                }
            }
        }
        Request::ShareRequest => {
            Err("share request currently unsupported".to_string())
        }
    };

    // Build and serialize response.
    let envelope = ResponseEnvelope { version: version::V1, response };
    buf.clear();
    serde_json::to_writer(&mut buf, &envelope)
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
