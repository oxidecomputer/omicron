// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Server API for bootstrap-related functionality.

use super::agent::Agent;
use super::config::Config;
use super::params::version;
use super::params::Request;
use super::params::RequestEnvelope;
use super::trust_quorum::ShareDistribution;
use super::views::Response;
use super::views::ResponseEnvelope;
use crate::bootstrap::http_entrypoints::api as http_api;
use crate::bootstrap::maghemite;
use crate::config::Config as SledConfig;
use crate::sp::AsyncReadWrite;
use crate::sp::SpHandle;
use crate::sp::SprocketsRole;
use sled_hardware::underlay;
use slog::Drain;
use slog::Logger;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

pub enum TrustQuorumMembership {
    Uninitialized,
    // TODO-cleanup `ShareDistribution` is optional here because we still
    // support dev/test environments that do not use a trust quorum. Eventually
    // it should be non-optional.
    Known(Arc<Option<ShareDistribution>>),
}

/// Wraps a [Agent] object, and provides helper methods for exposing it
/// via an HTTP interface.
pub struct Server {
    bootstrap_agent: Arc<Agent>,
    sprockets_server_handle: JoinHandle<Result<(), String>>,
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
        let log = slog::Logger::root(drain.fuse(), slog::o!());
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

        info!(log, "detecting (real or simulated) SP");
        let sp = SpHandle::detect(
            config.sp_config.as_ref().map(|c| &c.local_sp),
            &log,
        )
        .await
        .map_err(|err| format!("Failed to detect local SP: {err}"))?;

        info!(log, "setting up bootstrap agent server");
        let (bootstrap_agent, trust_quorum) =
            Agent::new(log.clone(), config.clone(), sled_config, sp.clone())
                .await
                .map_err(|e| e.to_string())?;
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

        let sprockets_log =
            log.new(o!("component" => "sprockets (BootstrapAgent)"));
        let sprockets_server_handle = Inner::start_sprockets(
            sp.clone(),
            trust_quorum,
            Arc::clone(&bootstrap_agent),
            sprockets_log,
        )
        .await?;

        let server = Server {
            bootstrap_agent,
            sprockets_server_handle,
            _http_server: http_server,
        };
        Ok(server)
    }

    pub fn agent(&self) -> &Agent {
        &self.bootstrap_agent
    }

    pub async fn wait_for_finish(self) -> Result<(), String> {
        match self.sprockets_server_handle.await {
            Ok(result) => result,
            Err(err) => {
                if err.is_cancelled() {
                    // We control cancellation of `sprockets_server_handle`,
                    // which only happens if we intentionally abort it in
                    // `close()`; that should not result in an error here.
                    Ok(())
                } else {
                    Err(format!("Join on server tokio task failed: {err}"))
                }
            }
        }
    }

    pub async fn close(self) -> Result<(), String> {
        self.sprockets_server_handle.abort();
        self.wait_for_finish().await
    }
}

struct Inner {
    listener: TcpListener,
    sp: Option<SpHandle>,
    trust_quorum: TrustQuorumMembership,
    bootstrap_agent: Arc<Agent>,
    log: Logger,
}

impl Inner {
    async fn start_sprockets(
        // TODO-cleanup `sp` is optional because we support running without an
        // SP / any trust quorum mechanisms. Eventually it should be required.
        sp: Option<SpHandle>,
        trust_quorum: TrustQuorumMembership,
        bootstrap_agent: Arc<Agent>,
        log: Logger,
    ) -> Result<JoinHandle<Result<(), String>>, String> {
        let bind_address = bootstrap_agent.sprockets_address();

        let listener =
            TcpListener::bind(bind_address).await.map_err(|err| {
                format!("could not bind to {bind_address}: {err}")
            })?;
        info!(log, "Started listening"; "local_addr" => %bind_address);
        let inner = Inner { listener, sp, trust_quorum, bootstrap_agent, log };
        Ok(tokio::spawn(inner.run()))
    }

    async fn run(self) -> Result<(), String> {
        // Do we already have our trust quorum share? If not, we need to wait
        // for RSS to send us a sled agent init request.
        let trust_quorum = match self.trust_quorum {
            TrustQuorumMembership::Uninitialized => {
                self.wait_for_sled_initialization().await?
            }
            TrustQuorumMembership::Known(quorum) => quorum,
        };

        loop {
            let (stream, remote_addr) =
                self.listener.accept().await.map_err(|err| {
                    format!("accept() on already-bound socket failed: {err}")
                })?;

            let log = self.log.new(o!("remote_addr" => remote_addr));
            info!(log, "Accepted connection");

            let bootstrap_agent = self.bootstrap_agent.clone();
            let sp = self.sp.clone();
            let trust_quorum = Arc::clone(&trust_quorum);
            tokio::spawn(async move {
                match serve_request_after_quorum_initialization(
                    stream,
                    bootstrap_agent,
                    sp,
                    trust_quorum.as_ref(),
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

    async fn wait_for_sled_initialization(
        &self,
    ) -> Result<Arc<Option<ShareDistribution>>, String> {
        // Channel on which we receive the trust quorum share after quorum has
        // been established.
        let (tx_share, mut rx_share) = mpsc::channel(1);

        // Shared trust quorum share allowing us to return the share while we're
        // still establishing quorum.
        let initial_share = Arc::new(Mutex::new(None));

        loop {
            // Wait for either a new client or a response on our channel sent by
            // a task spawned for a previously-accepted client that provides us
            // our quorum share.
            let (stream, remote_addr) = tokio::select! {
                share = rx_share.recv() => {
                    // `share` can never be `None`, as we're holding
                    // `tx_share`; we can `.unwrap()` it.
                    return Ok(Arc::new(share.unwrap()));
                }
                result = self.listener.accept() => {
                    result.map_err(|err| {
                        format!("accept() on already-bound socket failed: {err}")
                    })?
                }
            };

            let log = self.log.new(o!("remote_addr" => remote_addr));
            info!(log, "Accepted connection");

            let sp = self.sp.clone();
            let ba = Arc::clone(&self.bootstrap_agent);
            let tx_share = tx_share.clone();
            let initial_share = Arc::clone(&initial_share);
            tokio::spawn(async move {
                match serve_request_before_quorum_initialization(
                    stream,
                    sp,
                    &ba,
                    tx_share,
                    &initial_share,
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

async fn serve_request_before_quorum_initialization(
    stream: TcpStream,
    sp: Option<SpHandle>,
    bootstrap_agent: &Agent,
    tx_share: mpsc::Sender<Option<ShareDistribution>>,
    initial_share: &Mutex<Option<ShareDistribution>>,
    log: &Logger,
) -> Result<(), String> {
    // Establish sprockets session (if we have an SP).
    let mut stream = crate::sp::maybe_wrap_stream(
        stream,
        &sp,
        SprocketsRole::Server,
        // If we've already received a sled agent request, any future
        // connections to us are only allowed for members of our trust quorum.
        // If we haven't yet received a sled agent request, we don't know our
        // trust quorum members, and have to blindly accept any valid sprockets
        // connection.
        initial_share
            .lock()
            .await
            .as_ref()
            .map(|dist| dist.member_device_id_certs.as_slice()),
        log,
    )
    .await
    .map_err(|err| format!("Failed to establish sprockets session: {err}"))?;

    let response = match read_request(&mut stream).await? {
        Request::SledAgentRequest(request, trust_quorum_share) => {
            let trust_quorum_share =
                trust_quorum_share.map(ShareDistribution::from);

            // Save the trust quorum share _before_ calling request_agent, so
            // that we can return it in the `Request::ShareRequest` branch below
            //
            // TODO should we check that if `initial_share` is already
            // `Some(_)`, the value is the same? If it's not, we got two
            // different shares from RSS...
            *initial_share.lock().await = trust_quorum_share.clone();

            match bootstrap_agent
                .request_agent(&request, &trust_quorum_share)
                .await
            {
                Ok(response) => {
                    // If this send fails, it means our caller already received
                    // our share from a different
                    // `serve_request_before_quorum_initialization()` task
                    // (i.e., from another incoming request from RSS). We'll
                    // ignore such failures.
                    let _ = tx_share.send(trust_quorum_share).await;

                    Ok(Response::SledAgentResponse(response))
                }
                Err(err) => {
                    warn!(log, "Sled agent request failed"; "err" => %err);
                    Err(format!("Sled agent request failed: {err}"))
                }
            }
        }
        Request::ShareRequest => match initial_share.lock().await.clone() {
            Some(dist) => Ok(Response::ShareResponse(dist.share)),
            None => {
                warn!(log, "Share requested before we have one");
                Err("Share request failed: share unavailable".to_string())
            }
        },
    };

    write_response(&mut stream, response).await
}

async fn serve_request_after_quorum_initialization(
    stream: TcpStream,
    bootstrap_agent: Arc<Agent>,
    // TODO-cleanup `sp` and `tx_share` are optional while we still allow
    // trust-quorum-free dev/test setups. Eventually they should be required.
    sp: Option<SpHandle>,
    trust_quorum_share: &Option<ShareDistribution>,
    log: &Logger,
) -> Result<(), String> {
    // Establish sprockets session (if we have an SP).
    let mut stream = crate::sp::maybe_wrap_stream(
        stream,
        &sp,
        SprocketsRole::Server,
        trust_quorum_share
            .as_ref()
            .map(|dist| dist.member_device_id_certs.as_slice()),
        log,
    )
    .await
    .map_err(|err| format!("Failed to establish sprockets session: {err}"))?;

    let response = match read_request(&mut stream).await? {
        Request::SledAgentRequest(request, trust_quorum_share) => {
            let trust_quorum_share =
                trust_quorum_share.map(ShareDistribution::from);

            // The call to `request_agent` should be idempotent if the request
            // was the same.
            bootstrap_agent
                .request_agent(&request, &trust_quorum_share)
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
        Request::ShareRequest => {
            match trust_quorum_share {
                Some(dist) => Ok(Response::ShareResponse(dist.share.clone())),
                None => {
                    // TODO-cleanup Remove this case once we always use trust
                    // quorum.
                    warn!(log, "Received share request, but we have no quorum");
                    Err("No trust quorum in use".to_string())
                }
            }
        }
    };

    write_response(&mut stream, response).await
}

async fn read_request(
    stream: &mut Box<dyn AsyncReadWrite>,
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
    stream: &mut Box<dyn AsyncReadWrite>,
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
