// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! sled-agent's handle to the Rack Setup Service it spawns

use super::client as bootstrap_agent_client;
use crate::rack_setup::service::RackSetupService;
use crate::rack_setup::service::SetupServiceError;
use ::bootstrap_agent_client::Client as BootstrapAgentClient;
use bootstore::schemes::v0 as bootstore;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use omicron_common::backoff::BackoffError;
use omicron_common::backoff::retry_notify;
use omicron_common::backoff::retry_policy_local;
use sled_agent_config_reconciler::InternalDisksReceiver;
use sled_agent_types::rack_init::RackInitializeRequest;
use sled_agent_types::rack_ops::RssStep;
use sled_agent_types::sled::StartSledAgentRequest;
use slog::Logger;
use sprockets_tls::keys::SprocketsConfig;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub(super) struct RssHandle {
    _rss: RackSetupService,
    task: JoinHandle<()>,
}

impl Drop for RssHandle {
    fn drop(&mut self) {
        // NOTE: Ideally, with async drop, we'd await completion of the our task
        // handler.
        //
        // Without that option, we instead opt to simply cancel the task to
        // ensure it does not remain alive beyond the handle itself.
        self.task.abort();
    }
}

impl RssHandle {
    /// Executes the rack setup service until it has completed
    pub(super) async fn run_rss(
        log: &Logger,
        sprockets: SprocketsConfig,
        config: RackInitializeRequest,
        our_bootstrap_address: Ipv6Addr,
        internal_disks_rx: InternalDisksReceiver,
        bootstore: bootstore::NodeHandle,
        step_tx: watch::Sender<RssStep>,
    ) -> Result<(), SetupServiceError> {
        let (tx, rx) = rss_channel(our_bootstrap_address, sprockets);

        let rss = RackSetupService::new(
            log.new(o!("component" => "RSS")),
            config,
            internal_disks_rx,
            tx,
            bootstore,
            step_tx,
        );
        let log = log.new(o!("component" => "BootstrapAgentRssHandler"));
        rx.await_local_rss_request(&log).await;
        rss.join().await
    }

    /// Executes the rack setup service (reset mode) until it has completed
    pub(super) async fn run_rss_reset(
        log: &Logger,
        our_bootstrap_address: Ipv6Addr,
        sprockets: SprocketsConfig,
    ) -> Result<(), SetupServiceError> {
        let (tx, rx) = rss_channel(our_bootstrap_address, sprockets);

        let rss = RackSetupService::new_reset_rack(
            log.new(o!("component" => "RSS")),
            tx,
        );
        let log = log.new(o!("component" => "BootstrapAgentRssHandler"));
        rx.await_local_rss_request(&log).await;
        rss.join().await
    }
}

// Send a message to start a sled agent via bootstrap agent client
async fn initialize_sled_agent(
    log: &Logger,
    bootstrap_addr: SocketAddrV6,
    sprockets: SprocketsConfig,
    request: &StartSledAgentRequest,
) -> Result<(), bootstrap_agent_client::Error> {
    let client = bootstrap_agent_client::Client::new(
        bootstrap_addr,
        sprockets,
        log.new(o!("BootstrapAgentClient" => bootstrap_addr.to_string())),
    );

    let sled_agent_initialize = || async {
        client
            .start_sled_agent(request)
            .await
            .map_err(BackoffError::transient)?;

        Ok::<(), BackoffError<bootstrap_agent_client::Error>>(())
    };

    let log_failure = |error, _| {
        warn!(log, "failed to start sled agent"; "error" => ?error);
    };
    retry_notify(retry_policy_local(), sled_agent_initialize, log_failure)
        .await?;
    info!(log, "Peer agent initialized"; "peer" => %bootstrap_addr);
    Ok(())
}

// RSS needs to send requests (and receive responses) to and from its local sled
// agent over some communication channel. Currently RSS lives in-process with
// sled-agent, so we can use tokio channels. If we move out-of-process we'll
// need to switch to something like Unix domain sockets. We'll wrap the
// communication in the types below to avoid using tokio channels directly and
// leave a breadcrumb for where the work will need to be done to switch the
// communication mechanism.
fn rss_channel(
    our_bootstrap_address: Ipv6Addr,
    sprockets: SprocketsConfig,
) -> (BootstrapAgentHandle, BootstrapAgentHandleReceiver) {
    let (tx, rx) = mpsc::channel(32);
    (
        BootstrapAgentHandle { inner: tx, our_bootstrap_address },
        BootstrapAgentHandleReceiver { inner: rx, sprockets },
    )
}

type InnerInitRequest = Vec<(SocketAddrV6, StartSledAgentRequest)>;
type InnerResetRequest = Vec<SocketAddrV6>;

#[derive(Debug)]
struct Request {
    kind: RequestKind,
    tx: oneshot::Sender<Result<(), String>>,
}

#[derive(Debug)]
enum RequestKind {
    Init(InnerInitRequest),
    Reset(InnerResetRequest),
}

pub(crate) struct BootstrapAgentHandle {
    inner: mpsc::Sender<Request>,
    our_bootstrap_address: Ipv6Addr,
}

impl BootstrapAgentHandle {
    /// Instruct the local bootstrap-agent to initialize sled-agents based on
    /// the contents of `requests`.
    ///
    /// This function takes `self` and can only be called once with the full set
    /// of sleds to initialize. Returns `Ok(())` if initializing all sleds
    /// succeeds; if any sled fails to initialize, an error is returned
    /// immediately (i.e., the error message will pertain only to the first sled
    /// that failed to initialize).
    pub(crate) async fn initialize_sleds(
        self,
        requests: Vec<(SocketAddrV6, StartSledAgentRequest)>,
    ) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();

        // IPC will require real error handling, but we know the sled-agent task
        // will not close the channel until we do and that it will always send a
        // response, so unwrapping here is fine.
        //
        // Moving from channels to IPC will happen as a part of
        // https://github.com/oxidecomputer/omicron/issues/820.
        self.inner
            .send(Request { kind: RequestKind::Init(requests), tx })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub(crate) async fn reset_sleds(
        self,
        requests: Vec<SocketAddrV6>,
    ) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .send(Request { kind: RequestKind::Reset(requests), tx })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub(crate) fn our_address(&self) -> Ipv6Addr {
        self.our_bootstrap_address
    }
}

struct BootstrapAgentHandleReceiver {
    inner: mpsc::Receiver<Request>,
    sprockets: SprocketsConfig,
}

impl BootstrapAgentHandleReceiver {
    // Wait for a request from RSS telling us to initialize or reset all sled agents
    // via the bootstrap client.
    async fn await_local_rss_request(mut self, log: &Logger) {
        let Request { kind, tx } = match self.inner.recv().await {
            Some(requests) => requests,
            None => {
                warn!(log, "Failed receiving local requests from RSS",);
                return;
            }
        };

        match kind {
            RequestKind::Init(requests) => {
                // Convert the vec of requests into a `FuturesUnordered` containing all
                // of the initialization requests, allowing them to run concurrently.

                let s = self.sprockets.clone();
                let mut futs = requests
                    .into_iter()
                    .map(|(bootstrap_addr, request)| {
                        let value = s.clone();
                        async move {
                            info!(
                                log, "Received initialization request from RSS";
                                "request" => ?request,
                                "target_sled" => %bootstrap_addr,
                            );

                            initialize_sled_agent(
                                log,
                                bootstrap_addr,
                                value,
                                &request,
                            )
                            .await
                            .map_err(|err| {
                                format!(
                                    "Failed to initialize sled agent at {}: {}",
                                    bootstrap_addr, err
                                )
                            })?;

                            info!(
                                log, "Initialized sled agent";
                                "target_sled" => %bootstrap_addr,
                            );

                            Ok(())
                        }
                    })
                    .collect::<FuturesUnordered<_>>();

                // Wait for all initialization requests to complete, but stop on the
                // first error.
                //
                // We `.unwrap()` when sending a result on `tx` (either in this
                // loop or afterwards if all requests succeed), which is okay because we
                // know RSS is waiting for our response (i.e., we can only panic if RSS
                // already panicked itself). When we move RSS
                // out-of-process, tracked by
                // https://github.com/oxidecomputer/omicron/issues/820, we'll have to
                // replace these channels with IPC, which will also eliminiate these
                // unwraps.
                while let Some(result) = futs.next().await {
                    if result.is_err() {
                        tx.send(result).unwrap();
                        return;
                    }
                }
            }
            RequestKind::Reset(requests) => {
                let mut futs = requests
                    .into_iter()
                    .map(|bootstrap_addr| async move {
                        info!(
                            log, "Received reset request from RSS";
                            "target_sled" => %bootstrap_addr,
                        );

                        let dur = std::time::Duration::from_secs(60);
                        let client = reqwest::ClientBuilder::new()
                            .connect_timeout(dur)
                            .timeout(dur)
                            .build()
                            .map_err(|e| e.to_string())?;
                        let client = BootstrapAgentClient::new_with_client(
                            &format!("http://{}", bootstrap_addr),
                            client,
                            log.new(o!("BootstrapAgentClient" => bootstrap_addr.to_string())),
                        );
                        client.sled_reset().await.map_err(|e| e.to_string())?;

                        info!(
                            log, "Reset sled";
                            "target_sled" => %bootstrap_addr,
                        );

                        Ok(())
                    })
                    .collect::<FuturesUnordered<_>>();
                while let Some(result) = futs.next().await {
                    if result.is_err() {
                        tx.send(result).unwrap();
                        return;
                    }
                }
            }
        }

        // All requests succeeded; inform RSS of completion.
        tx.send(Ok(())).unwrap();
    }
}
