// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! sled-agent's handle to the Rack Setup Service it spawns

use super::client as bootstrap_agent_client;
use super::discovery::PeerMonitorObserver;
use super::params::SledAgentRequest;
use crate::rack_setup::config::SetupServiceConfig;
use crate::rack_setup::service::Service;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::StreamExt;
use omicron_common::backoff::internal_service_policy;
use omicron_common::backoff::retry_notify;
use omicron_common::backoff::BackoffError;
use slog::Logger;
use std::net::SocketAddrV6;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub(super) struct RssHandle {
    _rss: Service,
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
    // Start the Rack setup service.
    pub(super) fn start_rss(
        log: &Logger,
        config: SetupServiceConfig,
        peer_monitor: PeerMonitorObserver,
    ) -> Self {
        let (tx, rx) = rss_channel();

        let rss = Service::new(
            log.new(o!("component" => "RSS")),
            config,
            peer_monitor,
            tx,
        );
        let log = log.new(o!("component" => "BootstrapAgentRssHandler"));
        let task = tokio::spawn(async move {
            handle_rss_requests(&log, rx).await;
        });
        Self { _rss: rss, task }
    }
}

async fn handle_rss_requests(
    log: &Logger,
    requests: BootstrapAgentHandleReceiver,
) {
    requests
        .initialize_sleds(log, |bootstrap_addr, request| async move {
            info!(
                log, "Received initialization request from RSS";
                "request" => ?request,
                "target_sled" => %bootstrap_addr,
            );

            initialize_sled_agent(log, bootstrap_addr, &request)
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
        })
        .await;
}

#[derive(Debug, Error)]
enum InitializeSledAgentError {
    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("Error making HTTP request to Bootstrap Agent: {0}")]
    BootstrapApi(
        #[from]
        bootstrap_agent_client::Error<bootstrap_agent_client::types::Error>,
    ),
}

async fn initialize_sled_agent(
    log: &Logger,
    bootstrap_addr: SocketAddrV6,
    request: &SledAgentRequest,
) -> Result<(), InitializeSledAgentError> {
    let dur = std::time::Duration::from_secs(60);

    let client = reqwest::ClientBuilder::new()
        .connect_timeout(dur)
        .timeout(dur)
        .build()?;

    let url = format!("http://{}", bootstrap_addr);
    info!(log, "Sending request to peer agent: {}", url);
    let client = bootstrap_agent_client::Client::new_with_client(
        &url,
        client,
        log.new(o!("BootstrapAgentClient" => url.clone())),
    );

    let sled_agent_initialize = || async {
        client
            .start_sled(&bootstrap_agent_client::types::SledAgentRequest {
                subnet: bootstrap_agent_client::types::Ipv6Subnet {
                    net: bootstrap_agent_client::types::Ipv6Net(
                        request.subnet.net().to_string(),
                    ),
                },
            })
            .await
            .map_err(BackoffError::transient)?;

        Ok::<
            (),
            BackoffError<
                bootstrap_agent_client::Error<
                    bootstrap_agent_client::types::Error,
                >,
            >,
        >(())
    };

    let log_failure = |error, _| {
        warn!(log, "failed to start sled agent"; "error" => ?error);
    };
    retry_notify(internal_service_policy(), sled_agent_initialize, log_failure)
        .await?;
    info!(log, "Peer agent at {} initialized", url);
    Ok(())
}

// RSS needs to send requests (and receive responses) to and from its local sled
// agent over some communication channel. Currently RSS lives in-process with
// sled-agent, so we can use tokio channels. If we move out-of-process we'll
// need to switch to something like Unix domain sockets. We'll wrap the
// communication in the types below to avoid using tokio channels directly and
// leave a breadcrumb for where the work will need to be done to switch the
// communication mechanism.
fn rss_channel() -> (BootstrapAgentHandle, BootstrapAgentHandleReceiver) {
    let (tx, rx) = mpsc::channel(32);
    (
        BootstrapAgentHandle { inner: tx },
        BootstrapAgentHandleReceiver { inner: rx },
    )
}

type InnerInitRequest = (
    Vec<(SocketAddrV6, SledAgentRequest)>,
    oneshot::Sender<Result<(), String>>,
);

pub(crate) struct BootstrapAgentHandle {
    inner: mpsc::Sender<InnerInitRequest>,
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
        requests: Vec<(SocketAddrV6, SledAgentRequest)>,
    ) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();

        // IPC will require real error handling, but we know the sled-agent task
        // will not close the channel until we do and that it will always send a
        // response, so unwrapping here is fine.
        //
        // Moving from channels to IPC will happen as a part of
        // https://github.com/oxidecomputer/omicron/issues/820.
        self.inner.send((requests, tx)).await.unwrap();
        rx.await.unwrap()
    }
}

struct BootstrapAgentHandleReceiver {
    inner: mpsc::Receiver<InnerInitRequest>,
}

impl BootstrapAgentHandleReceiver {
    async fn initialize_sleds<F, Fut>(mut self, log: &Logger, init_one_sled: F)
    where
        F: Fn(SocketAddrV6, SledAgentRequest) -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        let (requests, tx_response) = match self.inner.recv().await {
            Some(requests) => requests,
            None => {
                warn!(
                    log,
                    "Failed receiving sled initialization requests from RSS",
                );
                return;
            }
        };

        let mut futs = requests
            .into_iter()
            .map(|(addr, req)| init_one_sled(addr, req))
            .collect::<FuturesUnordered<_>>();

        // Wait for all initialization requests to complete, but stop on the
        // first error.
        //
        // We `.unwrap()` when sending a result on `tx_response` (either in this
        // loop or afterwards if all requests succeed), which is okay because we
        // know RSS is waiting for our response (i.e., we can only panic if RSS
        // already panicked itself). When we move RSS
        // out-of-process, tracked by
        // https://github.com/oxidecomputer/omicron/issues/820, we'll have to
        // replace these channels with IPC, which will also eliminiate these
        // unwraps.
        while let Some(result) = futs.next().await {
            if result.is_err() {
                tx_response.send(result).unwrap();
                return;
            }
        }

        // All init requests succeeded; inform RSS of completion.
        tx_response.send(Ok(())).unwrap();
    }
}
