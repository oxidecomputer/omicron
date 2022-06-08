// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! sled-agent's handle to the Rack Setup Service it spawns

use super::client as bootstrap_agent_client;
use super::discovery::PeerMonitorObserver;
use super::params::SledAgentRequest;
use crate::rack_setup::config::SetupServiceConfig;
use crate::rack_setup::service::Service;
use crate::sp::SpHandle;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use omicron_common::backoff::internal_service_policy;
use omicron_common::backoff::retry_notify;
use omicron_common::backoff::BackoffError;
use slog::Logger;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::time::Duration;
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
        sp: Option<SpHandle>,
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
            rx.initialize_sleds(&log, &sp).await;
        });
        Self { _rss: rss, task }
    }
}

#[derive(Debug, Error)]
enum InitializeSledAgentError {
    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("Failed to start sprockets proxy: {0}")]
    SprocketsProxy(#[from] sprockets_proxy::Error),

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
    sp: &Option<SpHandle>,
) -> Result<(), InitializeSledAgentError> {
    let dur = std::time::Duration::from_secs(60);

    let client = reqwest::ClientBuilder::new()
        .connect_timeout(dur)
        .timeout(dur)
        .build()?;

    let (url, _proxy_task) = if let Some(sp) = sp.as_ref() {
        // We have an SP; spawn a sprockets proxy for this connection.
        let proxy_config = sprockets_proxy::Config {
            bind_address: "[::1]:0".parse().unwrap(),
            target_address: SocketAddr::V6(bootstrap_addr),
            role: sprockets_proxy::Role::Client,
        };
        // TODO-cleanup The `Duration` passed to `Proxy::new()` is the timeout
        // for communicating with the RoT. Currently it can be set to anything
        // at all (our simulated RoT always responds immediately). Should the
        // value move to our config?
        let proxy = sprockets_proxy::Proxy::new(
            &proxy_config,
            sp.manufacturing_public_key(),
            sp.rot_handle(),
            sp.rot_certs(),
            Duration::from_secs(5),
            log.new(o!("BootstrapAgentClientSprocketsProxy"
                        => proxy_config.target_address)),
        )
        .await?;

        let proxy_addr = proxy.local_addr();

        let proxy_task = tokio::spawn(async move {
            // TODO-robustness `proxy.run()` only fails if `accept()`ing on our
            // already-bound listening socket fails, which means something has
            // gone very wrong. Do we have any recourse other than panicking?
            // What does dropshot do if `accept()` fails?
            proxy.run().await.expect("sprockets client proxy failed");
        });

        // Wrap `proxy_task` in `AbortOnDrop`, which will abort it (shutting
        // down the proxy) when we return.
        let proxy_task = AbortOnDrop(proxy_task);

        info!(
            log, "Sending request to peer agent via sprockets proxy";
            "peer" => %bootstrap_addr,
            "sprockets_proxy" => %proxy_addr,
        );
        (format!("http://{}", proxy_addr), Some(proxy_task))
    } else {
        // We have no SP; connect directly.
        info!(
            log, "Sending request to peer agent";
            "peer" => %bootstrap_addr,
        );
        (format!("http://{}", bootstrap_addr), None)
    };

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
    async fn initialize_sleds(mut self, log: &Logger, sp: &Option<SpHandle>) {
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

        // Convert the vec of requests into a `FuturesUnordered` containing all
        // of the initialization requests, allowing them to run concurrently.
        let mut futs = requests
            .into_iter()
            .map(|(bootstrap_addr, request)| async move {
                info!(
                    log, "Received initialization request from RSS";
                    "request" => ?request,
                    "target_sled" => %bootstrap_addr,
                );

                initialize_sled_agent(log, bootstrap_addr, &request, sp)
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

struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
