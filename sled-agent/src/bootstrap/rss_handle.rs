// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! sled-agent's handle to the Rack Setup Service it spawns

use super::client as bootstrap_agent_client;
use super::params::SledAgentRequest;
use super::trust_quorum::ShareDistribution;
use crate::rack_setup::config::SetupServiceConfig;
use crate::rack_setup::service::RackSetupService;
use crate::sp::SpHandle;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use omicron_common::backoff::internal_service_policy;
use omicron_common::backoff::retry_notify;
use omicron_common::backoff::BackoffError;
use slog::Logger;
use sprockets_host::Ed25519Certificate;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
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
    // Start the Rack setup service.
    pub(super) fn start_rss(
        log: &Logger,
        config: SetupServiceConfig,
        our_bootstrap_address: Ipv6Addr,
        sp: Option<SpHandle>,
        member_device_id_certs: Vec<Ed25519Certificate>,
    ) -> Self {
        let (tx, rx) = rss_channel(our_bootstrap_address);

        let rss = RackSetupService::new(
            log.new(o!("component" => "RSS")),
            config,
            tx,
            member_device_id_certs,
        );
        let log = log.new(o!("component" => "BootstrapAgentRssHandler"));
        let task = tokio::spawn(async move {
            rx.initialize_sleds(&log, &sp).await;
        });
        Self { _rss: rss, task }
    }
}

async fn initialize_sled_agent(
    log: &Logger,
    bootstrap_addr: SocketAddrV6,
    request: &SledAgentRequest,
    trust_quorum_share: Option<ShareDistribution>,
    sp: &Option<SpHandle>,
) -> Result<(), bootstrap_agent_client::Error> {
    let client = bootstrap_agent_client::Client::new(
        bootstrap_addr,
        sp,
        // TODO-cleanup: Creating a bootstrap client requires the list of trust
        // quorum members (as clients should always know the set of possible
        // servers they can connect to), but `trust_quorum_share` is
        // optional for now because we don't yet require trust quorum in all
        // sled-agent deployments. We use `.map_or(&[], ...)` here to pass an
        // empty set of trust quorum members if we're in such a
        // trust-quorum-free deployment. This would cause any sprockets
        // connections to fail with unknown peers, but in a trust-quorum-free
        // deployment we don't actually wrap connections in sprockets.
        trust_quorum_share
            .as_ref()
            .map_or(&[], |share| share.member_device_id_certs.as_slice()),
        log.new(o!("BootstrapAgentClient" => bootstrap_addr.to_string())),
    );

    let sled_agent_initialize = || async {
        client
            .start_sled(request, trust_quorum_share.clone())
            .await
            .map_err(BackoffError::transient)?;

        Ok::<(), BackoffError<bootstrap_agent_client::Error>>(())
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
fn rss_channel(
    our_bootstrap_address: Ipv6Addr,
) -> (BootstrapAgentHandle, BootstrapAgentHandleReceiver) {
    let (tx, rx) = mpsc::channel(32);
    (
        BootstrapAgentHandle { inner: tx, our_bootstrap_address },
        BootstrapAgentHandleReceiver { inner: rx },
    )
}

type InnerInitRequest = (
    Vec<(SocketAddrV6, SledAgentRequest, Option<ShareDistribution>)>,
    oneshot::Sender<Result<(), String>>,
);

pub(crate) struct BootstrapAgentHandle {
    inner: mpsc::Sender<InnerInitRequest>,
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
        requests: Vec<(
            SocketAddrV6,
            SledAgentRequest,
            Option<ShareDistribution>,
        )>,
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

    pub(crate) fn our_address(&self) -> Ipv6Addr {
        self.our_bootstrap_address
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
            .map(|(bootstrap_addr, request, trust_quorum_share)| async move {
                info!(
                    log, "Received initialization request from RSS";
                    "request" => ?request,
                    "target_sled" => %bootstrap_addr,
                );

                initialize_sled_agent(
                    log,
                    bootstrap_addr,
                    &request,
                    trust_quorum_share,
                    sp,
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
