// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code to report events to the artifact server.

use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use display_error_chain::DisplayErrorChain;
use futures::StreamExt;
use http::StatusCode;
use installinator_client::ClientError;
use installinator_common::{Event, EventBuffer, EventReport};
use tokio::{sync::mpsc, task::JoinHandle, time};
use update_engine::AsError;
use uuid::Uuid;

use crate::{
    artifact::ArtifactClient,
    errors::DiscoverPeersError,
    peers::{DiscoveryMechanism, PeerAddress},
};

#[derive(Debug)]
pub(crate) struct ProgressReporter {
    log: slog::Logger,
    update_id: Uuid,
    report_backend: ReportProgressBackend,
    // Receives updates about progress and completion.
    event_receiver: mpsc::Receiver<Event>,
    buffer: EventBuffer,
    last_reported: Option<usize>,
    on_tick_task: Option<JoinHandle<Option<Option<usize>>>>,
}

impl ProgressReporter {
    pub(crate) fn new(
        log: &slog::Logger,
        update_id: Uuid,
        report_backend: ReportProgressBackend,
    ) -> (Self, mpsc::Sender<Event>) {
        let (event_sender, event_receiver) = update_engine::channel();
        let ret = Self {
            log: log.new(slog::o!("component" => "EventReporter")),
            update_id,
            event_receiver,
            // We have to keep max_low_priority low since a bigger number will
            // cause a payload that's too large.
            buffer: EventBuffer::new(8),
            last_reported: None,
            on_tick_task: None,
            report_backend,
        };
        (ret, event_sender)
    }

    pub(crate) fn start(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // TODO: jitter?
            let mut interval = time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            interval.tick().await;

            let mut events_done = false;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        self.on_tick().await;
                    }

                    event = self.event_receiver.recv(), if !events_done => {
                        if let Some(event) = event {
                            self.buffer.add_event(event);
                        } else {
                            // The completion sender has been dropped; this is
                            // an indication to drain the completion queue and
                            // end the loop.
                            events_done = true;
                        }
                    }
                }

                if events_done
                    && !self.buffer.has_pending_events_since(self.last_reported)
                {
                    // All done, now exit.
                    break;
                }
            }
        })
    }

    fn spawn_on_tick_task(&self) -> JoinHandle<Option<Option<usize>>> {
        let report = self.buffer.generate_report();
        let update_id = self.update_id;
        let log = self.log.clone();
        let report_backend = self.report_backend.clone();

        tokio::spawn(async move {
            let peers = match report_backend.discover_peers().await {
                Ok(peers) => peers,
                Err(DiscoverPeersError::Retry(error)) => {
                    slog::warn!(
                        log,
                        "failed to discover peers: {}",
                        DisplayErrorChain::new(error.as_error()),
                    );
                    return None;
                }
                #[cfg(test)]
                Err(DiscoverPeersError::Abort(_)) => {
                    // This is not possible, since test implementations don't
                    // currently generate Abort errors during reporter
                    // discovery.
                    unreachable!(
                        "DiscoverPeersError::Abort is not generated for the \
                         reporter by test implementations"
                    )
                }
            };

            // We could use StreamExt::any() here, but we're choosing to avoid
            // short-circuiting for now in favor of reaching out to all peers.
            //
            // TODO: is this correct? If a peer takes too long this might end up
            // stalling updates for everyone -- feels wrong. The tradeoff is
            // that if two servers both say, only one of them will
            // deterministically get the update. Need to decide post-PVT1.
            let last_reported = report.last_seen;
            let results: Vec<_> = futures::stream::iter(peers)
                .map(|peer| {
                    report_backend.send_report_to_peer(
                        peer,
                        update_id,
                        report.clone(),
                    )
                })
                .buffer_unordered(8)
                .collect()
                .await;

            if results.iter().any(|res| res.is_ok()) {
                Some(last_reported)
            } else {
                None
            }
        })
    }

    async fn on_tick(&mut self) {
        if let Some(task) = self.on_tick_task.take() {
            // If the task we spawned on a previous tick is still running, just
            // put it back; we'll wait for the next tick.
            if !task.is_finished() {
                self.on_tick_task = Some(task);
                return;
            }

            // Our last `on_tick` task is done; get its result, and possibly
            // update `self.last_reported` (if we successfully reported
            // progress to a peer).
            if let Some(last_reported) = task.await.unwrap() {
                self.last_reported = last_reported;
            }
        }

        // Spawn a task to do the actual work, which may take considerable time.
        self.on_tick_task = Some(self.spawn_on_tick_task());
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReportProgressBackend {
    log: slog::Logger,
    imp: Arc<dyn ReportProgressImpl>,
}

impl ReportProgressBackend {
    pub(crate) fn new<P: ReportProgressImpl + 'static>(
        log: &slog::Logger,
        imp: P,
    ) -> Self {
        let log = log.new(slog::o!("component" => "ReportProgressBackend"));
        Self { log, imp: Arc::new(imp) }
    }

    pub(crate) async fn discover_peers(
        &self,
    ) -> Result<Vec<PeerAddress>, DiscoverPeersError> {
        let log = self.log.new(slog::o!("task" => "discover_peers"));
        slog::debug!(log, "discovering peers");

        self.imp.discover_peers().await
    }

    pub(crate) async fn send_report_to_peer(
        &self,
        peer: PeerAddress,
        update_id: Uuid,
        report: EventReport,
    ) -> Result<SendReportStatus, ClientError> {
        let log = self.log.new(slog::o!("peer" => peer.to_string()));
        // For each peer, report it to the network.
        match self.imp.report_progress_impl(peer, update_id, report).await {
            Ok(()) => {
                slog::debug!(log, "sent report to peer");
                Ok(SendReportStatus::Processed)
            }
            Err(err) => {
                // Error 422 means that the server didn't accept the update ID.
                if err.status() == Some(StatusCode::UNPROCESSABLE_ENTITY) {
                    slog::debug!(
                        log,
                        "received HTTP 422 Unprocessable Entity \
                         for update ID {update_id} (returning UnknownUpdateId)",
                    );
                    Ok(SendReportStatus::UnknownUpdateId)
                } else if err.status() == Some(StatusCode::GONE) {
                    // XXX If we establish a 1:1 relationship between a
                    // particular instance of wicketd and installinator, 410
                    // Gone can be used to abort the update. But we don't have
                    // that kind of relationship at the moment.
                    slog::warn!(
                        log,
                        "received HTTP 410 Gone for update ID {update_id} \
                         (peer closed, returning PeerFinished)",
                    );
                    Ok(SendReportStatus::PeerFinished)
                } else {
                    slog::warn!(
                        log,
                        "received HTTP error code {:?} for update ID {update_id}",
                        err.status()
                    );
                    Err(err)
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[must_use]
pub(crate) enum SendReportStatus {
    /// The peer accepted the report.
    Processed,

    /// The peer could not process the report.
    UnknownUpdateId,

    /// The peer indicated that it had finished processing all reports.
    PeerFinished,
}

#[async_trait]
pub(crate) trait ReportProgressImpl: fmt::Debug + Send + Sync {
    async fn discover_peers(
        &self,
    ) -> Result<Vec<PeerAddress>, DiscoverPeersError>;

    async fn report_progress_impl(
        &self,
        peer: PeerAddress,
        update_id: Uuid,
        report: EventReport,
    ) -> Result<(), ClientError>;
}

#[derive(Debug)]
pub(crate) struct HttpProgressBackend {
    log: slog::Logger,
    discovery: DiscoveryMechanism,
}

impl HttpProgressBackend {
    pub(crate) fn new(
        log: &slog::Logger,
        discovery: DiscoveryMechanism,
    ) -> Self {
        let log = log.new(slog::o!("component" => "HttpProgressBackend"));
        Self { log, discovery }
    }
}

#[async_trait]
impl ReportProgressImpl for HttpProgressBackend {
    async fn discover_peers(
        &self,
    ) -> Result<Vec<PeerAddress>, DiscoverPeersError> {
        self.discovery.discover_peers(&self.log).await
    }

    async fn report_progress_impl(
        &self,
        peer: PeerAddress,
        update_id: Uuid,
        report: EventReport,
    ) -> Result<(), ClientError> {
        let artifact_client = ArtifactClient::new(peer.address(), &self.log);
        artifact_client.report_progress(update_id, report).await
    }
}
