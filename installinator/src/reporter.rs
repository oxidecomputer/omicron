// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code to report events to the artifact server.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use cancel_safe_futures::coop_cancel;
use daft::Diffable;
use display_error_chain::DisplayErrorChain;
use http::StatusCode;
use installinator_client::ClientError;
use installinator_common::{Event, EventBuffer, EventReport};
use omicron_uuid_kinds::MupdateUuid;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time,
};
use update_engine::AsError;

use crate::{
    artifact::ArtifactClient,
    errors::DiscoverPeersError,
    peers::{DiscoveryMechanism, PeerAddress, PeerAddresses},
};

const REPORT_INTERVAL: Duration = Duration::from_secs(2);
const DISCOVER_INTERVAL: Duration = Duration::from_secs(5);

/// A progress reporter that sends progress updates to a backend.
///
/// # Design
///
/// The progress reporter works against a [`ReportProgressBackend`], and spins
/// up three kinds of tasks to do its work:
///
/// 1. **The discovery task** is responsible for discovering peers
///    and publishing updates to the reconciler task.
/// 2. **The reconciler task** accepts updates from the discovery task, starts up
///    new report tasks for each new peer discovered, and cancels old report tasks
///    for peers that are no longer active.
/// 3. **The report task**, a separate one for each peer, sends progress updates
///    to that peer's backend.
///
/// Production users use [`HttpProgressBackend`] to send progress updates via
/// HTTP. For testing, there's also a mock backend in `crate::mock_peers` which
/// can inject faults.
#[derive(Debug)]
pub(crate) struct ProgressReporter {
    log: slog::Logger,
    update_id: MupdateUuid,
    report_backend: ReportProgressBackend,
    // Receives updates about progress and completion.
    event_receiver: mpsc::Receiver<Event>,
    buffer: EventBuffer,
    last_reported: Option<usize>,
    discover_task: Option<JoinHandle<()>>,
    reconcile_peers_task: Option<JoinHandle<()>>,
}

impl ProgressReporter {
    pub(crate) fn new(
        log: &slog::Logger,
        update_id: MupdateUuid,
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
            discover_task: None,
            reconcile_peers_task: None,
            report_backend,
        };
        (ret, event_sender)
    }

    /// Starts the event reporter in a new Tokio task.
    ///
    /// The returned task handle exits once at least one peer has accepted a
    /// terminal event report.
    pub(crate) fn start(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            // TODO: jitter?
            let mut report_interval = time::interval(REPORT_INTERVAL);
            report_interval
                .set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            // Do not tick report_interval immediately. We'd like to generate a
            // report at the start of the loop to ensure that report_tx is
            // initialized right away and that new peers receive reports ASAP.

            let (peers_tx, peers_rx) = watch::channel(None);
            let (discover_cancel_tx, discover_cancel_rx) =
                coop_cancel::new_pair();
            let (report_tx, report_rx) = watch::channel(None);
            let (completion_tx, mut completion_rx) = mpsc::channel(1);
            self.discover_task =
                Some(self.spawn_discover_task(peers_tx, discover_cancel_rx));
            self.reconcile_peers_task =
                Some(self.spawn_reconcile_peers_task(peers_rx, report_rx));

            let mut reports_done = false;
            let mut events_done = false;

            // The goal of this loop is to:
            //
            // * process all events
            // * get a fully completed report out to at least one peer which
            //   accepts this report
            //
            // The state machine is implemented via a `tokio::select` loop. The
            // general logic is:
            //
            // * on receiving an event from the update engine
            //   - add event to self.buffer
            //   - if event receiver is closed, the update engine has completed, so
            //     mark events_done = true, and indicate that the next report should
            //     be sent right away
            //
            // * on report_interval tick:
            //   - generate report
            //   - if events_done is true, mark report as complete and pass in a
            //     completion channel sender
            //   - send message to report tasks
            //
            // * on receiving completion channel message (indicating that at
            //   least one completed report has been received by a peer):
            //   - mark reports_done = true
            //
            // If events_done and reports_done are true, then the loop's goal is
            // met, so exit the loop.
            while !(events_done && reports_done) {
                tokio::select! {
                    event = self.event_receiver.recv(), if !events_done => {
                        if let Some(event) = event {
                            self.buffer.add_event(event);
                        } else {
                            // The event sender has been dropped; this is an
                            // indication to send one final report. In order to
                            // do so, reset the report interval to now so that
                            // the next loop iteration can send the report right
                            // away.
                            events_done = true;
                            report_interval.reset_immediately();
                        }
                    }

                    _ = report_interval.tick(), if !reports_done => {
                        // Generate a report and send it over the report
                        // channel.
                        let report = self.buffer.generate_report();
                        self.last_reported = report.last_seen;

                        let message = ReportMessage {
                            state: if events_done {
                                EventState::Finished {
                                    completion_tx: completion_tx.clone(),
                                }
                            } else {
                                EventState::InProgress
                            },
                            report,
                        };

                        if report_tx.send(Some(message)).is_err() {
                            // All report receivers have been dropped -- not
                            // much more to do here.
                            slog::info!(
                                self.log,
                                "all report receivers have been dropped, \
                                 marking reports done"
                            );
                            reports_done = true;
                        }
                    }

                    Some(peer) = completion_rx.recv() => {
                        // One peer accepted a completed report. Treat reports
                        // as done.
                        slog::info!(
                            self.log,
                            "completed report accepted by peer, \
                             marking reports_done";
                             "peer" => %peer,
                        );
                        reports_done = true;
                    }
                }
            }

            // At least one peer has accepted a completed report. The discovery
            // task can now be cancelled.
            _ = discover_cancel_tx.cancel(());
        })
    }

    /// Spawn a persistent task to discover peers.
    ///
    /// This task will attempt to discover peers and send updated lists of peers
    /// to the reconciler task.
    ///
    /// The task is persistent, and will continue to run until cancelled.
    fn spawn_discover_task(
        &self,
        peers_tx: watch::Sender<Option<PeerAddresses>>,
        mut cancel_rx: coop_cancel::Receiver<()>,
    ) -> JoinHandle<()> {
        let report_backend = self.report_backend.clone();
        let log = self.log.new(slog::o!("task" => "discover"));

        tokio::spawn(async move {
            let mut discover_task_loop = std::pin::pin!(discover_task_loop(
                &log,
                report_backend,
                peers_tx
            ));

            tokio::select! {
                _ = &mut discover_task_loop => {
                    slog::info!(log, "discover task completed");
                }
                _ = cancel_rx.recv() => {
                    // The task is cancelled; exit. This will cause report_tx to
                    // be dropped, which will cause the reconciler task to exit
                    // as well.
                    slog::info!(
                        log,
                        "cancellation received, cancelling discover task",
                    );
                    // It's fine to drop discover_task_loop at this point, since
                    // the only writes it does are publications to the peers_tx
                    // watch channel.
                }
            }
        })
    }

    /// Spawn a task to listen to lists of peers, and:
    ///
    /// * spawn new report tasks for peers that are new
    /// * cancel report tasks for peers that are no longer present
    ///
    /// This task is persistent and activates on changes to `peers_rx`.
    fn spawn_reconcile_peers_task(
        &self,
        mut peers_rx: watch::Receiver<Option<PeerAddresses>>,
        report_rx: watch::Receiver<Option<ReportMessage>>,
    ) -> JoinHandle<()> {
        let log = self.log.new(slog::o!("task" => "reconcile_peers"));
        let update_id = self.update_id;
        let backend = self.report_backend.clone();
        // A map of peers to their report tasks.
        let mut peer_tasks: BTreeMap<PeerAddress, ReportTask> = BTreeMap::new();

        tokio::spawn(async move {
            loop {
                // Listen to changes in the list of peers.
                let peers = match peers_rx.changed().await {
                    Ok(()) => peers_rx.borrow_and_update(),
                    Err(error) => {
                        slog::warn!(
                            log,
                            "failed to receive peers (sender dropped, \
                             discovery task likely completed), exiting: {}",
                            error
                        );
                        return;
                    }
                };

                let Some(peers) = peers.as_ref() else {
                    continue;
                };

                let before_peers =
                    peer_tasks.keys().cloned().collect::<BTreeSet<_>>();
                let after_peers = peers.peers().clone();
                let diff = before_peers.diff(&after_peers);

                // For each removed peer, cancel the report task.
                for peer in diff.removed {
                    let task = peer_tasks
                        .remove(&peer)
                        .expect("present in removed => present in peer_tasks");
                    _ = task.cancel_tx.cancel(());
                }

                // For each added pear, spawn a new report task.
                for peer in diff.added {
                    let task = ReportTask::new(
                        *peer,
                        update_id,
                        // Note: report_rx.clone() starts off as seen. However,
                        // the initial state of report_rx is None, so if there's
                        // an report in the receiver, it will always be treated
                        // as unseen. This means that if a report is available,
                        // the task will immediately dispatch it to the peer.
                        report_rx.clone(),
                        backend.clone(),
                    );
                    peer_tasks.insert(*peer, task);
                }
            }
        })
    }
}

async fn discover_task_loop(
    log: &slog::Logger,
    report_backend: ReportProgressBackend,
    peers_tx: watch::Sender<Option<PeerAddresses>>,
) {
    let mut discover_interval = time::interval(DISCOVER_INTERVAL);
    // MissedTickBehavior::Skip ensures that discovery happens every
    // interval, rather than bursting in case report_backend.discover_peers
    // takes a long time.
    discover_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    // Do not tick discover_interval immediately. We'd like one discovery to
    // happen at the start of the loop.

    loop {
        discover_interval.tick().await;
        let peers = match report_backend.discover_peers().await {
            Ok(peers) => peers,
            Err(DiscoverPeersError::Retry(error)) => {
                slog::warn!(
                    log,
                    "failed to discover peers, will retry: {}",
                    DisplayErrorChain::new(error.as_error()),
                );
                continue;
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

        slog::debug!(log, "discovered peers"; "peers" => ?peers);

        peers_tx.send_if_modified(|prev_peers| {
            if prev_peers.as_ref() == Some(&peers) {
                false
            } else {
                *prev_peers = Some(peers);
                true
            }
        });
    }
}

struct ReportTask {
    // This will be used to cancel the report task a bit more gracefully than
    // simply join_handle.abort().
    cancel_tx: coop_cancel::Canceler<()>,
}

impl ReportTask {
    fn new(
        peer: PeerAddress,
        update_id: MupdateUuid,
        report_rx: watch::Receiver<Option<ReportMessage>>,
        backend: ReportProgressBackend,
    ) -> ReportTask {
        let (cancel_tx, mut cancel_rx) = coop_cancel::new_pair();

        let log = backend
            .log
            .new(slog::o!("task" => "report", "peer" => peer.to_string()));
        slog::info!(log, "spawning new report task for peer");

        tokio::spawn(async move {
            // Note: we do not use a loop here, because we don't want to cancel
            // report_task_loop after every iteration.
            let mut report_task_loop = std::pin::pin!(report_task_loop(
                &log, peer, update_id, report_rx, backend
            ));

            tokio::select! {
                _ = &mut report_task_loop => {
                    slog::info!(log, "report task completed");
                }
                _ = cancel_rx.recv() => {
                    // The task is cancelled; exit. This will cancel any
                    // in-flight report requests, though we assume that if a
                    // peer disappears from the discovery mechanism, it is no
                    // longer reported.
                    slog::info!(log, "cancellation received, report task cancelled");
                }
            }
        });

        Self { cancel_tx }
    }
}

async fn report_task_loop(
    log: &slog::Logger,
    peer: PeerAddress,
    update_id: MupdateUuid,
    mut report_rx: watch::Receiver<Option<ReportMessage>>,
    backend: ReportProgressBackend,
) {
    // This loop is gated on report_rx updates, which only happen at every
    // REPORT_INTERVAL -- so reports will be sent at that interval.
    //
    // report_rx returning an error is a sign that report_tx was dropped, i.e.
    // that the discovery task completed or was cancelled.
    while let Ok(()) = report_rx.changed().await {
        // .cloned is required because borrow_and_update returns watch::Ref,
        // which isn't thread-safe.
        let Some(message) = report_rx.borrow_and_update().as_ref().cloned()
        else {
            continue;
        };

        // Report the event to the peer. Note that this will block until the
        // report is sent. In case the peer is unreachable, this will likely
        // result in a timeout.
        match backend.send_report_to_peer(peer, update_id, message.report).await
        {
            Ok(status) => {
                match status {
                    SendReportStatus::UnknownUpdateId => {
                        // The update ID is unknown. No point sending further
                        // updates to this peer.
                        slog::info!(
                            log,
                            "peer indicated that update ID is unknown, \
                             exiting report task"
                        );
                        break;
                    }
                    SendReportStatus::Processed
                    | SendReportStatus::PeerFinished => {
                        // The report was sent successfully. Exit if the task is
                        // finished.
                        if let EventState::Finished { completion_tx } =
                            message.state
                        {
                            slog::info!(
                                log,
                                "report task finished, sending completion event"
                            );
                            if completion_tx.send(peer).await.is_err() {
                                slog::warn!(
                                    log,
                                    "completion receiver dropped, failed to \
                                     send completion event",
                                );
                            }
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                slog::warn!(log, "failed to send report to peer: {}", error);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct ReportMessage {
    state: EventState,
    report: EventReport,
}

#[derive(Clone, Debug)]
enum EventState {
    InProgress,
    Finished {
        // Used to indicate that a completed report has been sent to at least
        // one peer.
        completion_tx: mpsc::Sender<PeerAddress>,
    },
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
    ) -> Result<PeerAddresses, DiscoverPeersError> {
        let log = self.log.new(slog::o!("task" => "discover_peers"));
        slog::debug!(log, "discovering peers");

        self.imp.discover_peers().await
    }

    pub(crate) async fn send_report_to_peer(
        &self,
        peer: PeerAddress,
        update_id: MupdateUuid,
        report: EventReport,
    ) -> Result<SendReportStatus, ClientError> {
        let log = self.log.new(slog::o!("peer" => peer.to_string()));
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
    async fn discover_peers(&self)
    -> Result<PeerAddresses, DiscoverPeersError>;

    async fn report_progress_impl(
        &self,
        peer: PeerAddress,
        update_id: MupdateUuid,
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
    ) -> Result<PeerAddresses, DiscoverPeersError> {
        self.discovery.discover_peers(&self.log).await
    }

    async fn report_progress_impl(
        &self,
        peer: PeerAddress,
        update_id: MupdateUuid,
        report: EventReport,
    ) -> Result<(), ClientError> {
        let artifact_client = ArtifactClient::new(peer.address(), &self.log);
        artifact_client.report_progress(update_id, report).await
    }
}
