// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code to report events to the artifact server.

use std::{collections::VecDeque, time::Duration};

use display_error_chain::DisplayErrorChain;
use futures::{Future, StreamExt};
use installinator_common::{ProgressReport, ReportEvent, ReportEventKind};
use tokio::{
    sync::mpsc,
    time::{self, Instant},
};
use uuid::Uuid;

use crate::{errors::DiscoverPeersError, peers::Peers};

#[derive(Debug)]
pub(crate) struct EventReporter<F> {
    log: slog::Logger,
    update_id: Uuid,
    start: Instant,
    discover_fn: F,
    // Receives updates about progress and completion.
    event_receiver: mpsc::Receiver<ReportEventKind>,
    last_progress: Option<ReportEvent>,
    completion_queue: Vec<ReportEvent>,
}

impl<F, Fut> EventReporter<F>
where
    F: 'static + Send + FnMut() -> Fut,
    Fut: Future<Output = Result<Peers, DiscoverPeersError>> + Send,
{
    pub(crate) fn new(
        log: &slog::Logger,
        update_id: Uuid,
        discover_fn: F,
    ) -> (Self, mpsc::Sender<ReportEventKind>) {
        let (event_sender, event_receiver) = mpsc::channel(32);
        let ret = Self {
            log: log.new(slog::o!("component" => "EventReporter")),
            update_id,
            start: Instant::now(),
            discover_fn,
            event_receiver,
            last_progress: None,
            completion_queue: Vec::with_capacity(8),
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
                            let event = ReportEvent {
                                total_elapsed: self.start.elapsed(),
                                kind: event,
                            };
                            if event.kind.is_completion_event() {
                                self.completion_queue.push(event);
                                // Reset progress: we don't want to send
                                // progress notifications after completion ones
                                // for the same artifact.
                                self.last_progress = None;
                            } else {
                                self.last_progress = Some(event);
                            }
                        } else {
                            // The completion sender has been dropped; this is
                            // an indication to drain the completion queue and
                            // end the loop.
                            events_done = true;
                        };
                    }
                }

                if events_done && self.completion_queue.is_empty() {
                    // All done, now exit.
                    break;
                }
            }
        })
    }

    async fn on_tick(&mut self) {
        // Assemble a report out of pending completion and progress events.
        let events = self
            .completion_queue
            .clone()
            .into_iter()
            .chain(self.last_progress.clone())
            .collect();
        let report =
            ProgressReport { total_elapsed: self.start.elapsed(), events };

        let peers = match (self.discover_fn)().await {
            Ok(peers) => peers,
            Err(error) => {
                // Ignore DiscoverPeersError::Abort here because the
                // installinator must keep retrying.
                slog::warn!(
                    self.log,
                    "failed to discover peers: {}",
                    DisplayErrorChain::new(&error),
                );
                return;
            }
        };

        // We could use StreamExt::any() here, but we're choosing to avoid
        // short-circuiting for now in favor of reaching out to all peers.
        //
        // TODO: is this correct? If a peer takes too long this might end up
        // stalling updates for everyone -- feels wrong. The tradeoff is that if
        // two servers both say, only one of them will deterministically get the
        // update. Need to decide post-PVT1.
        let results: Vec<_> =
            peers.broadcast_report(self.update_id, report).collect().await;
        if results.iter().any(|res| res.is_ok()) {
            // Reset the state.
            self.completion_queue.clear();
            self.last_progress = None;
        }
    }
}
