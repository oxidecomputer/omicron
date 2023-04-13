// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code to report events to the artifact server.

use std::time::Duration;

use display_error_chain::DisplayErrorChain;
use futures::{Future, StreamExt};
use installinator_common::{Event, ProgressEvent, ProgressReport, StepEvent};
use tokio::{
    sync::mpsc,
    time::{self, Instant},
};
use update_engine::events::StepEventPriority;
use uuid::Uuid;

use crate::{errors::DiscoverPeersError, peers::Peers};

#[derive(Debug)]
pub(crate) struct ProgressReporter<F> {
    log: slog::Logger,
    update_id: Uuid,
    start: Instant,
    discover_fn: F,
    // Receives updates about progress and completion.
    event_receiver: mpsc::Receiver<Event>,
    step: Vec<StepEvent>,
    last_progress: Option<ProgressEvent>,
}

impl<F, Fut> ProgressReporter<F>
where
    F: 'static + Send + FnMut() -> Fut,
    Fut: Future<Output = Result<Peers, DiscoverPeersError>> + Send,
{
    pub(crate) fn new(
        log: &slog::Logger,
        update_id: Uuid,
        discover_fn: F,
    ) -> (Self, mpsc::Sender<Event>) {
        // Set a large enough buffer that it filling up isn't an actual problem
        // outside of something going horribly wrong.
        let (event_sender, event_receiver) = mpsc::channel(512);
        let ret = Self {
            log: log.new(slog::o!("component" => "EventReporter")),
            update_id,
            start: Instant::now(),
            discover_fn,
            event_receiver,
            step: Vec::with_capacity(8),
            last_progress: None,
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
                        match event {
                            Some(Event::Step(event)) => {
                                self.step.push(event);
                                // Reset progress: we don't want to send
                                // progress notifications after completion ones
                                // for the same artifact.
                                self.last_progress = None;
                            }
                            Some(Event::Progress(event)) => {
                                self.last_progress = Some(event);
                            }
                            None => {
                                // The completion sender has been dropped; this
                                // is an indication to drain the completion
                                // queue and end the loop.
                                events_done = true;
                            }
                        }
                    }
                }

                if events_done && self.step.is_empty() {
                    // All done, now exit.
                    break;
                }
            }
        })
    }

    async fn on_tick(&mut self) {
        // Assemble a report out of pending completion and progress events. For
        // completion, include all step events other than the last 20 retry
        // events (with the goal being to keep the size of the report down,
        // since a report that's too large will cause the max payload size to be
        // exceeded.)
        let mut failure_events_seen = 0;
        let mut step_events: Vec<_> = self
            .step
            .iter()
            .rev()
            .filter(|event| {
                if event.kind.priority() >= StepEventPriority::High {
                    true
                } else {
                    failure_events_seen += 1;
                    failure_events_seen <= 20
                }
            })
            .cloned()
            .collect();
        // STEP: You might be tempted to replace this reverse call with
        // another `.rev()` above. Don't do that! It will cause the *first* 20
        // elements to be taken rather than the *last* 20 elements.
        step_events.reverse();

        let progress_events = self.last_progress.clone().into_iter().collect();
        let report = ProgressReport {
            total_elapsed: self.start.elapsed(),
            step_events,
            progress_events,
        };

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
            self.step.clear();
            self.last_progress = None;
        }
    }
}
