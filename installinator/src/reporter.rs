// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code to report events to the artifact server.

use std::time::Duration;

use display_error_chain::DisplayErrorChain;
use futures::{Future, StreamExt};
use installinator_common::{Event, EventBuffer};
use tokio::{sync::mpsc, task::JoinHandle, time};
use uuid::Uuid;

use crate::{errors::DiscoverPeersError, peers::Peers};

#[derive(Debug)]
pub(crate) struct ProgressReporter<F> {
    log: slog::Logger,
    update_id: Uuid,
    discover_fn: F,
    // Receives updates about progress and completion.
    event_receiver: mpsc::Receiver<Event>,
    buffer: EventBuffer,
    last_reported: Option<usize>,
    on_tick_task: Option<JoinHandle<Option<Option<usize>>>>,
}

impl<F, Fut> ProgressReporter<F>
where
    F: 'static + Clone + Send + FnMut() -> Fut,
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
            discover_fn,
            event_receiver,
            // We have to keep max_low_priority low since a bigger number will
            // cause a payload that's too large.
            buffer: EventBuffer::new(8),
            last_reported: None,
            on_tick_task: None,
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
        let mut discover_fn = self.discover_fn.clone();
        let update_id = self.update_id;
        let log = self.log.clone();

        tokio::spawn(async move {
            let peers = match (discover_fn)().await {
                Ok(peers) => peers,
                Err(error) => {
                    // Ignore DiscoverPeersError::Abort here because the
                    // installinator must keep retrying.
                    slog::warn!(
                        log,
                        "failed to discover peers: {}",
                        DisplayErrorChain::new(&error),
                    );
                    return None;
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
            let results: Vec<_> =
                peers.broadcast_report(update_id, report).collect().await;
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
