// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The general framework for one of the service-specific reconciler tasks
//! implemented in this crate.
//!
//! Each reconciler follows the same general structure:
//!
//! 1. Wait until we have enough information to begin: the `RackNetworkConfig`
//!    and our sled's switch zone underlay IP, both of which must be provided by
//!    `sled-agent`; neither is available until after RSS has completed (on
//!    first setup) or the rack has unlocked (on cold boot).
//! 2. Remain inert unless we are a scrimlet. `sled-agent` must notify us when
//!    we become a scrimlet or stop being a scrimlet; this is directly
//!    downstream from its hardware monitor notifying the presence or absence of
//!    an attached switch.
//! 3. Periodically or when the `RackNetworkConfig` changes, perform
//!    service-specific reconciliation. This is provided by implementors of the
//!    [`Reconciler`] trait elsewhere in this crate.
//! 4. Report status of this task in an output watch channel, suitable for
//!    reporting in the sled-agent inventory.
//!
//! [`ReconcilerTask`] in this module handles 1, 2, and 4; service-specific
//! implementations must provide 3.

use crate::ScrimletReconcilersPrereqs;
use crate::ThisSledSwitchZoneUnderlayIpAddr;
use crate::status::ReconcilerActivationReason;
use crate::status::ReconcilerCurrentStatus;
use crate::status::ReconcilerInertReason;
use crate::status::ReconcilerRunningStatus;
use crate::status::ReconcilerStatus;
use crate::status::ReconciliationCompletedStatus;
use crate::status::ScrimletStatus;
use chrono::Utc;
use sled_agent_types::early_networking::RackNetworkConfig;
use slog::Logger;
use slog::error;
use slog::info;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::SetOnce;
use tokio::sync::watch;
use tokio::sync::watch::error::RecvError;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

/// Trait that should be implemented by the service-specific reconciler tasks
/// elsewhere in this crate.
pub(crate) trait Reconciler: Send + 'static {
    type Status: Clone + Send + Sync + 'static;

    const LOGGER_COMPONENT_NAME: &'static str;
    const RE_RECONCILE_INTERVAL: Duration;

    /// Construct a new instance of this `Reconciler`.
    ///
    /// Typically builds a client for the relevant service based on
    /// `switch_zone_underlay_ip`.
    fn new(
        switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
        parent_log: &Logger,
    ) -> Self;

    /// Perform any required reconciliation based on the current contents of
    /// `rack_network_config`.
    ///
    /// This method is infallible; any errors must be described by
    /// `Self::Status`.
    fn do_reconciliation(
        &mut self,
        rack_network_config: &RackNetworkConfig,
        log: &Logger,
    ) -> impl Future<Output = Self::Status> + Send;
}

pub(crate) struct ReconcilerTaskHandle<T: Reconciler> {
    status_rx: watch::Receiver<ReconcilerStatus<T::Status>>,

    // We never wait on this task: the only way it exits is by panicking (which
    // results in a process abort, since we build sled-agent with panic=abort)
    // or if the watch channels it's reading are dropped, which itself can only
    // happen by a panic elsewhere.
    _task: JoinHandle<()>,
}

impl<T: Reconciler> ReconcilerTaskHandle<T> {
    pub(crate) fn spawn(
        scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
        prereqs: Arc<SetOnce<ScrimletReconcilersPrereqs>>,
        parent_log: &Logger,
    ) -> Self {
        Self::spawn_with_inner_constructor(
            scrimlet_status_rx,
            prereqs,
            parent_log,
            T::new,
        )
    }

    // Separate, private function that allows unit tests to customize how `T` is
    // constructed. Production passes `T::new` as `inner_constructor`; i.e.,
    // just call the constructor we know exists from the `Reconciler` trait.
    fn spawn_with_inner_constructor<F>(
        scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
        prereqs: Arc<SetOnce<ScrimletReconcilersPrereqs>>,
        parent_log: &Logger,
        inner_constructor: F,
    ) -> Self
    where
        F: FnOnce(ThisSledSwitchZoneUnderlayIpAddr, &Logger) -> T
            + Send
            + Sync
            + 'static,
    {
        let (status_tx, status_rx) = watch::channel(ReconcilerStatus {
            current_status: ReconcilerCurrentStatus::Inert(
                ReconcilerInertReason::WaitingForPrereqs,
            ),
            last_completion: None,
        });
        let log =
            parent_log.new(slog::o!("component" => T::LOGGER_COMPONENT_NAME));

        let parent_log = parent_log.clone();
        let task = tokio::spawn(async move {
            // Wait for sled-agent to give us our prereqs to really start this
            // task.
            info!(
                log,
                "task started; waiting for RackNetworkConfig and \
                 switch zone underlay IP"
            );
            let prereqs = prereqs.wait().await.clone();

            let ScrimletReconcilersPrereqs {
                rack_network_config_rx,
                switch_zone_underlay_ip,
            } = prereqs;

            let mut inner_task = ReconcilerTask {
                scrimlet_status_rx,
                rack_network_config_rx,
                status_tx,
                inner: inner_constructor(switch_zone_underlay_ip, &parent_log),
                log,
            };

            match inner_task.run().await {
                // `inner_task.run()` runs forever...
                Ok(never_returns) => match never_returns {},

                // ... unless one of its input watch channels has closed.
                Err(_recv_error) => {
                    inner_task.status_tx.send_modify(|status| {
                        status.current_status = ReconcilerCurrentStatus::Inert(
                            ReconcilerInertReason::TaskExitedUnexpectedly,
                        );
                    });
                    error!(
                        inner_task.log,
                        "exited due to watch channel closure (unexpected \
                         except during tokio runtime shutdown in tests)"
                    );
                }
            }
        });

        Self { status_rx, _task: task }
    }

    pub(crate) fn status(&self) -> ReconcilerStatus<T::Status> {
        self.status_rx.borrow().clone()
    }
}

struct ReconcilerTask<T: Reconciler> {
    scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
    rack_network_config_rx: watch::Receiver<RackNetworkConfig>,
    status_tx: watch::Sender<ReconcilerStatus<T::Status>>,
    inner: T,
    log: Logger,
}

impl<T: Reconciler> ReconcilerTask<T> {
    async fn run(&mut self) -> Result<Infallible, RecvError> {
        // Set up our timer, but we won't poll it until after we've performed
        // one reconciliation, so also reset it to not fire immediately.
        let mut re_reconcile_interval =
            tokio::time::interval(T::RE_RECONCILE_INTERVAL);
        re_reconcile_interval.reset();

        // If we miss a tick because the rack network config changed, just delay
        // and wait the full re-reconciliation interval before firing again.
        re_reconcile_interval
            .set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut activation_reason = ReconcilerActivationReason::Startup;
        let mut activation_count: u64 = 0;

        loop {
            // Are we a scrimlet? Most sleds aren't; on those, this blocks
            // indefinitely, leaving us in the inert status. On sleds that are
            // scrimlets, this will either return immediately (if we already
            // know we're a scrimlet) or as soon as we know we're a scrimlet;
            // ultimately this comes from `sled-agent` noticing the presence of
            // a switch and activating the switch zone (and these reconciler
            // tasks).
            self.wait_if_this_sled_is_not_a_scrimlet().await?;

            // We _are_ a scrimlet; perform reconciliation.
            info!(
                self.log, "starting reconciliation attempt";
                "activation_reason" => ?activation_reason,
                "activation_count" => activation_count,
            );

            // Snapshot the current rack network config so we hold the watch
            // channel as little as possible.
            let rack_network_config =
                self.rack_network_config_rx.borrow_and_update().clone();
            let running_status =
                ReconcilerRunningStatus::new(activation_reason);
            self.status_tx.send_modify(|status| {
                status.current_status =
                    ReconcilerCurrentStatus::Running(running_status);
            });

            // Actually perform reconciliation.
            let status_result = self
                .inner
                .do_reconciliation(&rack_network_config, &self.log)
                .await;

            // Update our output watch channel with the result.
            info!(
                self.log, "reconciliation attempt complete";
                "activation_reason" => ?activation_reason,
                "attempt_count" => activation_count,
            );
            self.status_tx.send_modify(|status| {
                status.current_status = ReconcilerCurrentStatus::Idle;
                status.last_completion = Some(ReconciliationCompletedStatus {
                    activation_reason,
                    completed_at_time: Utc::now(),
                    ran_for: running_status.elapsed_since_start(),
                    activation_count,
                    status: status_result,
                });
            });
            activation_count = activation_count.wrapping_add(1);

            // Wait until we should perform reconciliation again; either our
            // periodic timer fired or the rack network config changed.
            //
            // Both arms are cancel-safe and we do not `.await` within the body
            // of any arm, avoiding any opportunity for futurelock.
            activation_reason = tokio::select! {
                _ = re_reconcile_interval.tick() => {
                    ReconcilerActivationReason::PeriodicTimer
                }

                result = self.rack_network_config_rx.changed() => {
                    () = result?;
                    ReconcilerActivationReason::RackNetworkConfigChanged
                }

                result = self.scrimlet_status_rx.changed() => {
                    () = result?;
                    ReconcilerActivationReason::ScrimletStatusChanged
                }
            };
        }
    }

    async fn wait_if_this_sled_is_not_a_scrimlet(
        &mut self,
    ) -> Result<(), RecvError> {
        loop {
            let status = *self.scrimlet_status_rx.borrow_and_update();
            match status {
                ScrimletStatus::Scrimlet => {
                    return Ok(());
                }
                ScrimletStatus::NotScrimlet => {
                    self.status_tx.send_modify(|status| {
                        status.current_status = ReconcilerCurrentStatus::Inert(
                            ReconcilerInertReason::NotAScrimlet,
                        );
                    });
                    self.scrimlet_status_rx.changed().await?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
