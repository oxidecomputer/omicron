// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The general framework for one of the service-specific reconciler tasks
//! implemented in this crate.
//!
//! Each reconciler follows the same structure:
//!
//! 1. If we ever stop being a scrimlet (i.e., the attached switch goes away),
//!    go inert until we become a scrimlet again (i.e., the switch reappears).
//!    This can happen during sidecar updates if it powers off briefly to reset
//!    internal FPGAs, or in a variety of other less common and more rainy-day
//!    situations.
//! 2. Periodically or when the `SystemNetworkingConfig` changes, perform
//!    service-specific reconciliation. This is provided by implementors of the
//!    [`Reconciler`] trait elsewhere in this crate.
//! 3. Report status of this task in an output watch channel, suitable for
//!    reporting in the sled-agent inventory.
//!
//! [`ReconcilerTask::run()`] handles 1 and 3, and service-specific
//! implementations of [`Reconciler`] provide 2.

use crate::handle::ScrimletReconcilersMode;
use crate::status::ReconcilerActivationReason;
use crate::status::ReconcilerCurrentStatus;
use crate::status::ReconcilerInertReason;
use crate::status::ReconcilerRunningStatus;
use crate::status::ReconcilerStatus;
use crate::status::ReconciliationCompletedStatus;
use crate::status::ScrimletStatus;
use crate::status::UndeterminedSwitchSlotReason;
use crate::switch_zone_slot::DetermineSwitchSlotStatus;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use chrono::SecondsFormat;
use chrono::Utc;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog::error;
use slog::info;
use std::convert::Infallible;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;
use tokio::sync::watch::error::RecvError;
use tokio::task::JoinHandle;

/// Trait that should be implemented by the service-specific reconciler tasks
/// elsewhere in this crate.
pub(crate) trait Reconciler: Send + 'static {
    type Status: slog::KV + Clone + Send + Sync + 'static;

    const LOGGER_COMPONENT_NAME: &'static str;
    const RE_RECONCILE_INTERVAL: Duration;

    /// Construct a new instance of this `Reconciler`.
    ///
    /// Typically builds a client for the relevant service based on `mode`.
    fn new(mode: ScrimletReconcilersMode, parent_log: &Logger) -> Self;

    /// Perform any required reconciliation based on the current contents of
    /// `system_networking_config`.
    ///
    /// This method is infallible; any errors must be described by
    /// `Self::Status`.
    fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
        switch_slot: ThisSledSwitchSlot,
        log: &Logger,
    ) -> impl Future<Output = Self::Status> + Send;
}

#[derive(Debug)]
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
        determine_switch_slot_rx: watch::Receiver<DetermineSwitchSlotStatus>,
        system_networking_config_rx: watch::Receiver<SystemNetworkingConfig>,
        mode: ScrimletReconcilersMode,
        parent_log: &Logger,
    ) -> Self {
        Self::spawn_impl(
            scrimlet_status_rx,
            determine_switch_slot_rx,
            system_networking_config_rx,
            mode,
            parent_log,
            T::new,
        )
    }

    // Separate, private function that allows unit tests to customize how `T` is
    // constructed. Production passes `T::new` as `inner_constructor`; i.e.,
    // just call the constructor we know exists from the `Reconciler` trait.
    fn spawn_impl<F>(
        scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
        determine_switch_slot_rx: watch::Receiver<DetermineSwitchSlotStatus>,
        system_networking_config_rx: watch::Receiver<SystemNetworkingConfig>,
        mode: ScrimletReconcilersMode,
        parent_log: &Logger,
        inner_constructor: F,
    ) -> Self
    where
        F: FnOnce(ScrimletReconcilersMode, &Logger) -> T
            + Send
            + Sync
            + 'static,
    {
        let (status_tx, status_rx) = watch::channel(ReconcilerStatus {
            current_status: ReconcilerCurrentStatus::Idle,
            last_completion: None,
        });

        let log = parent_log
            .new(slog::o!("scrimlet_reconciler" => T::LOGGER_COMPONENT_NAME));

        let mut inner_task = ReconcilerTask {
            scrimlet_status_rx,
            determine_switch_slot_rx,
            system_networking_config_rx,
            status_tx,
            inner: inner_constructor(mode, parent_log),
            log,
        };

        let task = tokio::spawn(async move {
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
                        "exited due to watch channel closure \
                         (unexpected except during shutdown in tests)"
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
    determine_switch_slot_rx: watch::Receiver<DetermineSwitchSlotStatus>,
    system_networking_config_rx: watch::Receiver<SystemNetworkingConfig>,
    status_tx: watch::Sender<ReconcilerStatus<T::Status>>,
    inner: T,
    log: Logger,
}

impl<T: Reconciler> ReconcilerTask<T> {
    async fn run(&mut self) -> Result<Infallible, RecvError> {
        let mut activation_reason = ReconcilerActivationReason::Startup;
        let mut activation_count: u64 = 0;

        loop {
            // Wait until we're a scrimlet and know our switch slot.
            //
            // Non-scrimlet sleds will never move beyond this point. Scrimlets
            // may block here if either (a) we've not yet successfully contacted
            // MGS to determine our switch slot or (b) we're no longer a
            // scrimlet (i.e., a previously-attached switch has gone away at
            // runtime).
            let switch_slot = self.wait_until_this_sled_is_a_scrimlet().await?;

            // We _are_ a scrimlet; perform reconciliation.
            let start_instant = Instant::now();
            let start_time = Utc::now();
            info!(
                self.log, "starting reconciliation attempt";
                "activation_reason" => ?activation_reason,
                "activation_count" => activation_count,
            );

            // Snapshot the current networking config so we hold the watch
            // channel as little as possible.
            let system_networking_config =
                self.system_networking_config_rx.borrow_and_update().clone();
            let running_status =
                ReconcilerRunningStatus::new(activation_reason);

            // Update our status.
            self.status_tx.send_modify(|status| {
                status.current_status =
                    ReconcilerCurrentStatus::Running(running_status);
            });

            // Actually perform reconciliation.
            let status_result = self
                .inner
                .do_reconciliation(
                    &system_networking_config,
                    switch_slot,
                    &self.log,
                )
                .await;

            // Update our output watch channel with the result.
            info!(
                self.log, "reconciliation attempt complete";
                "activation_reason" => ?activation_reason,
                "activation_count" => activation_count,
                "started_at" => start_time.to_rfc3339_opts(
                    SecondsFormat::Millis,
                    /* use_z */ true,
                ),
                "elapsed" => ?start_instant.elapsed(),
                &status_result,
            );
            self.status_tx.send_modify(|status| {
                status.current_status = ReconcilerCurrentStatus::Idle;
                status.last_completion =
                    Some(Box::new(ReconciliationCompletedStatus {
                        activation_reason,
                        completed_at_time: Utc::now(),
                        ran_for: running_status.elapsed_since_start(),
                        activation_count,
                        status: status_result,
                    }));
            });
            activation_count = activation_count.wrapping_add(1);

            // Wait until we should perform reconciliation again: our
            // re-reconciliation periodic timer fires or one of our input watch
            // channels changes.
            //
            // All arms are cancel-safe and we do not `.await` within the body
            // of any arm, avoiding any opportunity for futurelock.
            activation_reason = tokio::select! {
                () = tokio::time::sleep(T::RE_RECONCILE_INTERVAL) => {
                    ReconcilerActivationReason::PeriodicTimer
                }

                result = self.system_networking_config_rx.changed() => {
                    () = result?;
                    ReconcilerActivationReason::SystemNetworkingConfigChanged
                }

                result = self.scrimlet_status_rx.changed() => {
                    () = result?;
                    ReconcilerActivationReason::ScrimletStatusChanged
                }
            };
        }
    }

    async fn wait_until_this_sled_is_a_scrimlet(
        &mut self,
    ) -> Result<ThisSledSwitchSlot, RecvError> {
        let mut logged_not_scrimlet = false;

        'check_status: loop {
            let status = *self.scrimlet_status_rx.borrow_and_update();
            match status {
                ScrimletStatus::Scrimlet => {
                    return self.wait_for_switch_slot().await;
                }
                ScrimletStatus::NotScrimlet => {
                    if !logged_not_scrimlet {
                        info!(
                            self.log,
                            "not a scrimlet - reconciler going inert"
                        );
                        logged_not_scrimlet = true;
                    }
                    self.status_tx.send_modify(|status| {
                        status.current_status = ReconcilerCurrentStatus::Inert(
                            ReconcilerInertReason::NotScrimlet,
                        );
                    });

                    // Select over all input channels so we can detect channel
                    // closure and exit cleanly if any channel goes away. We
                    // only loop back to the top ('check_status) if the
                    // `scrimlet_status_rx` channel changes; for other channel
                    // changes, we stay here and keep waiting.
                    //
                    // All arms are cancel-safe and we do not `.await` within
                    // the body of any arm, avoiding any opportunity for
                    // futurelock.
                    'wait_for_status_changed: loop {
                        tokio::select! {
                            res = self.scrimlet_status_rx.changed() => {
                                () = res?;
                                continue 'check_status;
                            }
                            res = self.system_networking_config_rx.changed() => {
                                () = res?;
                                continue 'wait_for_status_changed;
                            }
                            res = self.determine_switch_slot_rx.changed() => {
                                () = res?;
                                continue 'wait_for_status_changed;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn wait_for_switch_slot(
        &mut self,
    ) -> Result<ThisSledSwitchSlot, RecvError> {
        'check_status: loop {
            let switch_slot_status =
                self.determine_switch_slot_rx.borrow_and_update().clone();

            // Either return if we're done, or get the reason we're not.
            let inert_reason = match switch_slot_status {
                DetermineSwitchSlotStatus::Done { switch_slot } => {
                    return Ok(switch_slot);
                }
                DetermineSwitchSlotStatus::NotScrimlet => {
                    ReconcilerInertReason::NotScrimlet
                }
                DetermineSwitchSlotStatus::ContactingMgs {
                    prev_attempt_err,
                } => ReconcilerInertReason::UndeterminedSwitchSlot {
                    reason: UndeterminedSwitchSlotReason::ContactingMgs {
                        prev_attempt_err,
                    },
                },
                DetermineSwitchSlotStatus::WaitingToRetry {
                    prev_attempt_err,
                } => ReconcilerInertReason::UndeterminedSwitchSlot {
                    reason: UndeterminedSwitchSlotReason::WaitingToRetry {
                        prev_attempt_err,
                    },
                },
            };

            // Propogate why we're still inert.
            self.status_tx.send_modify(|status| {
                status.current_status =
                    ReconcilerCurrentStatus::Inert(inert_reason);
            });

            // Select over all input channels so we can detect channel
            // closure and exit cleanly if any channel goes away. We
            // only loop back to the top ('check_status) if the
            // `determine_switch_slot_rx` channel changes; for other channel
            // changes, we stay here and keep waiting.
            //
            // All arms are cancel-safe and we do not `.await` within
            // the body of any arm, avoiding any opportunity for
            // futurelock.
            'wait_for_status_changed: loop {
                tokio::select! {
                    res = self.determine_switch_slot_rx.changed() => {
                        () = res?;
                        continue 'check_status;
                    }
                    res = self.scrimlet_status_rx.changed() => {
                        () = res?;
                        continue 'wait_for_status_changed;
                    }
                    res = self.system_networking_config_rx.changed() => {
                        () = res?;
                        continue 'wait_for_status_changed;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
