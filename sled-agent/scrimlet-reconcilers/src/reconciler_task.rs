// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The general framework for one of the service-specific reconciler tasks
//! implemented in this crate.
//!
//! Each reconciler follows the same structure:
//!
//! 1. Wait until `sled-agent` gives us the `SystemNetworkingConfig` and our
//!    sled's switch zone underlay IP. Neither is available until after RSS has
//!    completed (on first setup) or the rack has unlocked (on cold boot).
//! 2. Wait to determine [`ThisSledSwitchSlot`]. This requires contacting MGS
//!    within our switch zone. Non-scrimlet sleds will block forever at this
//!    point.
//! 3. If we ever stop being a scrimlet (i.e., the attached switch goes away),
//!    go inert until we become a scrimlet again (i.e., the switch reappears).
//!    This can happen during sidecar updates if it powers off briefly to reset
//!    internal FPGAs, or in a variety of other less common and more rainy-day
//!    situations.
//! 4. Periodically or when the `SystemNetworkingConfig` changes, perform
//!    service-specific reconciliation. This is provided by implementors of the
//!    [`Reconciler`] trait elsewhere in this crate.
//! 5. Report status of this task in an output watch channel, suitable for
//!    reporting in the sled-agent inventory.
//!
//! [`ReconcilerTaskHandle::spawn()`] handles 1 and 2, [`ReconcilerTask::run()`]
//! handles 3 and 5, and service-specific implementations of [`Reconciler`] must
//! provide 4.

use crate::ScrimletReconcilersPrereqs;
use crate::ThisSledSwitchZoneUnderlayIpAddr;
use crate::status::ReconcilerActivationReason;
use crate::status::ReconcilerCurrentStatus;
use crate::status::ReconcilerInertReason;
use crate::status::ReconcilerRunningStatus;
use crate::status::ReconcilerStatus;
use crate::status::ReconciliationCompletedStatus;
use crate::status::ScrimletStatus;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use chrono::Utc;
use sled_agent_types::system_networking::SystemNetworkingConfig;
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

/// Trait that should be implemented by the service-specific reconciler tasks
/// elsewhere in this crate.
pub(crate) trait Reconciler: Send + 'static {
    type Status: Clone + Send + Sync + 'static;

    const LOGGER_COMPONENT_NAME: &'static str;
    const RE_RECONCILE_INTERVAL: Duration;

    /// Construct a new instance of this `Reconciler`.
    ///
    /// Typically builds a client for the relevant service based on
    /// `switch_zone_underlay_ip` and record `switch_slot` for use inside future
    /// calls to `do_reconciliation()`.
    fn new(
        switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
        switch_slot: ThisSledSwitchSlot,
        parent_log: &Logger,
    ) -> Self;

    /// Perform any required reconciliation based on the current contents of
    /// `system_networking_config`.
    ///
    /// This method is infallible; any errors must be described by
    /// `Self::Status`.
    fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
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
        switch_slot: Arc<SetOnce<ThisSledSwitchSlot>>,
        parent_log: &Logger,
    ) -> Self {
        Self::spawn_with_inner_constructor(
            scrimlet_status_rx,
            prereqs,
            switch_slot,
            parent_log,
            T::new,
        )
    }

    // Separate, private function that allows unit tests to customize how `T` is
    // constructed. Production passes `T::new` as `inner_constructor`; i.e.,
    // just call the constructor we know exists from the `Reconciler` trait.
    fn spawn_with_inner_constructor<F>(
        mut scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
        prereqs: Arc<SetOnce<ScrimletReconcilersPrereqs>>,
        switch_slot: Arc<SetOnce<ThisSledSwitchSlot>>,
        parent_log: &Logger,
        inner_constructor: F,
    ) -> Self
    where
        F: FnOnce(
                ThisSledSwitchZoneUnderlayIpAddr,
                ThisSledSwitchSlot,
                &Logger,
            ) -> T
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

        // Helper called when this reconciler exits unexpectedly (i.e., due to
        // an input watch channel being closed). This can happen either while
        // we're waiting for our prereqs or any time later while we're running.
        let log_task_exiting =
            |status_tx: &watch::Sender<ReconcilerStatus<T::Status>>,
             log: &Logger| {
                status_tx.send_modify(|status| {
                    status.current_status = ReconcilerCurrentStatus::Inert(
                        ReconcilerInertReason::TaskExitedUnexpectedly,
                    );
                });
                error!(
                    log,
                    "exited due to watch channel closure \
                     (unexpected except during shutdown in tests)"
                );
            };

        let task = tokio::spawn(async move {
            // Wait for all the information we need to construct a `T` (the
            // actual reconciler).
            let (prereqs, switch_slot) = match wait_for_all_prereqs::<T>(
                &mut scrimlet_status_rx,
                prereqs,
                switch_slot,
                &status_tx,
                &log,
            )
            .await
            {
                Ok((prereqs, switch_slot)) => (prereqs, switch_slot),
                Err(_recv_error) => {
                    return log_task_exiting(&status_tx, &log);
                }
            };

            // Unpack the prereqs and create our inner reconciler.
            let ScrimletReconcilersPrereqs {
                system_networking_config_rx,
                switch_zone_underlay_ip,
            } = prereqs;
            let inner = inner_constructor(
                switch_zone_underlay_ip,
                switch_slot,
                &parent_log,
            );

            // Start reconciling.
            let mut inner_task = ReconcilerTask {
                scrimlet_status_rx,
                system_networking_config_rx,
                status_tx,
                inner,
                log,
            };
            match inner_task.run().await {
                // `inner_task.run()` runs forever...
                Ok(never_returns) => match never_returns {},

                // ... unless one of its input watch channels has closed.
                Err(_recv_error) => {
                    return log_task_exiting(
                        &inner_task.status_tx,
                        &inner_task.log,
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

/// Wait for all prereqs: information that comes from sled-agent and information
/// we fetch from MGS.
///
/// These inputs come in the shape of two independent `SetOnce<_>` values, but
/// we know they can only be populated in order: `prereqs` must come first; once
/// sled-agent passes those into our parent `ScrimletReconcilers`, it spawns a
/// task that will subsequently populate `switch_slot`.
///
/// This function will block forever on non-scrimlet sleds, because
/// `switch_slot` will never be populated.
///
/// # Errors
///
/// This method will return an error if one of its input channels is closed:
/// either `scrimlet_status_rx`, or once `prereqs` have been received, if the
/// `system_networking_config_rx` channel inside them is closed. Channel
/// closures are only expected in tests; in sled-agent it keeps the sending half
/// of these channels inside its long-running tasks.
async fn wait_for_all_prereqs<T: Reconciler>(
    scrimlet_status_rx: &mut watch::Receiver<ScrimletStatus>,
    prereqs: Arc<SetOnce<ScrimletReconcilersPrereqs>>,
    switch_slot: Arc<SetOnce<ThisSledSwitchSlot>>,
    status_tx: &watch::Sender<ReconcilerStatus<T::Status>>,
    log: &Logger,
) -> Result<(ScrimletReconcilersPrereqs, ThisSledSwitchSlot), RecvError> {
    // Wait for sled-agent to give us our prereqs. We also have to check for the
    // `scrimlet_status_rx` channel being closed, which is our signal to bail.
    info!(
        log,
        "task started; waiting for SystemNetworkingConfig and underlay IP"
    );
    let mut prereqs = loop {
        // Both arms are cancel-safe and we do not `.await` within the
        // body of any arm, avoiding any opportunity for futurelock.
        tokio::select! {
            prereqs = prereqs.wait() => {
                break prereqs.clone();
            }
            result = scrimlet_status_rx.changed() => {
                // We can't do anything about the scrimlet status changing yet,
                // because we're still waiting on prereqs. Keep waiting.
                () = result?;
                continue;
            }
        }
    };

    // Now wait for `ThisSledSwitchSlot` to contact MGS and determine our slot.
    // This will block forever on non-scrimlets.
    //
    // We also have to check for either input channel (`scrimlet_status_rx` or
    // `prereqs.system_networking_config_rx`) being closed, upon which we bail.
    info!(
        log,
        "received SystemNetworkingConfig and underlay IP; \
         now waiting to determine our switch slot \
         (will block forever if we are not a scrimlet)",
    );
    status_tx.send_modify(|status| {
        status.current_status = ReconcilerCurrentStatus::Inert(
            ReconcilerInertReason::WaitingToDetermineSwitchSlot,
        );
    });
    let switch_slot = loop {
        // All arms are cancel-safe and we do not `.await` within the
        // body of any arm, avoiding any opportunity for futurelock.
        tokio::select! {
            switch_slot = switch_slot.wait() => {
                break *switch_slot;
            }

            // We can't do anything about either of these changing yet, because
            // we're still waiting to find our slot. Keep waiting.
            result = scrimlet_status_rx.changed() => {
                () = result?;
                continue;
            }
            result = prereqs.system_networking_config_rx.changed() => {
                () = result?;
                continue;
            }
        }
    };

    Ok((prereqs, switch_slot))
}

struct ReconcilerTask<T: Reconciler> {
    scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
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
            // We know we _were_ a scrimlet at some point, because we determined
            // our switch slot by contacting MGS within our own switch zone. But
            // it's possible we could become "not a scrimlet" in the future
            // (e.g., if the switch disappears out from under us). In such a
            // case, block until it comes back.
            self.wait_if_this_sled_is_no_longer_a_scrimlet().await?;

            // We _are_ a scrimlet; perform reconciliation.
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
            self.status_tx.send_modify(|status| {
                status.current_status =
                    ReconcilerCurrentStatus::Running(running_status);
            });

            // Actually perform reconciliation.
            let status_result = self
                .inner
                .do_reconciliation(&system_networking_config, &self.log)
                .await;

            // Update our output watch channel with the result.
            info!(
                self.log, "reconciliation attempt complete";
                "activation_reason" => ?activation_reason,
                "activation_count" => activation_count,
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

    async fn wait_if_this_sled_is_no_longer_a_scrimlet(
        &mut self,
    ) -> Result<(), RecvError> {
        let mut logged_not_scrimlet = false;

        loop {
            let status = *self.scrimlet_status_rx.borrow_and_update();
            match status {
                ScrimletStatus::Scrimlet => {
                    return Ok(());
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
                            ReconcilerInertReason::NoLongerAScrimlet,
                        );
                    });

                    // Select over both input channels so we can detect channel
                    // closure and exit cleanly if either channel goes away. If
                    // the rack network config changes here, we'll spuriously
                    // loop around and reread the scrimlet status, but that's no
                    // big deal.
                    //
                    // Both arms are cancel-safe and we do not `.await` within
                    // the body of any arm, avoiding any opportunity for
                    // futurelock.
                    tokio::select! {
                        result = self.scrimlet_status_rx.changed() => {
                            () = result?;
                        }
                        result = self.system_networking_config_rx.changed() => {
                            () = result?;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
