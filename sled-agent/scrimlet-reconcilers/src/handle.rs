// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`ScrimletReconcilers`] is the entry point into this crate for `sled-agent`;
//! it provides a suitable handle for `sled-agent`'s "long running tasks", and
//! contains a handle to each of the inner service-specific reconcilers.

use crate::ThisSledSwitchZoneUnderlayIpAddr;
use crate::dpd_reconciler::DpdReconciler;
use crate::mgd_reconciler::MgdReconciler;
use crate::reconciler_task::ReconcilerTaskHandle;
use crate::status::ScrimletReconcilersStatus;
use crate::status::ScrimletStatus;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use crate::uplinkd_reconciler::UplinkdReconciler;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Information required to enable all the scrimlet reconciler tasks provided
/// by this crate.
///
#[derive(Debug, Clone)]
pub struct SledAgentNetworkingInfo {
    pub system_networking_config_rx: watch::Receiver<SystemNetworkingConfig>,
    pub switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
}

// Possible states of a `ScrimletReconcilers`.
//
// Every state is only visited once, and they progress (mostly) in order:
//
// 1. `WaitingForSledAgentNetworkingInfo` is the initial state.
// 2. When `set_sled_agent_networking_info_once()` is called, we transition to
//    the `WaitingForThisSledSwitchSlot` state. If we are not a scrimlet, we
//    stay in this state forever.
// 3. If we are a scrimlet, we transition either to `Running` once we've
//    contacted MGS within our switch zone to identify which switch slot we're
//    in, or to `Shutdown` if the watch channel from sled-agent is closed. (This
//    never happens in production, but can happen in tests.)
enum ScrimletReconcilersState {
    WaitingForSledAgentNetworkingInfo,
    WaitingForThisSledSwitchSlot {
        // We never await this task; it's spawned by an instance of
        // `ScrimletReconcilers` and is itself responsible for transitioning
        // that instance's `state` to either `Running` or `Shutdown`.
        _determine_switch_slot_task: JoinHandle<()>,
    },
    Running {
        dpd_reconciler: ReconcilerTaskHandle<DpdReconciler>,
        mgd_reconciler: ReconcilerTaskHandle<MgdReconciler>,
        uplinkd_reconciler: ReconcilerTaskHandle<UplinkdReconciler>,
    },
    Shutdown,
}

/// Handle to tasks that reconcile network configuration with services within a
/// scrimlet's local switch zone.
///
/// [`ScrimletReconcilers`] has a two-phase initialization process to support
/// being included in `sled-agent`'s set of "long running tasks".
/// [`ScrimletReconcilers::new()`] can be constructed at any time (in
/// particular: very soon after `sled-agent` starts), and it defers actually
/// running any of the reconciliation tasks until `sled-agent` has progressed to
/// the point where it can provide these prerequisites: we need a channel on
/// which we can receive the current [`SystemNetworkingConfig`], and we need to
/// know our switch zone's underlay IP address.
///
/// Even after [`ScrimletReconcilers::set_sled_agent_networking_info_once()`] is
/// called to provide the basic prerequisites that all sled-agents have, the
/// scrimlet reconciler tasks will not be spawned until:
///
/// 1. [`ScrimletReconcilers::set_scrimlet_status()`] is called with
///    [`ScrimletStatus::Scrimlet`]. (Non-scrimlet sled-agents will never make
///    this call, and therefore will never spawn the reconciliation tasks.)
/// 2. We successfully contact MGS within our switch zone to determine which
///    switch slot we are.
///
/// Once both of these are satisfied, all reconciliation tasks are spawned and
/// begin running. They will reactivate periodically and in response to any
/// changes to the [`SystemNetworkingConfig`] from sled-agent, and will go inert
/// if [`ScrimletReconcilers::set_scrimlet_status()`] is called with
/// [`ScrimletStatus::NotScrimlet`] (remaining inert until we are told we are a
/// scrimlet again).
pub struct ScrimletReconcilers {
    scrimlet_status_tx: watch::Sender<ScrimletStatus>,
    state: Arc<Mutex<ScrimletReconcilersState>>,
    parent_log: Logger,
}

impl ScrimletReconcilers {
    pub fn new(parent_log: &Logger) -> Self {
        // We discard the receiver here, and create new subscribers if and when
        // we spawn tasks that need to consume it.
        let (scrimlet_status_tx, _scrimlet_status_rx) =
            watch::channel(ScrimletStatus::NotScrimlet);
        let state = Arc::new(Mutex::new(
            ScrimletReconcilersState::WaitingForSledAgentNetworkingInfo,
        ));

        Self { scrimlet_status_tx, state, parent_log: parent_log.clone() }
    }

    pub fn status(&self) -> ScrimletReconcilersStatus {
        match &*self.state.lock().unwrap() {
            ScrimletReconcilersState::WaitingForSledAgentNetworkingInfo => {
                ScrimletReconcilersStatus::WaitingForSledAgentNetworkingInfo
            }
            ScrimletReconcilersState::WaitingForThisSledSwitchSlot {
                ..
            } => {
                match *self.scrimlet_status_tx.borrow() {
                    ScrimletStatus::Scrimlet => {
                        // If we are a scrimlet but are still in the
                        // `WaitingForThisSledSwitchSlot` state, we haven't yet
                        // contacted MGS.
                        ScrimletReconcilersStatus::WaitingForSwitchSlotFromMgs
                    }
                    ScrimletStatus::NotScrimlet => {
                        ScrimletReconcilersStatus::NotScrimlet
                    }
                }
            }
            ScrimletReconcilersState::Running {
                dpd_reconciler,
                mgd_reconciler,
                uplinkd_reconciler,
            } => ScrimletReconcilersStatus::Running {
                dpd_reconciler: dpd_reconciler.status(),
                mgd_reconciler: mgd_reconciler.status(),
                uplinkd_reconciler: uplinkd_reconciler.status(),
            },
            ScrimletReconcilersState::Shutdown => {
                ScrimletReconcilersStatus::Shutdown
            }
        }
    }

    /// Set whether this sled is a scrimlet or not.
    ///
    /// This doesn't change _much_ at runtime, but it can: we may start out "not
    /// a scrimlet" and then discover an attached switch later after boot, at
    /// which point we become a scrimlet, or we may become "not a scrimlet" if
    /// the switch is detached at runtime.
    pub fn set_scrimlet_status(&self, status: ScrimletStatus) {
        self.scrimlet_status_tx.send_if_modified(|prev| {
            if *prev == status {
                false
            } else {
                *prev = status;
                true
            }
        });
    }

    /// Provide the networking information necessary to start the scrimlet
    /// reconciler tasks.
    ///
    /// # Panics
    ///
    /// This method panics if called more than once. We set up the reconciler
    /// tasks with the initial info provided, and should never receive a
    /// second call. (One of the bits inside `info` is a watch channel:
    /// receiving a second channel is a sign of control flow gone very wrong, as
    /// all the tasks will already be operating based on the first channel
    /// received.)
    pub fn set_sled_agent_networking_info_once(
        &self,
        info: SledAgentNetworkingInfo,
    ) {
        // Lock the state; we only hold this long enough to spawn a task and
        // then set the state to `WaitingForThisSledSwitchSlot`.
        let mut state = self.state.lock().unwrap();

        // We're constructed in the `WaitingForSledAgentNetworkingInfo`; if
        // we're still in that state, transition to
        // `WaitingForThisSledSwitchSlot`. If we're in any other state, that
        // means this function has been called multiple times, which is a
        // programmer error: panic.
        match &*state {
            ScrimletReconcilersState::WaitingForSledAgentNetworkingInfo => {
                // Spawn the task that waits until we become a scrimlet,
                // contacts MGS to determine our switch slot, then spawns all
                // the actual reconciler tasks. When it does so, it will update
                // `self.state` and transition us to the `Running` state.
                let determine_switch_slot_task =
                    tokio::spawn(determine_switch_slot(
                        Arc::clone(&self.state),
                        self.scrimlet_status_tx.subscribe(),
                        info,
                        self.parent_log.clone(),
                    ));

                *state =
                    ScrimletReconcilersState::WaitingForThisSledSwitchSlot {
                        _determine_switch_slot_task: determine_switch_slot_task,
                    };
            }

            ScrimletReconcilersState::WaitingForThisSledSwitchSlot {
                ..
            }
            | ScrimletReconcilersState::Running { .. }
            | ScrimletReconcilersState::Shutdown => {
                panic!(
                    "set_sled_agent_networking_info_once() called more than \
                     once - scrimlet reconcilers are already set up and \
                     running based on the initial information provided!"
                );
            }
        }
    }
}

async fn determine_switch_slot(
    state: Arc<Mutex<ScrimletReconcilersState>>,
    mut scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
    networking_info: SledAgentNetworkingInfo,
    parent_log: Logger,
) {
    let this_sled_switch_slot =
        match ThisSledSwitchSlot::determine_retrying_forever(
            &mut scrimlet_status_rx,
            networking_info.switch_zone_underlay_ip,
            &parent_log,
        )
        .await
        {
            Ok(slot) => slot,
            Err(_recv_error) => {
                *state.lock().unwrap() = ScrimletReconcilersState::Shutdown;
                return;
            }
        };

    // We now know that we're a scrimlet and have successfully contacted MGS
    // within our switch zone. Spawn all the reconciler tasks...
    let dpd_reconciler = ReconcilerTaskHandle::<DpdReconciler>::spawn(
        scrimlet_status_rx.clone(),
        networking_info.system_networking_config_rx.clone(),
        networking_info.switch_zone_underlay_ip,
        this_sled_switch_slot,
        &parent_log,
    );
    let mgd_reconciler = ReconcilerTaskHandle::<MgdReconciler>::spawn(
        scrimlet_status_rx.clone(),
        networking_info.system_networking_config_rx.clone(),
        networking_info.switch_zone_underlay_ip,
        this_sled_switch_slot,
        &parent_log,
    );
    let uplinkd_reconciler = ReconcilerTaskHandle::<UplinkdReconciler>::spawn(
        scrimlet_status_rx,
        networking_info.system_networking_config_rx,
        networking_info.switch_zone_underlay_ip,
        this_sled_switch_slot,
        &parent_log,
    );

    // ... and transition the `ScrimletReconcilers` that spawned us into the
    // `Running` state.
    *state.lock().unwrap() = ScrimletReconcilersState::Running {
        dpd_reconciler,
        mgd_reconciler,
        uplinkd_reconciler,
    };
}
