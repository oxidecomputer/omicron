// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`ScrimletReconcilers`] is the entry point into this crate for `sled-agent`;
//! it provides a suitable handle for `sled-agent`'s "long running tasks", and
//! contains a handle to each of the inner service-specific reconcilers.

use crate::DetermineSwitchSlotStatus;
use crate::ThisSledSwitchZoneUnderlayIpAddr;
use crate::dpd_reconciler::DpdReconciler;
use crate::mgd_reconciler::MgdReconciler;
use crate::reconciler_task::ReconcilerTaskHandle;
use crate::status::ScrimletReconcilersStatus;
use crate::status::ScrimletStatus;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use crate::uplinkd_reconciler::UplinkdReconciler;
use omicron_common::address::MGS_PORT;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog::info;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::watch;

/// Information required to enable all the scrimlet reconciler tasks provided
/// by this crate.
///
#[derive(Debug, Clone)]
pub struct SledAgentNetworkingInfo {
    pub system_networking_config_rx: watch::Receiver<SystemNetworkingConfig>,
    pub switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
}

/// Handle to tasks that reconcile network configuration with services within a
/// scrimlet's local switch zone.
///
/// [`ScrimletReconcilers`] has a two- or three-phase initialization process
/// (depending on whether the sled is a non-scrimlet or a scrimlet) to support
/// being included in `sled-agent`'s set of "long running tasks".
/// [`ScrimletReconcilers::new()`] can be constructed at any time (in
/// particular: very soon after `sled-agent` starts).
///
/// After a [`ScrimletReconcilers`] has been created, `sled-agent` is
/// responsible for calling
/// [`ScrimletReconcilers::set_sled_agent_networking_info_once()`] exactly one
/// time: this provides the reconcilers with a watch channel to receive the
/// current [`SystemNetworkingConfig`] and the IP address of this sled's switch
/// zone (should it have one).
///
/// After `sled-agent` has provided those prerequisites, on all sleds,
/// [`ScrimletReconcilers`] spawns a tokio task that waits until both of these
/// have occurred:
///
/// 1. [`ScrimletReconcilers::set_scrimlet_status()`] is called with
///    [`ScrimletStatus::Scrimlet`].
/// 2. We successfully contact MGS within our switch zone to determine which
///    switch slot we are.
///
/// On non-scrimlet sleds, step 1 never happens, so the reconciler tasks are
/// never spawned.
///
/// Once both of these are satisfied on scrimlets, all reconciliation tasks are
/// spawned and begin running. They will reactivate periodically and in response
/// to any changes to the [`SystemNetworkingConfig`] from sled-agent, and will
/// go inert if [`ScrimletReconcilers::set_scrimlet_status()`] is called with
/// [`ScrimletStatus::NotScrimlet`] (remaining inert until we are told we are a
/// scrimlet again).
pub struct ScrimletReconcilers {
    // Sending half of the channel used to communicate to all the reconcilers
    // whether we're still a scrimlet.
    scrimlet_status_tx: watch::Sender<ScrimletStatus>,

    // These once locks hold the second and third phases of initialization
    // described in the doc comment above.
    determining_switch_slot:
        OnceLock<watch::Receiver<DetermineSwitchSlotStatus>>,
    running_reconcilers: Arc<OnceLock<RunningReconcilers>>,

    parent_log: Logger,

    // Hook for unit tests to modify how we construct an MGS client when
    // determining our switch slot.
    #[cfg(test)]
    override_make_mgs_client: Option<Box<dyn Fn() -> gateway_client::Client>>,
}

impl ScrimletReconcilers {
    pub fn new(parent_log: &Logger) -> Self {
        // We discard the receiver here, and create new subscribers if and when
        // we spawn tasks that need to consume it.
        let (scrimlet_status_tx, _scrimlet_status_rx) =
            watch::channel(ScrimletStatus::NotScrimlet);

        Self {
            scrimlet_status_tx,
            determining_switch_slot: OnceLock::new(),
            running_reconcilers: Arc::new(OnceLock::new()),
            parent_log: parent_log.clone(),
            #[cfg(test)]
            override_make_mgs_client: None,
        }
    }

    pub fn status(&self) -> ScrimletReconcilersStatus {
        // Do we have running reconcilers? If so, report their status.
        if let Some(running) = self.running_reconcilers.get() {
            let RunningReconcilers {
                dpd_reconciler,
                mgd_reconciler,
                uplinkd_reconciler,
            } = running;
            ScrimletReconcilersStatus::Running {
                dpd_reconciler: dpd_reconciler.status(),
                mgd_reconciler: mgd_reconciler.status(),
                uplinkd_reconciler: uplinkd_reconciler.status(),
            }
        }
        // Otherwise, have we started determining our switch slot?
        else if let Some(status_rx) = self.determining_switch_slot.get() {
            ScrimletReconcilersStatus::DeterminingSwitchSlot(
                status_rx.borrow().clone(),
            )
        }
        // Otherwise, we're still waiting for the networking info.
        else {
            ScrimletReconcilersStatus::WaitingForSledAgentNetworkingInfo
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
    /// This method panics if called more than once; this is considered a
    /// programmer error.
    ///
    /// One of the bits inside `info` is a watch channel: receiving a second
    /// channel is a sign of control flow gone very wrong, as all the tasks will
    /// already be operating based on the first channel received.
    pub fn set_sled_agent_networking_info_once(
        &self,
        info: SledAgentNetworkingInfo,
    ) {
        let (determining_switch_slot_tx, determining_switch_slot_rx) =
            watch::channel(DetermineSwitchSlotStatus::NotScrimlet);

        // Ensure we're only called once.
        if self.determining_switch_slot.set(determining_switch_slot_rx).is_err()
        {
            panic!(
                "set_sled_agent_networking_info_once() called more than \
                 once - scrimlet reconcilers are already set up and \
                 running based on the initial information provided!"
            );
        }

        let mgs_client = self.make_mgs_client(info.switch_zone_underlay_ip);

        // We now know this is the one and only time we've been called; spawn a
        // task that waits until we're a scrimlet, then waits until we can
        // determine our switch slot, then spawns all the running reconcilers
        // (populating `self.running_reconcilers`).
        //
        // We don't hang on to the join handle from this task; it exits either
        // when it populates `self.running_reconcilers`, or when we're dropped
        // (because it will exit when `self.scrimlet_status_tx` is closed).
        tokio::spawn(determine_switch_slot(
            Arc::clone(&self.running_reconcilers),
            determining_switch_slot_tx,
            self.scrimlet_status_tx.subscribe(),
            mgs_client,
            info,
            self.parent_log.clone(),
        ));
    }

    #[cfg(test)]
    fn make_mgs_client(
        &self,
        switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
    ) -> gateway_client::Client {
        if let Some(make_client) = &self.override_make_mgs_client {
            make_client()
        } else {
            self.make_real_mgs_client(switch_zone_underlay_ip)
        }
    }

    #[cfg(not(test))]
    fn make_mgs_client(
        &self,
        switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
    ) -> gateway_client::Client {
        self.make_real_mgs_client(switch_zone_underlay_ip)
    }

    fn make_real_mgs_client(
        &self,
        switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
    ) -> gateway_client::Client {
        let baseurl = format!("http://[{switch_zone_underlay_ip}]:{MGS_PORT}");
        gateway_client::Client::new(
            &baseurl,
            self.parent_log
                .new(slog::o!("component" => "ThisSledSwitchSlotMgsClient")),
        )
    }
}

async fn determine_switch_slot(
    running_reconcilers: Arc<OnceLock<RunningReconcilers>>,
    determining_switch_slot_tx: watch::Sender<DetermineSwitchSlotStatus>,
    mut scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
    mgs_client: gateway_client::Client,
    networking_info: SledAgentNetworkingInfo,
    parent_log: Logger,
) {
    let log = parent_log
        .new(slog::o!("component" => "ThisSledSwitchSlotDetermination"));

    // Block until either we successfully contact MGS at our switch zone
    // underlay IP or the sending half of `scrimlet_status_rx` is closed (which
    // means our parent `ScrimletReconcilers` was dropped - this only happens in
    // tests).
    let this_sled_switch_slot =
        match ThisSledSwitchSlot::determine_retrying_forever(
            determining_switch_slot_tx,
            &mut scrimlet_status_rx,
            &mgs_client,
            &log,
        )
        .await
        {
            Ok(slot) => slot,
            Err(_recv_error) => {
                return;
            }
        };
    info!(
        log, "determined this sled's switch slot";
        "slot" => ?this_sled_switch_slot,
    );

    // We know `running_reconcilers` must be unset, because we're the only one
    // that sets it, and we're only spawned from
    // `set_sled_agent_networking_info_once()` (which itself guarantees it's
    // only called once).
    running_reconcilers
        .set(RunningReconcilers::spawn_all(
            scrimlet_status_rx,
            networking_info,
            this_sled_switch_slot,
            &parent_log,
        ))
        .expect("running reconcilers is only set once");
}

#[derive(Debug)]
struct RunningReconcilers {
    dpd_reconciler: ReconcilerTaskHandle<DpdReconciler>,
    mgd_reconciler: ReconcilerTaskHandle<MgdReconciler>,
    uplinkd_reconciler: ReconcilerTaskHandle<UplinkdReconciler>,
}

impl RunningReconcilers {
    fn spawn_all(
        scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
        networking_info: SledAgentNetworkingInfo,
        this_sled_switch_slot: ThisSledSwitchSlot,
        parent_log: &Logger,
    ) -> Self {
        let dpd_reconciler = ReconcilerTaskHandle::<DpdReconciler>::spawn(
            scrimlet_status_rx.clone(),
            networking_info.system_networking_config_rx.clone(),
            networking_info.switch_zone_underlay_ip,
            this_sled_switch_slot,
            parent_log,
        );
        let mgd_reconciler = ReconcilerTaskHandle::<MgdReconciler>::spawn(
            scrimlet_status_rx.clone(),
            networking_info.system_networking_config_rx.clone(),
            networking_info.switch_zone_underlay_ip,
            this_sled_switch_slot,
            parent_log,
        );
        let uplinkd_reconciler =
            ReconcilerTaskHandle::<UplinkdReconciler>::spawn(
                scrimlet_status_rx,
                networking_info.system_networking_config_rx,
                networking_info.switch_zone_underlay_ip,
                this_sled_switch_slot,
                parent_log,
            );
        Self { dpd_reconciler, mgd_reconciler, uplinkd_reconciler }
    }
}

#[cfg(test)]
mod tests;
