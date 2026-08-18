// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`ScrimletReconcilers`] is the entry point into this crate for `sled-agent`;
//! it provides a suitable handle for `sled-agent`'s "long running tasks", and
//! contains a handle to each of the inner service-specific reconcilers.

use crate::dpd_reconciler::DpdReconciler;
use crate::lldpd_reconciler::LldpdReconciler;
use crate::mgd_reconciler::MgdReconciler;
use crate::reconciler_task::ReconcilerTaskHandle;
use crate::status::DetermineSwitchSlotStatus;
use crate::status::ScrimletReconcilersStatus;
use crate::status::ScrimletStatus;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use crate::uplinkd_reconciler::UplinkdReconciler;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers as api_status;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::MGD_PORT;
use omicron_common::address::MGS_PORT;
use sled_agent_types::sled::ThisSledSwitchZoneUnderlayIpAddr;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog::info;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::watch;

/// Mode in which the scrimlet reconcilers should run.
///
/// This exists to support tests where we don't have a real switch zone like
/// we expect to have on real hardware. The production sled-agent will always
/// pass `SwitchZone(ip)`; in this mode, we'll run reconcilers that talk to
/// services at the provided IP with their well-known ports and that communicate
/// with SMF within the switch zone. In the `Test { .. }` mode, we'll point the
/// reconcilers at the specified addresses for some services, and won't run the
/// SMF-based reconcilers at all.
#[derive(Debug, Clone, Copy)]
pub enum ScrimletReconcilersMode {
    SwitchZone(ThisSledSwitchZoneUnderlayIpAddr),
    #[cfg(any(test, feature = "testing"))]
    Test {
        mgs_addr: SocketAddr,
        dpd_addr: SocketAddr,
        mgd_addr: SocketAddr,
    },
}

impl ScrimletReconcilersMode {
    // Build a `reqwest` client with different timeout settings depending on
    // whether we're expecting to contact a real service or a test one.
    fn reqwest_client(&self) -> reqwest::Client {
        match self {
            ScrimletReconcilersMode::SwitchZone(_) => {
                // Build a custom reqwest client, primarily to set a lower
                // `pool_idle_timeout`. dropshot's default connection timeout is
                // 30 seconds. We want to ensure we don't hit
                // <https://github.com/hyperium/hyper/issues/2136> in any
                // reconcilers that try to re-reconcile on a 30 second interval,
                // so we choose a much lower `pool_idle_timeout`: 10 seconds is
                // long enough to reuse a connection for all the requests made
                // during one reconciliation pass, but is short enough we should
                // discard it before the server wants to time us out.
                //
                // The 15 second connect and read timeout are consistent with
                // progenitor's normal defaults.
                reqwest::ClientBuilder::new()
                    .connect_timeout(Duration::from_secs(15))
                    .pool_idle_timeout(Duration::from_secs(10))
                    .build()
                    .expect("reqwest parameters are valid")
            }
            #[cfg(any(test, feature = "testing"))]
            ScrimletReconcilersMode::Test { .. } => {
                // Some of our tests use tokio's paused time. We want to
                // construct a reqwest client that does not specify any timeouts
                // at all, allowing it to wait forever; this plays nicely with
                // paused time. (Paused time + timeouts cause the timeouts to
                // elapse instantly, which doesn't mesh well with establishing
                // TCP connections.)
                reqwest::Client::new()
            }
        }
    }

    pub(crate) fn mgs_client(
        self,
        parent_log: &Logger,
    ) -> gateway_client::Client {
        let addr: SocketAddr = match self {
            ScrimletReconcilersMode::SwitchZone(ip) => {
                SocketAddrV6::new(ip.into(), MGS_PORT, 0, 0).into()
            }
            #[cfg(any(test, feature = "testing"))]
            ScrimletReconcilersMode::Test { mgs_addr, .. } => mgs_addr,
        };
        let baseurl = format!("http://{addr}");
        gateway_client::Client::new_with_client(
            &baseurl,
            self.reqwest_client(),
            parent_log
                .new(slog::o!("component" => "ThisSledSwitchSlotMgsClient")),
        )
    }

    pub(crate) fn dpd_client(self, parent_log: &Logger) -> dpd_client::Client {
        use omicron_common::OMICRON_DPD_TAG;

        let addr: SocketAddr = match self {
            ScrimletReconcilersMode::SwitchZone(ip) => {
                SocketAddrV6::new(ip.into(), DENDRITE_PORT, 0, 0).into()
            }
            #[cfg(any(test, feature = "testing"))]
            ScrimletReconcilersMode::Test { dpd_addr, .. } => dpd_addr,
        };
        let baseurl = format!("http://{addr}");
        dpd_client::Client::new_with_client(
            &baseurl,
            self.reqwest_client(),
            dpd_client::ClientState {
                tag: OMICRON_DPD_TAG.to_owned(),
                log: parent_log
                    .new(slog::o!("component" => "DpdReconcilerClient")),
            },
        )
    }

    pub(crate) fn mgd_client(
        self,
        parent_log: &Logger,
    ) -> mg_admin_client::Client {
        let addr: SocketAddr = match self {
            ScrimletReconcilersMode::SwitchZone(ip) => {
                SocketAddrV6::new(ip.into(), MGD_PORT, 0, 0).into()
            }
            #[cfg(any(test, feature = "testing"))]
            ScrimletReconcilersMode::Test { mgd_addr, .. } => mgd_addr,
        };
        let baseurl = format!("http://{addr}");
        mg_admin_client::Client::new_with_client(
            &baseurl,
            self.reqwest_client(),
            parent_log.new(slog::o!("component" => "MgdReconcilerClient")),
        )
    }
}

/// Information required to enable all the scrimlet reconciler tasks provided
/// by this crate.
#[derive(Debug, Clone)]
pub struct SledAgentNetworkingInfo {
    pub system_networking_config_rx: watch::Receiver<SystemNetworkingConfig>,
    pub mode: ScrimletReconcilersMode,
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
        }
    }

    pub fn status(&self) -> api_status::ScrimletReconcilersStatus {
        // Do we have running reconcilers? If so, report their status.
        let status = if let Some(running) = self.running_reconcilers.get() {
            let RunningReconcilers {
                dpd_reconciler,
                lldpd_reconciler,
                mgd_reconciler,
                uplinkd_reconciler,
            } = running;
            ScrimletReconcilersStatus::Running {
                dpd_reconciler: dpd_reconciler.status(),
                lldpd_reconciler: lldpd_reconciler.status(),
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
        };
        status.into()
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
            info.mode.mgs_client(&self.parent_log),
            info,
            self.parent_log.clone(),
        ));
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
    lldpd_reconciler: ReconcilerTaskHandle<LldpdReconciler>,
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
            networking_info.mode,
            this_sled_switch_slot,
            parent_log,
        );
        let lldpd_reconciler = ReconcilerTaskHandle::<LldpdReconciler>::spawn(
            scrimlet_status_rx.clone(),
            networking_info.system_networking_config_rx.clone(),
            networking_info.mode,
            this_sled_switch_slot,
            parent_log,
        );
        let mgd_reconciler = ReconcilerTaskHandle::<MgdReconciler>::spawn(
            scrimlet_status_rx.clone(),
            networking_info.system_networking_config_rx.clone(),
            networking_info.mode,
            this_sled_switch_slot,
            parent_log,
        );
        let uplinkd_reconciler =
            ReconcilerTaskHandle::<UplinkdReconciler>::spawn(
                scrimlet_status_rx,
                networking_info.system_networking_config_rx,
                networking_info.mode,
                this_sled_switch_slot,
                parent_log,
            );
        Self {
            dpd_reconciler,
            lldpd_reconciler,
            mgd_reconciler,
            uplinkd_reconciler,
        }
    }
}

#[cfg(test)]
mod tests;
