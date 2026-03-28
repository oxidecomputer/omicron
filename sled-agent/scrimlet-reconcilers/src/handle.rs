// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`ScrimletReconcilers`] is the entry point into this crate for `sled-agent`;
//! it provides a suitable handle for `sled-agent`'s "long running tasks", and
//! contains a handle to each of the inner service-specific reconcilers.

use crate::ThisSledSwitchZoneUnderlayIpAddr;
use crate::dpd_reconciler::DpdReconciler;
use crate::reconciler_task::ReconcilerTaskHandle;
use crate::status::ScrimletReconcilersStatus;
use crate::status::ScrimletStatus;
use sled_agent_types::early_networking::RackNetworkConfig;
use slog::Logger;
use std::sync::Arc;
use tokio::sync::SetOnce;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct ScrimletReconcilersPrereqs {
    pub rack_network_config_rx: watch::Receiver<RackNetworkConfig>,
    pub switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
}

pub struct ScrimletReconcilers {
    scrimlet_status_tx: watch::Sender<ScrimletStatus>,
    prereqs: Arc<SetOnce<ScrimletReconcilersPrereqs>>,
    dpd_reconciler: ReconcilerTaskHandle<DpdReconciler>,
}

impl ScrimletReconcilers {
    pub fn new(parent_log: &Logger) -> Self {
        let (scrimlet_status_tx, scrimlet_status_rx) =
            watch::channel(ScrimletStatus::NotScrimlet);
        let prereqs = Arc::new(SetOnce::new());

        let dpd_reconciler = ReconcilerTaskHandle::<DpdReconciler>::spawn(
            scrimlet_status_rx.clone(),
            Arc::clone(&prereqs),
            parent_log,
        );

        Self { scrimlet_status_tx, prereqs, dpd_reconciler }
    }

    pub fn status(&self) -> ScrimletReconcilersStatus {
        ScrimletReconcilersStatus {
            dpd_reconciler: self.dpd_reconciler.status(),
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

    /// Provide the prerequisites necessary to start the scrimlet reconciler
    /// tasks.
    ///
    /// # Panics
    ///
    /// This method panics if called more than once. We set up the reconciler
    /// tasks with the initial prereqs provided, and should never receive a
    /// second set. (One of the prereqs is a watch channel: receiving a second
    /// channel is a sign of control flow gone very wrong, as all the tasks will
    /// already be operating based on the first channel received.)
    pub fn set_prereqs_once(&self, prereqs: ScrimletReconcilersPrereqs) {
        if self.prereqs.set(prereqs).is_err() {
            panic!(
                "set_prereqs_once() called more than once - scrimlet \
                 reconcilers are already set up and running based on the \
                 initial set of prereqs!"
            );
        }
    }
}
