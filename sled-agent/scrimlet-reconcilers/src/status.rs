// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the reconcilers in this crate.

use crate::DpdReconcilerStatus;
use crate::MgdReconcilerStatus;
use crate::UplinkdReconcilerStatus;
use chrono::DateTime;
use chrono::Utc;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrimletStatus {
    Scrimlet,
    NotScrimlet,
}

#[derive(Debug, Clone, Copy)]
pub enum ReconcilerInertReason {
    NoLongerAScrimlet,
    TaskExitedUnexpectedly,
}

#[derive(Debug, Clone, Copy)]
pub enum ReconcilerActivationReason {
    Startup,
    PeriodicTimer,
    SystemNetworkingConfigChanged,
    ScrimletStatusChanged,
}

#[derive(Debug, Clone)]
pub struct ReconciliationCompletedStatus<T> {
    pub activation_reason: ReconcilerActivationReason,
    pub completed_at_time: DateTime<Utc>,
    pub ran_for: Duration,
    pub activation_count: u64,
    pub status: T,
}

#[derive(Debug, Clone, Copy)]
pub struct ReconcilerRunningStatus {
    activation_reason: ReconcilerActivationReason,
    started_at_time: DateTime<Utc>,
    started_at_instant: Instant,
}

impl ReconcilerRunningStatus {
    pub(crate) fn new(activation_reason: ReconcilerActivationReason) -> Self {
        Self {
            activation_reason,
            started_at_time: Utc::now(),
            started_at_instant: Instant::now(),
        }
    }

    pub fn activation_reason(&self) -> ReconcilerActivationReason {
        self.activation_reason
    }

    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at_time
    }

    pub fn elapsed_since_start(&self) -> Duration {
        self.started_at_instant.elapsed()
    }
}

#[derive(Debug, Clone)]
pub enum ReconcilerCurrentStatus {
    /// The reconciler is inert: it will not or cannot run for some reason.
    Inert(ReconcilerInertReason),
    /// The reconciler is currently running.
    Running(ReconcilerRunningStatus),
    /// The reconciler is not currently running.
    Idle,
}

#[derive(Debug, Clone)]
pub struct ReconcilerStatus<T> {
    pub current_status: ReconcilerCurrentStatus,
    // Box the inner status to avoid clippy complaining about
    // `ScrimletReconcilersStatus::Running { ... }` being overly large.
    pub last_completion: Option<Box<ReconciliationCompletedStatus<T>>>,
}

#[derive(Debug, Clone)]
pub enum ScrimletReconcilersStatus {
    /// `sled-agent` has not yet provided underlay networking information.
    WaitingForSledAgentNetworkingInfo,

    /// This sled is not a scrimlet.
    ///
    /// A "not scrimlet" sled could become a scrimlet at runtime if a switch is
    /// later discovered.
    NotScrimlet,

    /// We are a scrimlet, but we haven't yet contacted MGS within our switch
    /// zone to identify which switch slot we're in.
    WaitingForSwitchSlotFromMgs,

    /// We are a scrimlet and the individual reconcilers are running.
    Running {
        dpd_reconciler: ReconcilerStatus<DpdReconcilerStatus>,
        mgd_reconciler: ReconcilerStatus<MgdReconcilerStatus>,
        uplinkd_reconciler: ReconcilerStatus<UplinkdReconcilerStatus>,
    },

    /// All reconcilers have been shut down.
    ///
    /// This state should only exist in tests and is terminal.
    Shutdown,
}
