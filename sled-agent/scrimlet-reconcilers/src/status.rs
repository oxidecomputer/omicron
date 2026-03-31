// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the reconcilers in this crate.

use crate::DpdReconcilerStatus;
use crate::MgdReconcilerStatus;
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
    WaitingForPrereqs,
    WaitingToDetermineSwitchSlot,
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
    /// The reconciler has run but is currently idle, waiting for reactivation.
    Idle,
}

#[derive(Debug, Clone)]
pub struct ReconcilerStatus<T> {
    pub current_status: ReconcilerCurrentStatus,
    pub last_completion: Option<ReconciliationCompletedStatus<T>>,
}

#[derive(Debug, Clone)]
pub struct ScrimletReconcilersStatus {
    pub dpd_reconciler: ReconcilerStatus<DpdReconcilerStatus>,
    pub mgd_reconciler: ReconcilerStatus<MgdReconcilerStatus>,
}
