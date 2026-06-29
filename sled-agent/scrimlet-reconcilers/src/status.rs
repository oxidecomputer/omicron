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

/// Whether or not this sled is a scrimlet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrimletStatus {
    Scrimlet,
    NotScrimlet,
}

/// Status of attempting to determine this sled's switch slot via MGS within
/// this sled's switch zone.
#[derive(Debug, Clone)]
pub enum DetermineSwitchSlotStatus {
    /// We're not attempting to contact MGS because we're not a scrimlet.
    NotScrimlet,

    /// We're currently attempting to contact MGS.
    ///
    /// If this is not the first attempt, `prev_attempt_err` contains the error
    /// we encountered the last time. (If the last time succeeded, we'd be
    /// done!)
    ContactingMgs { prev_attempt_err: Option<String> },

    /// We're currently idle waiting for a timeout to retry due to a previous
    /// failure.
    WaitingToRetry { prev_attempt_err: String },
}

/// Why a reconciler task has gone inert.
#[derive(Debug, Clone, Copy)]
pub enum ReconcilerInertReason {
    /// The reconciler task started when this sled was a scrimlet, but it has
    /// since become "not a scrimlet" (e.g., because the attached switch has
    /// gone away).
    NoLongerAScrimlet,

    /// The reconciler task exited. This is not expected except in tests; the
    /// task runs forever as long as sled-agent holds on to the channels used to
    /// communicate with it.
    TaskExitedUnexpectedly,
}

/// Why a reconciler task was activated.
#[derive(Debug, Clone, Copy)]
pub enum ReconcilerActivationReason {
    /// Each reconciler runs once on startup.
    Startup,
    /// The task was activated due to its periodic timer firing.
    PeriodicTimer,
    /// The task was activated in response to a change in the networking config.
    SystemNetworkingConfigChanged,
    /// The task was activated in response to the sled becoming a scrimlet again
    /// (after previously transitioning to "not a scrimlet").
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
    /// Status of the task at this moment.
    pub current_status: ReconcilerCurrentStatus,
    /// Final status of the most recent activation of this task.
    // Box the inner status to avoid clippy complaining about
    // `ScrimletReconcilersStatus::Running { ... }` being overly large.
    pub last_completion: Option<Box<ReconciliationCompletedStatus<T>>>,
}

#[derive(Debug, Clone)]
pub enum ScrimletReconcilersStatus {
    /// `sled-agent` has not yet provided underlay networking information.
    WaitingForSledAgentNetworkingInfo,

    /// We're attempting to determine our switch slot.
    DeterminingSwitchSlot(DetermineSwitchSlotStatus),

    /// We are a scrimlet and the individual reconcilers are running.
    Running {
        dpd_reconciler: ReconcilerStatus<DpdReconcilerStatus>,
        mgd_reconciler: ReconcilerStatus<MgdReconcilerStatus>,
        uplinkd_reconciler: ReconcilerStatus<UplinkdReconcilerStatus>,
    },
}
