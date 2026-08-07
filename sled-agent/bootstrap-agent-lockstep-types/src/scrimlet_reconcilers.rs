// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconcilers responsible for
//! syncing network configuration from the bootstore to the services in the
//! switch zone.

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub mod dpd;
pub mod lldpd;
pub mod mgd;
pub mod uplinkd;

/// Whether or not this sled is a scrimlet.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ScrimletStatus {
    Scrimlet,
    NotScrimlet,
}

/// Status of attempting to determine this sled's switch slot via MGS within
/// this sled's switch zone.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status")]
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
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

/// Status of a completed-in-the-past reconciliation attempt.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "ReconciliationCompletedStatus{T}")]
pub struct ReconciliationCompletedStatus<T> {
    /// Why the reconciliation attempt fired.
    pub activation_reason: ReconcilerActivationReason,
    /// When the attempt completed.
    pub completed_at_time: DateTime<Utc>,
    /// How long the attempt ran.
    pub ran_for: Duration,
    /// 0-based counter of the number of times this reconciler has activated
    /// since the last time sled-agent started.
    pub activation_count: u64,
    /// Reconciler-specific status.
    pub status: T,
}

/// Status of a currently-running reconciliation attempt.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
pub struct ReconcilerRunningStatus {
    /// Why the reconciliation attempt fired.
    pub activation_reason: ReconcilerActivationReason,
    /// When the attempt started.
    pub started_at_time: DateTime<Utc>,
    /// How long the attempt has been running.
    pub running_for: Duration,
}

/// Current-point-in-time status of a single scrimlet reconciler.
///
/// `Inert` and `Idle` both indicate "not running", but `Inert` means "not
/// running and will continue to not run for a given reason", whereas `Idle`
/// means "not currently running but will run again soon".
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum ReconcilerCurrentStatus {
    /// The reconciler is inert: it will not or cannot run for some reason.
    Inert(ReconcilerInertReason),
    /// The reconciler is currently running.
    Running(ReconcilerRunningStatus),
    /// The reconciler is not currently running.
    Idle,
}

<<<<<<< HEAD
impl ReconcilerCurrentStatus {
    pub fn display(&self) -> ReconcilerCurrentStatusDisplay<'_> {
        ReconcilerCurrentStatusDisplay(self)
    }
}

pub struct ReconcilerCurrentStatusDisplay<'a>(&'a ReconcilerCurrentStatus);

impl fmt::Display for ReconcilerCurrentStatusDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ReconcilerCurrentStatus::Inert(reason) => match reason {
                ReconcilerInertReason::NoLongerAScrimlet => {
                    write!(f, "inert: switch no longer present")
                }
                ReconcilerInertReason::TaskExitedUnexpectedly => write!(
                    f,
                    "inert: task exited unexpectedly \
                     (this should be impossible!)"
                ),
            },
            ReconcilerCurrentStatus::Running(ReconcilerRunningStatus {
                activation_reason,
                started_at_time,
                running_for,
            }) => {
                writeln!(f, "currently running:")?;
                let mut f = IndentWriter::new("    ", f);
                writeln!(
                    f,
                    "activation reason: {}",
                    activation_reason.description()
                )?;
                writeln!(f, "started at {started_at_time}")?;
                write!(f, "running for {running_for:?}")
            }
            ReconcilerCurrentStatus::Idle => write!(f, "idle"),
        }
    }
}

/// Status of a single scrimlet reconciler.
=======
>>>>>>> f1fb43473 (move Display adapters to omdb)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "ReconcilerStatus{T}")]
pub struct ReconcilerStatus<T> {
    /// Status of the task at this moment.
    pub current_status: ReconcilerCurrentStatus,
    /// Final status of the most recent activation of this task.
    // Box the inner status to avoid clippy complaining about
    // `ScrimletReconcilersStatus::Running { ... }` being overly large.
    pub last_completion: Option<Box<ReconciliationCompletedStatus<T>>>,
}

/// Status of the collective set of scrimlet reconcilers.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status", content = "value")]
pub enum ScrimletReconcilersStatus {
    /// `sled-agent` has not yet provided underlay networking information.
    WaitingForSledAgentNetworkingInfo,

    /// We're attempting to determine our switch slot.
    DeterminingSwitchSlot(DetermineSwitchSlotStatus),

    /// We are a scrimlet and the individual reconcilers are running.
    Running {
        dpd_reconciler: ReconcilerStatus<dpd::DpdReconcilerStatus>,
        lldpd_reconciler: ReconcilerStatus<lldpd::LldpdReconcilerStatus>,
        mgd_reconciler: ReconcilerStatus<mgd::MgdReconcilerStatus>,
        uplinkd_reconciler: ReconcilerStatus<uplinkd::UplinkdReconcilerStatus>,
    },
}
