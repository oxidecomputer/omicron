// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the scrimlet reconcilers responsible for
//! syncing network configuration from the bootstore to the services in the
//! switch zone.

use chrono::DateTime;
use chrono::Utc;
use indent_write::fmt::IndentWriter;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Write;
use std::time::Duration;

pub mod dpd;
pub mod lldpd;
pub mod mgd;
pub mod uplinkd;

trait DisplayableStatus {
    type DisplayAdapter<'a>: fmt::Display
    where
        Self: 'a;

    fn display<'a>(&'a self) -> Self::DisplayAdapter<'a>;
}

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

impl ReconcilerActivationReason {
    fn description(&self) -> &'static str {
        match self {
            Self::Startup => "first execution on start",
            Self::PeriodicTimer => "periodic timer fired",
            Self::SystemNetworkingConfigChanged => "networking config changed",
            Self::ScrimletStatusChanged => "switch presence changed",
        }
    }
}

/// Status of a completed-in-the-past reconciliation attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationCompletedStatus<T> {
    /// Why the reconciliation attempt fired.
    pub activation_reason: ReconcilerActivationReason,
    /// When the attempt completed.
    pub completed_at_time: DateTime<Utc>,
    /// How long the attempt ran.
    pub ran_for: Duration,
    /// The 0-based index of this reconciler's activation since the last time
    /// sled-agent started.
    pub activation_count: u64,
    /// Reconciler-specific status.
    pub status: T,
}

// shadow type for our manual JsonScema impl on the real
// ReconciliationCompletedStatus
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ReconciliationCompletedStatusShadow<T> {
    activation_reason: ReconcilerActivationReason,
    completed_at_time: DateTime<Utc>,
    ran_for: Duration,
    activation_count: u64,
    status: T,
}

impl<T> JsonSchema for ReconciliationCompletedStatus<T>
where
    T: JsonSchema,
{
    fn schema_name() -> String {
        format!("ReconciliationCompletedStatus{}", T::schema_name())
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        ReconciliationCompletedStatusShadow::<T>::json_schema(generator)
    }
}

impl<T> ReconciliationCompletedStatus<T> {
    pub fn display(&self) -> ReconciliationCompletedStatusDisplay<'_, T> {
        ReconciliationCompletedStatusDisplay(self)
    }
}

pub struct ReconciliationCompletedStatusDisplay<'a, T>(
    &'a ReconciliationCompletedStatus<T>,
);

impl<T> fmt::Display for ReconciliationCompletedStatusDisplay<'_, T>
where
    T: DisplayableStatus,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ReconciliationCompletedStatus {
            activation_reason,
            completed_at_time,
            ran_for,
            activation_count,
            status,
        } = self.0;
        writeln!(f, "activation reason: {}", activation_reason.description())?;
        writeln!(f, "activation count: {activation_count}")?;
        writeln!(f, "completed at {completed_at_time}")?;
        writeln!(f, "ran for {ran_for:?}")?;
        writeln!(f, "detailed status:")?;
        write!(IndentWriter::new("    ", f), "{}", status.display())
    }
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcilerStatus<T> {
    /// Status of the task at this moment.
    pub current_status: ReconcilerCurrentStatus,
    /// Final status of the most recent activation of this task.
    // Box the inner status to avoid clippy complaining about
    // `ScrimletReconcilersStatus::Running { ... }` being overly large.
    pub last_completion: Option<Box<ReconciliationCompletedStatus<T>>>,
}

// shadow type for our manual JsonScema impl on the real ReconcilerStatus
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ReconcilerStatusShadow<T> {
    /// Status of the task at this moment.
    current_status: ReconcilerCurrentStatus,
    /// Final status of the most recent activation of this task.
    // Box the inner status to avoid clippy complaining about
    // `ScrimletReconcilersStatus::Running { ... }` being overly large.
    last_completion: Option<Box<ReconciliationCompletedStatus<T>>>,
}

impl<T> JsonSchema for ReconcilerStatus<T>
where
    T: JsonSchema,
{
    fn schema_name() -> String {
        format!("ReconcilerStatus{}", T::schema_name())
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        ReconcilerStatusShadow::<T>::json_schema(generator)
    }
}

impl<T> ReconcilerStatus<T> {
    pub fn display(&self) -> ReconcilerStatusDisplay<'_, T> {
        ReconcilerStatusDisplay(self)
    }
}

pub struct ReconcilerStatusDisplay<'a, T>(&'a ReconcilerStatus<T>);

impl<T> fmt::Display for ReconcilerStatusDisplay<'_, T>
where
    T: DisplayableStatus,
{
    fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ReconcilerStatus { current_status, last_completion } = self.0;
        writeln!(f, "current status:")?;
        writeln!(
            IndentWriter::new("    ", &mut f),
            "{}",
            current_status.display()
        )?;

        if let Some(last_completion) = last_completion {
            writeln!(f, "last completion:")?;
            writeln!(
                IndentWriter::new("    ", f),
                "{}",
                last_completion.display()
            )?;
        } else {
            writeln!(f, "last completion: none")?;
        }
        Ok(())
    }
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

impl ScrimletReconcilersStatus {
    /// Get a `fmt::Display`-able version of this status (e.g., for `omdb`).
    pub fn display(&self) -> ScrimletReconcilersStatusDisplay<'_> {
        ScrimletReconcilersStatusDisplay(self)
    }
}

pub struct ScrimletReconcilersStatusDisplay<'a>(&'a ScrimletReconcilersStatus);

impl fmt::Display for ScrimletReconcilersStatusDisplay<'_> {
    fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ScrimletReconcilersStatus::WaitingForSledAgentNetworkingInfo => {
                write!(
                    f,
                    "not running: sled-agent has not yet initialized \
                     the reconciler subsystem with networking information"
                )
            }
            ScrimletReconcilersStatus::DeterminingSwitchSlot(status) => {
                match status {
                    DetermineSwitchSlotStatus::NotScrimlet => {
                        write!(f, "not running: no switch detected")
                    }
                    DetermineSwitchSlotStatus::ContactingMgs {
                        prev_attempt_err,
                    } => {
                        writeln!(
                            f,
                            "not yet running: attempting to contact MGS \
                             to determine our location"
                        )?;
                        if let Some(err) = prev_attempt_err {
                            write!(
                                f,
                                "    previous MGS contact attempt failed: {err}"
                            )?;
                        }
                        Ok(())
                    }
                    DetermineSwitchSlotStatus::WaitingToRetry {
                        prev_attempt_err,
                    } => {
                        writeln!(
                            f,
                            "not yet running: sleeping before retrying \
                             contacting MGS to determine our location"
                        )?;
                        write!(
                            f,
                            "    previous MGS contact attempt failed: \
                                 {prev_attempt_err}"
                        )
                    }
                }
            }
            ScrimletReconcilersStatus::Running {
                dpd_reconciler,
                lldpd_reconciler,
                mgd_reconciler,
                uplinkd_reconciler,
            } => {
                for (name, displayable) in [
                    ("dpd", &dpd_reconciler.display() as &dyn fmt::Display),
                    ("mgd", &mgd_reconciler.display()),
                    ("lldpd", &lldpd_reconciler.display()),
                    ("uplinkd", &uplinkd_reconciler.display()),
                ] {
                    writeln!(f, "{name} reconciler:")?;
                    writeln!(
                        IndentWriter::new("    ", &mut f),
                        "{displayable}"
                    )?;
                }
                Ok(())
            }
        }
    }
}
