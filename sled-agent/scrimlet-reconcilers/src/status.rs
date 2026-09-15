// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the status and results of the reconcilers in this crate.

use bootstrap_agent_lockstep_types::scrimlet_reconcilers as api_status;
use chrono::DateTime;
use chrono::Utc;
use std::time::Duration;
use std::time::Instant;

pub(crate) use api_status::DetermineSwitchSlotStatus;
pub(crate) use api_status::ReconcilerActivationReason;
pub(crate) use api_status::ReconcilerInertReason;
pub(crate) use api_status::ReconciliationCompletedStatus;
pub(crate) use api_status::ScrimletStatus;

use api_status::dpd::DpdReconcilerStatus;
use api_status::lldpd::LldpdReconcilerStatus;
use api_status::mgd::MgdReconcilerStatus;
use api_status::uplinkd::UplinkdReconcilerStatus;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReconcilerRunningStatus {
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

impl From<ReconcilerRunningStatus> for api_status::ReconcilerRunningStatus {
    fn from(value: ReconcilerRunningStatus) -> Self {
        Self {
            activation_reason: value.activation_reason(),
            started_at_time: value.started_at(),
            running_for: value.elapsed_since_start(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ReconcilerCurrentStatus {
    /// The reconciler is inert: it will not or cannot run for some reason.
    Inert(ReconcilerInertReason),
    /// The reconciler is currently running.
    Running(ReconcilerRunningStatus),
    /// The reconciler is not currently running.
    Idle,
}

impl From<ReconcilerCurrentStatus> for api_status::ReconcilerCurrentStatus {
    fn from(value: ReconcilerCurrentStatus) -> Self {
        match value {
            ReconcilerCurrentStatus::Inert(reason) => Self::Inert(reason),
            ReconcilerCurrentStatus::Running(status) => {
                Self::Running(status.into())
            }
            ReconcilerCurrentStatus::Idle => Self::Idle,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ReconcilerStatus<T> {
    /// Status of the task at this moment.
    pub(crate) current_status: ReconcilerCurrentStatus,
    /// Final status of the most recent activation of this task.
    // Box the inner status to avoid clippy complaining about
    // `ScrimletReconcilersStatus::Running { ... }` being overly large.
    pub(crate) last_completion: Option<Box<ReconciliationCompletedStatus<T>>>,
}

impl<T> From<ReconcilerStatus<T>> for api_status::ReconcilerStatus<T> {
    fn from(value: ReconcilerStatus<T>) -> Self {
        Self {
            current_status: value.current_status.into(),
            last_completion: value.last_completion,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ScrimletReconcilersStatus {
    /// `sled-agent` has not yet provided underlay networking information.
    WaitingForSledAgentNetworkingInfo,

    /// We're attempting to determine our switch slot.
    DeterminingSwitchSlot(DetermineSwitchSlotStatus),

    /// We are a scrimlet and the individual reconcilers are running.
    Running {
        dpd_reconciler: ReconcilerStatus<DpdReconcilerStatus>,
        lldpd_reconciler: ReconcilerStatus<LldpdReconcilerStatus>,
        mgd_reconciler: ReconcilerStatus<MgdReconcilerStatus>,
        uplinkd_reconciler: ReconcilerStatus<UplinkdReconcilerStatus>,
    },
}

impl From<ScrimletReconcilersStatus> for api_status::ScrimletReconcilersStatus {
    fn from(value: ScrimletReconcilersStatus) -> Self {
        match value {
            ScrimletReconcilersStatus::WaitingForSledAgentNetworkingInfo => {
                Self::WaitingForSledAgentNetworkingInfo
            }
            ScrimletReconcilersStatus::DeterminingSwitchSlot(status) => {
                Self::DeterminingSwitchSlot(status)
            }
            ScrimletReconcilersStatus::Running {
                dpd_reconciler,
                lldpd_reconciler,
                mgd_reconciler,
                uplinkd_reconciler,
            } => Self::Running {
                dpd_reconciler: dpd_reconciler.into(),
                lldpd_reconciler: lldpd_reconciler.into(),
                mgd_reconciler: mgd_reconciler.into(),
                uplinkd_reconciler: uplinkd_reconciler.into(),
            },
        }
    }
}
