// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! MUPdate types for the commissioning API.
//!
//! The progress types here are a projection of wicketd's internal event buffers.

use std::time::Duration;

use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1;
use crate::v1::inventory::SpIdentifier;
use crate::v1::update::UpdateStepStatus;

/// The response to a request for update progress.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct GetUpdateProgressResponse {
    /// The service processors that have update state, and their progress.
    ///
    /// A service processor appears here only once an update has been started
    /// for it, and disappears when its update state is cleared.
    pub sps: IdOrdMap<SpUpdateProgress>,
}

impl From<GetUpdateProgressResponse> for v1::update::GetUpdateProgressResponse {
    fn from(new: GetUpdateProgressResponse) -> Self {
        Self { sps: new.sps.into_iter().map(Into::into).collect() }
    }
}

/// Update progress for a single service processor.
///
/// An SP's update progress is only provided once an update has been started for
/// it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SpUpdateProgress {
    /// The service processor this progress describes.
    pub sp: SpIdentifier,
    /// The update progress for that service processor.
    pub progress: UpdateProgress,
}

impl IdOrdItem for SpUpdateProgress {
    type Key<'a> = SpIdentifier;

    fn key(&self) -> Self::Key<'_> {
        self.sp
    }

    id_upcast!();
}

impl From<SpUpdateProgress> for v1::update::SpUpdateProgress {
    fn from(new: SpUpdateProgress) -> Self {
        Self { sp: new.sp, progress: new.progress.into() }
    }
}

/// The progress of a single update execution: its overall state and its steps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct UpdateProgress {
    /// The overall rollup state of this execution.
    pub state: UpdateState,
    /// The steps of this execution, in order.
    pub steps: Vec<UpdateStep>,
}

impl From<UpdateProgress> for v1::update::UpdateProgress {
    fn from(new: UpdateProgress) -> Self {
        Self {
            state: new.state.into(),
            steps: new.steps.into_iter().map(Into::into).collect(),
        }
    }
}

/// The state of an update execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum UpdateState {
    /// An update has been started, but execution has not begun yet.
    Waiting,
    /// The update is currently running.
    Running {
        /// The time elapsed since the update started.
        elapsed: Duration,
    },
    /// The update ran to completion.
    ///
    /// Whether individual steps performed work or were skipped (for example,
    /// because a component was already at its target version) is recorded in
    /// the per-step outcomes.
    Completed {
        /// The time it took for the update to complete.
        ///
        /// Absent when the update was interrupted, and completion was inferred
        /// rather than reported.
        elapsed: Option<Duration>,
    },
    /// The update failed.
    Failed {
        /// A human-readable description of the failure, combining the failed
        /// step and its error message (including any nested causes).
        message: String,
        /// The time it took for the update to fail.
        ///
        /// Absent when the update was interrupted, and failure was inferred
        /// rather than reported.
        elapsed: Option<Duration>,
    },
    /// The update was aborted.
    Aborted {
        /// A human-readable description of the abort, combining the aborted
        /// step and its message.
        message: String,
        /// The time it took for the update to abort.
        ///
        /// Absent when the update was interrupted, and the abort was inferred
        /// rather than reported.
        elapsed: Option<Duration>,
    },
}

impl From<UpdateState> for v1::update::UpdateState {
    fn from(new: UpdateState) -> Self {
        match new {
            UpdateState::Waiting => Self::Waiting,
            UpdateState::Running { elapsed: _ } => Self::Running,
            UpdateState::Completed { elapsed: _ } => Self::Completed,
            UpdateState::Failed { message, elapsed: _ } => {
                Self::Failed { message }
            }
            UpdateState::Aborted { message, elapsed: _ } => {
                Self::Aborted { message }
            }
        }
    }
}

/// A single node in the update step tree.
///
/// A step may spawn nested executions (for example, a per-component update that
/// runs its own engine). Those executions appear in `children`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct UpdateStep {
    /// A human-readable description of this step.
    pub description: String,
    /// The status of this step.
    pub status: UpdateStepStatus,
    /// The nested executions this step spawned, in order. Empty if the step ran
    /// no nested engine.
    pub children: Vec<UpdateProgress>,
}

impl From<UpdateStep> for v1::update::UpdateStep {
    fn from(new: UpdateStep) -> Self {
        Self {
            description: new.description,
            status: new.status,
            children: new.children.into_iter().map(Into::into).collect(),
        }
    }
}
