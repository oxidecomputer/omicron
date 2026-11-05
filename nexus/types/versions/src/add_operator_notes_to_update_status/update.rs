// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00::update::TargetRelease;
use crate::v2026_05_20_00;

use chrono::{DateTime, Utc};
use omicron_common::api::external::Nullable;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UpdateStatus {
    /// Current target release of the system software
    ///
    /// This may not correspond to the actual system software running
    /// at the time of request; it is instead the release that the system
    /// should be moving towards as a goal state. The system asynchronously
    /// updates software to match this target release.
    ///
    /// Will only be null if a target release has never been set. In that case,
    /// the system is not automatically attempting to manage software versions.
    pub target_release: Nullable<TargetRelease>,

    /// Count of components running each release version
    ///
    /// Keys will be either:
    ///
    /// * Semver-like release version strings
    /// * "install dataset", representing the initial rack software before
    ///   any updates
    /// * "unknown", which means there is no TUF repo uploaded that matches
    ///   the software running on the component)
    pub components_by_release_version: BTreeMap<String, usize>,

    /// Time of most recent update planning activity
    ///
    /// This is intended as a rough indicator of the last time something
    /// happened in the update planner.
    pub time_last_step_planned: DateTime<Utc>,

    /// Whether automatic update is suspended due to manual update activity
    ///
    /// After a manual support procedure that changes the system software,
    /// automatic update activity is suspended to avoid undoing the change. To
    /// resume automatic update, first upload the TUF repository matching the
    /// manually applied update, then set that as the target release.
    pub suspended: bool,

    /// If true, the system has detected one or more known conditions that
    /// require Oxide support to resolve
    ///
    /// You should contact support to resolve these issues before proceeding
    /// with an update, or after one has completed.
    /// The checks underlying this field are not exhaustive, so this being
    /// `false` does not mean the entire system is completely healthy.
    pub contact_support: bool,

    /// Summary of what the system is doing
    ///
    /// This summarizes the most recent automated planning run on the Nexus
    /// instance that handled this request. If that instance has not completed
    /// a planning run since it started, the status is `unknown`.
    pub operator_notes: OperatorNotes,

    /// Error from the most recent automated planning run, if it failed
    ///
    /// When this is set, `operator_notes` may describe an earlier planning
    /// run.
    pub planner_error: Nullable<String>,
}

// Response-only type: convert from new to old.
impl From<UpdateStatus> for v2026_05_20_00::update::UpdateStatus {
    fn from(new: UpdateStatus) -> Self {
        Self {
            target_release: new.target_release,
            components_by_release_version: new.components_by_release_version,
            time_last_step_planned: new.time_last_step_planned,
            suspended: new.suspended,
            contact_support: new.contact_support,
        }
    }
}

/// Summary of what the system is doing, intended for operators
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct OperatorNotes {
    /// Time when the summarized planning run started
    ///
    /// This is null if no planning run has completed, in which case the status
    /// is `unknown`.
    pub time_planned: Nullable<DateTime<Utc>>,
    /// What the system is doing
    pub status: OperatorStatus,
    /// Human-readable description of `status`
    pub status_message: String,
}

/// What the system is doing, as summarized for operators
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", content = "details", rename_all = "snake_case")]
pub enum OperatorStatus {
    /// The reconfiguration system is at rest because no changes are required
    AtRest,
    /// The system is busy with reconfigurations that don't have a more specific
    /// description
    Busy,
    /// The system is waiting for resolution after having detected MUPdates
    WaitingForMupdateResolution,
    /// The system is working on an automated software update
    WaitingForUpdate(UpdateWait),
    /// Unable to determine the current status
    Unknown(UnknownReason),
}

/// Why the current status could not be determined
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UnknownReason {
    /// The planner has not yet run
    PlannerNotYetRun,
    /// The planner has run, but it has never succeeded
    PlannerFailed {
        /// Error from the most recent planning run
        error: String,
    },
}

/// What an automated software update is waiting for
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UpdateWait {
    /// The system is waiting for internal services to recover before proceeding
    /// with next steps
    ServiceRecovery { service: String },
    /// The system is waiting for a sled to be evacuated so it can be updated
    SledEvacuation {
        sled_id: SledUuid,
        baseboard_id: BaseboardId,
        progress: SledEvacuationProgress,
    },
    /// The system is waiting for a sled to be put back into service
    SledAvailable { baseboard_id: BaseboardId },
    /// The system is waiting for a board update to finish
    Mgs { baseboard_id: BaseboardId, component: String },
    /// The system is waiting for zone updates to finish
    Zones {
        /// Kinds of the zones being updated
        kinds: BTreeSet<String>,
    },
    /// The system is waiting for sagas to finish running
    HandoffSagas,
    /// The system is waiting for the final handoff to the new Nexus instances
    HandoffFinal,
}

/// How far along a sled's evacuation is, as far as the planner knows
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SledEvacuationProgress {
    /// The planner is marking the sled for evacuation in this blueprint.
    ///
    /// The planner has not yet checked on the evacuation's progress, so it
    /// doesn't know how many VMMs are on the sled.
    Starting,
    /// The sled still has this many VMMs registered.
    RegisteredVmms { count: NonZeroUsize },
    /// The planner is waiting for something other than VMMs to leave the
    /// sled (e.g., for inventory to report the sled's latest config).
    Other { message: String },
}
