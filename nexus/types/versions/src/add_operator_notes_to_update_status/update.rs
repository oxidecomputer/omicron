// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00::update::TargetRelease;
use crate::v2026_05_20_00;

use chrono::{DateTime, Utc};
use omicron_common::api::external::Nullable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

    /// Summary of what the update / reconfiguration system is doing
    pub operator_notes: OperatorNotes,

    /// Error from the most recent run of the update / reconfiguration planner,
    /// if it failed
    pub planner_error: Nullable<String>,
}

// Response-only type: convert from new to old.
impl From<UpdateStatus> for v2026_05_20_00::update::UpdateStatus {
    fn from(new: UpdateStatus) -> Self {
        // Destructure the whole struct so that adding a field causes a compile
        // error here.  The older version has no place for the operator notes
        // or the planner error, but those can be safely dropped.
        let UpdateStatus {
            target_release,
            components_by_release_version,
            time_last_step_planned,
            suspended,
            contact_support,
            operator_notes: _,
            planner_error: _,
        } = new;
        Self {
            target_release,
            components_by_release_version,
            time_last_step_planned,
            suspended,
            contact_support,
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
///
/// The `status_message` field of `OperatorNotes` describes the status in more
/// detail.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum OperatorStatus {
    /// The reconfiguration system is at rest because no changes are required
    AtRest,
    /// The system is busy with reconfigurations that don't have a more specific
    /// description
    Busy,
    /// The system is waiting for resolution after having detected MUPdates
    WaitingForMupdateResolution,
    /// The system is working on an automated software update
    WaitingForUpdate,
    /// Unable to determine the current status
    ///
    /// If the most recent planning run failed, `planner_error` in
    /// `UpdateStatus` describes the error.
    Unknown,
}
