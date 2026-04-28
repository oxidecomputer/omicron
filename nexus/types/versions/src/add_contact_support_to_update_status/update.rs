// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::update::TargetRelease;

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

    /// Whether the user should contact support
    ///
    /// This is a rough high-level indicator of overall system health based on
    /// a subset of components. When true, one or more components may be
    /// experiencing issues and you should contact support.
    pub contact_support: bool,
}

// Response-only type: convert from new to old.
impl From<UpdateStatus> for v2025_11_20_00::update::UpdateStatus {
    fn from(new: UpdateStatus) -> Self {
        Self {
            target_release: new.target_release,
            components_by_release_version: new.components_by_release_version,
            time_last_step_planned: new.time_last_step_planned,
            suspended: new.suspended,
        }
    }
}
