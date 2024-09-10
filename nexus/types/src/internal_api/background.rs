// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use serde::Deserialize;
use serde::Serialize;

/// The status of a `region_replacement_drive` background task activation
#[derive(Serialize, Deserialize, Default)]
pub struct RegionReplacementDriverStatus {
    pub drive_invoked_ok: Vec<String>,
    pub finish_invoked_ok: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of a `lookup_region_port` background task activation
#[derive(Serialize, Deserialize, Default)]
pub struct LookupRegionPortStatus {
    pub found_port_ok: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of a `region_snapshot_replacement_start` background task
/// activation
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct RegionSnapshotReplacementStartStatus {
    pub requests_created_ok: Vec<String>,
    pub start_invoked_ok: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of a `region_snapshot_replacement_garbage_collect` background
/// task activation
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct RegionSnapshotReplacementGarbageCollectStatus {
    pub garbage_collect_requested: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of a `region_snapshot_replacement_step` background task
/// activation
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct RegionSnapshotReplacementStepStatus {
    pub step_records_created_ok: Vec<String>,
    pub step_garbage_collect_invoked_ok: Vec<String>,
    pub step_invoked_ok: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of a `region_snapshot_replacement_finish` background task activation
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct RegionSnapshotReplacementFinishStatus {
    pub records_set_to_done: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of an `abandoned_vmm_reaper` background task activation.
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct AbandonedVmmReaperStatus {
    pub vmms_found: usize,
    pub sled_reservations_deleted: usize,
    pub vmms_deleted: usize,
    pub vmms_already_deleted: usize,
    pub errors: Vec<String>,
}
