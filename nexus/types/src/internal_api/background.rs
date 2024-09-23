// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

/// The status of a `region_replacement` background task activation
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RegionReplacementStatus {
    pub requests_created_ok: Vec<String>,
    pub start_invoked_ok: Vec<String>,
    pub errors: Vec<String>,
}

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

/// The status of an `instance_updater` background task activation.
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct InstanceUpdaterStatus {
    /// if `true`, background instance updates have been explicitly disabled.
    pub disabled: bool,

    /// number of instances found with destroyed active VMMs
    pub destroyed_active_vmms: usize,

    /// number of instances found with failed active VMMs
    pub failed_active_vmms: usize,

    /// number of instances found with terminated active migrations
    pub terminated_active_migrations: usize,

    /// number of update sagas started.
    pub sagas_started: usize,

    /// number of sagas completed successfully
    pub sagas_completed: usize,

    /// errors returned by instance update sagas which failed, and the UUID of
    /// the instance which could not be updated.
    pub saga_errors: Vec<(Option<Uuid>, String)>,

    /// errors which occurred while querying the database for instances in need
    /// of updates.
    pub query_errors: Vec<String>,
}

impl InstanceUpdaterStatus {
    pub fn errors(&self) -> usize {
        self.saga_errors.len() + self.query_errors.len()
    }

    pub fn total_instances_found(&self) -> usize {
        self.destroyed_active_vmms
            + self.failed_active_vmms
            + self.terminated_active_migrations
    }
}

/// The status of an `instance_reincarnation` background task activation.
#[derive(Default, Serialize, Deserialize, Debug)]
pub struct InstanceReincarnationStatus {
    /// Total number of instances in need of reincarnation on this activation.
    pub instances_found: usize,
    /// UUIDs of instances reincarnated successfully by this activation.
    pub instances_reincarnated: Vec<Uuid>,
    /// UUIDs of instances which changed state before they could be
    /// reincarnated.
    pub changed_state: Vec<Uuid>,
    /// Any error that occured while finding instances in need of reincarnation.
    pub query_error: Option<String>,
    /// Errors that occurred while restarting individual instances.
    pub restart_errors: HashMap<Uuid, String>,
}
