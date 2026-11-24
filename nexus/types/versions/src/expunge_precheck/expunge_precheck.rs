// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Expunge precheck types for version `EXPUNGE_PRECHECK`.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Result of checking whether it'd be dangerous to expunge a working sled or
/// physical disk
///
/// If `ok` is `true`, no check found any problems. Otherwise, each check lists
/// the problems that it found.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ExpungePrecheck {
    /// Whether all checks passed
    ///
    /// This is `true` if and only if none of the checks below found any
    /// problems.
    pub ok: bool,
    /// Whether the inventory collection used for these checks is recent
    pub inventory: InventoryPrecheck,
    /// Whether user data stored on expunged physical disks has been fully
    /// re-replicated
    pub user_data: UserDataPrecheck,
    /// Whether the control plane database is fully replicated
    pub cockroachdb: CockroachdbPrecheck,
    /// Whether all sleds are reporting their status
    pub sleds: SledsPrecheck,
}

/// Result of the check that the latest inventory collection is recent
///
/// The other checks use this inventory collection. Problems that occurred
/// after it was collected may not be reflected in their results.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InventoryPrecheck {
    /// The time at which the inventory collection started
    pub time_started: DateTime<Utc>,
    /// The time at which the inventory collection finished
    pub time_done: DateTime<Utc>,
    /// Problems found by this check
    ///
    /// If this list is empty, this check passed.
    pub problems: Vec<ExpungePrecheckProblem<InventoryProblemKind>>,
}

/// Result of the check that user data on expunged physical disks has been
/// fully re-replicated
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UserDataPrecheck {
    /// Number of expunged physical disks that still hold user data
    pub expunged_disks_with_data: usize,
    /// Number of region replacements that have not finished
    pub region_replacements_in_progress: u64,
    /// Number of region snapshot replacements that have not finished
    pub region_snapshot_replacements_in_progress: u64,
    /// Problems found by this check
    ///
    /// If this list is empty, this check passed.
    pub problems: Vec<ExpungePrecheckProblem<UserDataProblemKind>>,
}

/// Result of the check that the control plane database is fully replicated
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct CockroachdbPrecheck {
    /// Number of CockroachDB nodes that are expected to be running
    pub nodes_expected: usize,
    /// Number of CockroachDB nodes that reported their status
    pub nodes_reporting: usize,
    /// Number of reporting CockroachDB nodes that reported no
    /// under-replicated ranges and at least the expected number of live nodes
    pub nodes_healthy: usize,
    /// Problems found by this check
    ///
    /// If this list is empty, this check passed.
    pub problems: Vec<ExpungePrecheckProblem<CockroachdbProblemKind>>,
}

/// Result of the check that all in-service sleds are reporting their status
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SledsPrecheck {
    /// Number of in-service sleds
    pub in_service: usize,
    /// Number of in-service sleds that are present in the latest inventory
    /// collection
    pub in_service_in_inventory: usize,
    /// Problems found by this check
    ///
    /// If this list is empty, this check passed.
    pub problems: Vec<ExpungePrecheckProblem<SledsProblemKind>>,
}

/// A problem found by an expunge precheck
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(rename = "ExpungePrecheckProblem{K}")]
pub struct ExpungePrecheckProblem<K> {
    /// The kind of problem
    pub kind: K,
    /// A human-readable description of this problem
    pub message: String,
}

/// Kinds of problems found by the inventory check
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum InventoryProblemKind {
    /// The latest inventory collection is too old to reliably reflect the
    /// current state of the system.
    TooOld,
}

/// Kinds of problems found by the user data check
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum UserDataProblemKind {
    /// An expunged physical disk still holds user data that has not been
    /// re-replicated elsewhere.
    ExpungedDiskHasData,
    /// A replacement of a copy of user data is still in progress.
    ReplacementInProgress,
}

/// Kinds of problems found by the CockroachDB check
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum CockroachdbProblemKind {
    /// A CockroachDB node reports that some data is under-replicated.
    UnderReplicated,
    /// The status of a CockroachDB node is unavailable or incomplete.
    StatusMissing,
    /// Fewer CockroachDB nodes are live than expected.
    TooFewLiveNodes,
}

/// Kinds of problems found by the sleds check
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum SledsProblemKind {
    /// An in-service sled is missing from the latest inventory collection.
    MissingFromInventory,
}
