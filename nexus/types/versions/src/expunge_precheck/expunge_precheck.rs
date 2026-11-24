// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Expunge precheck types for version `EXPUNGE_PRECHECK`.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Results of checks for conditions that make it risky to expunge a sled or
/// physical disk
///
/// The system is designed to survive only a limited number of concurrent
/// failures. When a sled or physical disk is expunged, the system repairs
/// itself by re-replicating user data and control plane data and by deploying
/// replacement control plane components. Each expunged component counts as
/// one failure until these repairs finish. Other conditions, like a sled that
/// is not reporting its status, can also reduce the number of additional
/// failures that the system can survive. Before expunging a sled or disk,
/// operators should use these checks to confirm that it's safe to do so.
///
/// These checks do not yet report whether replacement control plane
/// components have been deployed.
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
    /// The time at which the inventory collection used for these checks was
    /// completed.
    ///
    /// Problems that occurred after this time may not be reflected here.
    pub inventory_collection_time: DateTime<Utc>,
    /// Whether the inventory collection used for these checks is recent
    pub inventory: ExpungePrecheckResult<InventoryProblemKind>,
    /// Whether user data stored on expunged physical disks has been fully
    /// re-replicated
    pub user_data: ExpungePrecheckResult<UserDataProblemKind>,
    /// Whether the control plane database is fully replicated
    pub cockroachdb: ExpungePrecheckResult<CockroachdbProblemKind>,
    /// Whether all sleds are reporting their status
    pub sleds: ExpungePrecheckResult<SledsProblemKind>,
}

/// Result of one expunge precheck
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(rename = "ExpungePrecheckResult{K}")]
pub struct ExpungePrecheckResult<K> {
    /// A human-readable summary of what this check found
    ///
    /// The summary is present whether or not the check found any problems.
    pub summary: String,
    /// Problems found by this check
    ///
    /// If this list is empty, this check passed.
    pub problems: Vec<ExpungePrecheckProblem<K>>,
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
