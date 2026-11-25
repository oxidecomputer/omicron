// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Expunge precheck types for version `EXPUNGE_PRECHECK`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Result of checking whether it'd be dangerous to expunge a working sled or
/// physical disk
///
/// If `ok` is `true`, no check found any risks. Otherwise, each check lists the
/// risks that it found.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ExpungePrecheck {
    /// Whether all checks passed
    ///
    /// This is `true` if and only if none of the checks below found any
    /// risks.
    pub ok: bool,
    /// Whether user data stored on expunged physical disks has been fully
    /// re-replicated
    pub user_data: UserDataPrecheck,
}

/// Result of the check that user data on expunged physical disks has been
/// fully re-replicated
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UserDataPrecheck {
    /// Number of expunged physical disks that still hold user data
    pub expunged_disks_with_data: u64,
    /// Number of region replacements that have not finished
    pub region_replacements_in_progress: u64,
    /// Number of region snapshot replacements that have not finished
    pub region_snapshot_replacements_in_progress: u64,
    /// Human-readable descriptions of risks found by this check
    ///
    /// If this list is empty, this check passed.
    pub risks: Vec<String>,
}
