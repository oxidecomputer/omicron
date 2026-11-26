// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Expunge precheck types for version `EXPUNGE_PRECHECK_CONTROL_PLANE`.

use crate::v2026_11_24_00;
use crate::v2026_11_24_00::expunge_precheck::DistributedStoragePrecheck;
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
    /// Whether data in distributed storage that was stored on expunged
    /// physical disks has been fully re-replicated
    pub distributed_storage: DistributedStoragePrecheck,
    /// Whether the control plane has finished recovering from previous
    /// expungements
    pub control_plane: ControlPlanePrecheck,
}

/// Result of the check that the control plane has finished recovering from
/// previous expungements
///
/// This check passes when the system has accounted for all expunged sleds and
/// physical disks, has finished deploying replacements for any control plane
/// components that were lost, and those replacements are working.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ControlPlanePrecheck {
    /// Human-readable descriptions of risks found by this check
    ///
    /// If this list is empty, this check passed.
    pub risks: Vec<String>,
}

// Response-only type: convert from new to old.
impl From<ExpungePrecheck>
    for v2026_11_24_00::expunge_precheck::ExpungePrecheck
{
    fn from(new: ExpungePrecheck) -> Self {
        // Destructure the whole struct so that adding a field causes a compile
        // error here.  The older version has no place for the control plane
        // check.  Older clients get the result they would have gotten before
        // this check existed, so `ok` reflects only the checks that they can
        // see.
        let ExpungePrecheck { ok: _, distributed_storage, control_plane: _ } =
            new;
        Self { ok: distributed_storage.risks.is_empty(), distributed_storage }
    }
}
