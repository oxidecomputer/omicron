// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack membership types for version SLED_ADD_PRECHECK_OVERRIDES.

use crate::v2026_01_21_00;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;
use std::collections::BTreeSet;

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
pub struct RackMembershipAddSledsRequest {
    pub sled_ids: BTreeSet<BaseboardId>,
    /// Optional overrides for various safety checks
    ///
    /// Overriding checks is dangerous and should only be done at the direction
    /// of Oxide support.
    #[serde(default)]
    pub override_prechecks: SledAddPrecheckOverrides,
}

impl From<v2026_01_21_00::rack::RackMembershipAddSledsRequest>
    for RackMembershipAddSledsRequest
{
    fn from(old: v2026_01_21_00::rack::RackMembershipAddSledsRequest) -> Self {
        let v2026_01_21_00::rack::RackMembershipAddSledsRequest { sled_ids } =
            old;
        RackMembershipAddSledsRequest {
            sled_ids,
            override_prechecks: SledAddPrecheckOverrides::default(),
        }
    }
}

/// Instructs the system to override safety checks when adding this sled
#[derive(
    Clone,
    Debug,
    Default,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
pub struct SledAddPrecheckOverrides {
    /// Add the requested sleds even if the system has not settled on its
    /// current target release
    ///
    /// This allows sleds to be added when the system is in the middle of a
    /// software update or if it has recently been MUPdated and that MUPdate
    /// hasn't been resolved.  This is not generally supported because in order
    /// for adding a sled to work, the versions of software on that sled must
    /// match the rest of the control plane, but that's not well-defined in
    /// these two cases
    ///
    /// This override is provided for working around bugs and only in emergency
    /// situations.  It should not generally be used.
    #[serde(default)]
    pub allow_without_settled_target_release: bool,

    /// Add the requested sleds even if their software does not match the
    /// current target release
    ///
    /// This allows sleds to be added even when their SP, RoT, RoT bootloader,
    /// or host OS phase 1 images cannot be confirmed to match those expected by
    /// the control plane.  Adding sleds with mismatched software is not
    /// supported since the control plane needs compatible versions in order to
    /// communicate with the sled.
    ///
    /// This override is provided for working around bugs and only in emergency
    /// situations.  It should not generally be used.
    #[serde(default)]
    pub allow_unverified_sled_software: bool,
}
