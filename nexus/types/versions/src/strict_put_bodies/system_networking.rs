// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fleet-wide networking settings types for version STRICT_PUT_BODIES.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Parameters for updating the fleet-wide networking settings.
///
/// A `PUT` replaces the settings, so the opt-in toggle must be present.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SystemNetworkingSettingsUpdate {
    /// Toggle the fleet-wide external jumbo-frames opt-in.
    pub external_jumbo_frames_opt_in_enabled: bool,
}

impl From<crate::v2026_06_05_00::system_networking::SystemNetworkingSettingsUpdate>
    for SystemNetworkingSettingsUpdate
{
    fn from(
        old: crate::v2026_06_05_00::system_networking::SystemNetworkingSettingsUpdate,
    ) -> Self {
        Self {
            external_jumbo_frames_opt_in_enabled: old
                .external_jumbo_frames_opt_in_enabled,
        }
    }
}
