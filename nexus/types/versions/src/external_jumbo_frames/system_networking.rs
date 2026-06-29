// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fleet-wide networking settings.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Fleet-wide networking settings. Only fleet viewers may view these settings.
/// Only fleet admins can modify them.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SystemNetworkingSettings {
    /// When true, end users may opt in to jumbo frames (8500 byte MTU) on the
    /// primary interface of an instance. When false, instance-level opt-in is
    /// ignored and OPTE ports are created with the default MTU.
    pub external_jumbo_frames_opt_in_enabled: bool,
}

/// Parameters for updating the fleet-wide networking settings.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SystemNetworkingSettingsUpdate {
    /// Toggle the fleet-wide external jumbo-frames opt-in.
    #[serde(default)]
    pub external_jumbo_frames_opt_in_enabled: bool,
}
