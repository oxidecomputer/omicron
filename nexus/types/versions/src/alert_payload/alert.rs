// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2025_11_20_00::asset::AssetIdentityMetadata;
use crate::v2026_08_14_00;

/// An alert.
///
/// Alerts provide notifications about events that occurred in the system at a
/// point in time. See the guide-level documentation on alerts for details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Alert {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    /// The alert's class.
    ///
    /// See the guide-level documentation on alerts for details on alert
    /// classes.
    pub class: String,
    /// The schema version of this alert's data payload.
    ///
    /// Alert schemas are versioned on a per-alert-class basis. The schema
    /// version for a particular alert class does not correspond to an Oxide API
    /// version. Clients should expect to encounter earlier schema versions when
    /// retrieving alerts recorded by an earlier version of the system software.
    ///
    /// See the guide-level documentation on alerts for details.
    pub version: u32,
    /// The alert's data payload.
    ///
    /// The schema for this object depends on the alert class and version.
    pub payload: serde_json::Value,
}

impl From<Alert> for v2026_08_14_00::alert::Alert {
    fn from(new: Alert) -> Self {
        Self {
            identity: new.identity,
            class: new.class,
            version: new.version,
            alert: new.payload,
        }
    }
}
