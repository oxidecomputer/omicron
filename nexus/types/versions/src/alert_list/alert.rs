// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2025_11_20_00::alert::AlertSubscription;
use crate::v2025_11_20_00::asset::AssetIdentityMetadata;

/// An alert.
///
/// Alerts represent edge-triggered notifications of an event that occurred in
/// the system at a point in time. See the guide-level documentation alerts for
/// details.
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
    /// version. Clients should expect to encounter earlier schema versions for
    /// a given alert class, if the alert was recorded on an earlier Oxide
    /// system software version.
    pub version: u32,
    /// The alert's data payload.
    ///
    /// The schema for this object is determined based on the alert class and
    /// version.
    pub alert: serde_json::Value,
}

/// Query parameters for listing alerts
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct AlertListParams {
    /// Optional alert class or glob pattern used to filter alerts
    ///
    /// See the guide-level documentation on alerts for details on alert classes
    /// and alert class glob patterns.
    pub classes: Option<AlertSubscription>,
    /// Inclusive lower bound on the alert creation time.
    ///
    /// If this is included, only alerts created at or after this time will be
    /// returned.
    pub start_time: Option<DateTime<Utc>>,
    /// Inclusive upper bound on the alert creation time
    ///
    /// If this is included, only alerts created at or before this time will be
    /// returned.
    pub end_time: Option<DateTime<Utc>>,
}
