// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! System health types for version INITIAL.

use chrono::{DateTime, Utc};
use omicron_common::api::external::AllowedSourceIps as ExternalAllowedSourceIps;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// SYSTEM HEALTH

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PingStatus {
    Ok,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Ping {
    /// Whether the external API is reachable. Will always be Ok if the endpoint
    /// returns anything at all.
    pub status: PingStatus,
}

// ALLOWED SOURCE IPS

/// Allowlist of IPs or subnets that can make requests to user-facing services.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AllowList {
    /// Time the list was created.
    pub time_created: DateTime<Utc>,
    /// Time the list was last modified.
    pub time_modified: DateTime<Utc>,
    /// The allowlist of IPs or subnets.
    pub allowed_ips: ExternalAllowedSourceIps,
}

// SYSTEM PARAMS

/// Parameters for updating allowed source IPs
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AllowListUpdate {
    /// The new list of allowed source IPs.
    pub allowed_ips: ExternalAllowedSourceIps,
}
