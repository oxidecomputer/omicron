// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use omicron_common::api::external::NameOrId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FederationTokenRequest {
    /// Trust policy name or UUID within the silo addressed by the request hostname.
    pub trust_policy: NameOrId,
    /// RS256-signed identity token issued by the policy's identity provider.
    pub oidc_jwt: String,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
pub struct FederationToken {
    /// Bearer credential for the federation session.
    pub token: String,
    /// Expiration time of the five-minute session.
    pub expires_at: DateTime<Utc>,
    /// Revision of the assumed trust policy.
    pub revision: i64,
}
