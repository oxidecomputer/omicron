// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ServiceAccountTokenCreate {
    /// Requested token lifetime in seconds. Must not exceed the remaining
    /// lifetime of the authenticating token. If omitted, inherits that token's
    /// expiration, or has no expiration if the caller has none.
    pub ttl_seconds: Option<NonZeroU32>,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
pub struct ServiceAccountTokenGrant {
    /// UUID identifying the issued token.
    pub id: Uuid,
    /// Bearer token, returned only when issued.
    pub token: String,
    /// Expiration timestamp. Null means the token does not automatically expire.
    pub time_expires: Option<DateTime<Utc>>,
}
