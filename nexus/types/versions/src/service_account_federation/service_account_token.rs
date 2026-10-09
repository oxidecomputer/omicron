// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ServiceAccountFederationTokenCreate {
    /// Signed OIDC ID token from the configured identity provider.
    pub jwt: String,
    /// Requested lifetime in seconds. Must not exceed the configured federation
    /// maximum or the JWT's remaining lifetime. If omitted, uses the earlier limit.
    pub ttl_seconds: Option<NonZeroU32>,
}
