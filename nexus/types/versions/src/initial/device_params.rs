// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Device authentication parameters for the Nexus external API.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAuthRequest {
    pub client_id: Uuid,
    /// Optional lifetime for the access token in seconds.
    ///
    /// This value will be validated during the confirmation step. If not
    /// specified, it defaults to the silo's max TTL, which can be seen at
    /// `/v1/auth-settings`.  If specified, must not exceed the silo's max TTL.
    ///
    /// Some special logic applies when authenticating the confirmation request
    /// with an existing device token: the requested TTL must not produce an
    /// expiration time later than the authenticating token's expiration. If no
    /// TTL is specified, the expiration will be the lesser of the silo max and
    /// the authenticating token's expiration time. To get the longest allowed
    /// lifetime, omit the TTL and authenticate with a web console session.
    pub ttl_seconds: Option<NonZeroU32>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAuthVerify {
    pub user_code: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAccessTokenRequest {
    pub grant_type: String,
    pub device_code: String,
    pub client_id: Uuid,
}
