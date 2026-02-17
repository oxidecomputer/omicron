// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Device and session types for version INITIAL.

use chrono::{DateTime, Utc};
use omicron_common::api::external::SimpleIdentity;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// View of a device access token
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct DeviceAccessToken {
    /// A unique, immutable, system-controlled identifier for the token.
    ///
    /// Note that this ID is not the bearer token itself, which starts with
    /// "oxide-token-".
    pub id: Uuid,
    pub time_created: DateTime<Utc>,

    /// Expiration timestamp. A null value means the token does not automatically expire.
    pub time_expires: Option<DateTime<Utc>>,
}

impl SimpleIdentity for DeviceAccessToken {
    fn id(&self) -> Uuid {
        self.id
    }
}

/// View of a console session
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ConsoleSession {
    /// A unique, immutable, system-controlled identifier for the session
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_last_used: DateTime<Utc>,
}

impl SimpleIdentity for ConsoleSession {
    fn id(&self) -> Uuid {
        self.id
    }
}

// OAUTH 2.0 DEVICE AUTHORIZATION REQUESTS & TOKENS

/// Response to an initial device authorization request.
/// See RFC 8628 §3.2 (Device Authorization Response).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAuthResponse {
    /// The device verification code
    pub device_code: String,

    /// The end-user verification code
    pub user_code: String,

    /// The end-user verification URI on the authorization server.
    /// The URI should be short and easy to remember as end users
    /// may be asked to manually type it into their user agent.
    pub verification_uri: String,

    /// The lifetime in seconds of the `device_code` and `user_code`
    pub expires_in: u16,
}

/// Successful access token grant. See RFC 6749 §5.1.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAccessTokenGrant {
    /// The access token issued to the client
    pub access_token: String,

    /// The type of the token issued, as described in RFC 6749 §7.1.
    pub token_type: DeviceAccessTokenType,

    /// A unique, immutable, system-controlled identifier for the token
    pub token_id: Uuid,

    /// Expiration timestamp. A null value means the token does not automatically expire.
    pub time_expires: Option<DateTime<Utc>>,
}

/// The kind of token granted.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DeviceAccessTokenType {
    Bearer,
}
