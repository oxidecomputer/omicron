// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! SCIM client types for version INITIAL.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// SCIM

/// The POST response is the only time the generated bearer token is returned to
/// the client.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct ScimClientBearerTokenValue {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_expires: Option<DateTime<Utc>>,
    pub bearer_token: String,
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub struct ScimClientBearerToken {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_expires: Option<DateTime<Utc>>,
}

// SCIM PARAMS

#[derive(Deserialize, JsonSchema)]
pub struct ScimV2TokenPathParam {
    pub token_id: Uuid,
}

#[derive(Deserialize, JsonSchema)]
pub struct ScimV2UserPathParam {
    pub user_id: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct ScimV2GroupPathParam {
    pub group_id: String,
}
