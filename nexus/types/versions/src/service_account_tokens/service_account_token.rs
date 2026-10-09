// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2026_10_08_00::service_account::{
    ServiceAccountPath, ServiceAccountScope,
};
use chrono::{DateTime, Utc};
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::SimpleIdentity;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ServiceAccountTokenPath {
    /// Scope of the service account.
    pub scope: ServiceAccountScope,
    /// Name or UUID of the service account.
    pub service_account: NameOrId,
    /// UUID of the token.
    pub token_id: Uuid,
}

impl ServiceAccountTokenPath {
    pub fn account_path(&self) -> ServiceAccountPath {
        ServiceAccountPath {
            scope: self.scope,
            service_account: self.service_account.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ServiceAccountToken {
    /// UUID identifying this credential, distinct from the bearer token.
    pub id: Uuid,
    /// Time the credential was created.
    pub time_created: DateTime<Utc>,
    /// Time the credential was last used.
    pub time_last_used: DateTime<Utc>,
    /// Expiration timestamp. Null means the token does not automatically expire.
    pub time_expires: Option<DateTime<Utc>>,
}

impl SimpleIdentity for ServiceAccountToken {
    fn id(&self) -> Uuid {
        self.id
    }
}
