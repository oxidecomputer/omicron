// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Duration, Utc};
use nexus_db_schema::schema::federation_session;
use rand::{RngCore, SeedableRng, rngs::StdRng};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Insertable, Queryable, Selectable)]
#[diesel(table_name = federation_session)]
pub struct FederationSession {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_last_used: DateTime<Utc>,
    pub time_expires: DateTime<Utc>,
    pub trust_policy_id: Uuid,
    pub trust_policy_revision: i64,
    pub jwt_claims: Value,
    pub audit_log_id: Uuid,
    pub token: String,
}

impl FederationSession {
    pub fn new(
        trust_policy_id: Uuid,
        trust_policy_revision: i64,
        jwt_claims: Value,
        audit_log_id: Uuid,
    ) -> Self {
        let now = Utc::now();
        let mut bytes = [0; 20];
        StdRng::from_os_rng().fill_bytes(&mut bytes);
        Self {
            id: Uuid::new_v4(),
            time_created: now,
            time_last_used: now,
            time_expires: now + Duration::minutes(5),
            trust_policy_id,
            trust_policy_revision,
            jwt_claims,
            audit_log_id,
            token: hex::encode(bytes),
        }
    }
}
