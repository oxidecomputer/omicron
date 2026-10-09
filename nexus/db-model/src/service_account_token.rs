// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use nexus_db_schema::schema::service_account_token;
use nexus_types::external_api::service_account_token as api;
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Insertable, Queryable, Selectable)]
#[diesel(table_name = service_account_token)]
pub struct ServiceAccountToken {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_last_used: DateTime<Utc>,
    pub service_account_id: Uuid,
    pub token: String,
    pub idp_id: Option<Uuid>,
    pub federation_jwt_claims: Option<Value>,
    pub federation_generation: Option<i64>,
    pub time_expires: Option<DateTime<Utc>>,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl ServiceAccountToken {
    pub fn new(
        service_account_id: Uuid,
        time_expires: Option<DateTime<Utc>>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            time_created: now,
            time_last_used: now,
            service_account_id,
            token: crate::device_auth::generate_token(),
            idp_id: None,
            federation_jwt_claims: None,
            federation_generation: None,
            time_expires,
            time_deleted: None,
        }
    }
}

impl From<ServiceAccountToken> for api::ServiceAccountTokenGrant {
    fn from(token: ServiceAccountToken) -> Self {
        Self {
            id: token.id,
            token: format!("oxide-service-account-{}", token.token),
            time_expires: token.time_expires,
        }
    }
}

impl From<ServiceAccountToken> for api::ServiceAccountToken {
    fn from(token: ServiceAccountToken) -> Self {
        Self {
            id: token.id,
            time_created: token.time_created,
            time_last_used: token.time_last_used,
            time_expires: token.time_expires,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn service_account_token_view_omits_credentials() {
        let now = Utc::now();
        let token = ServiceAccountToken {
            id: Uuid::new_v4(),
            time_created: now,
            time_last_used: now,
            service_account_id: Uuid::new_v4(),
            token: "secret-token".to_owned(),
            idp_id: Some(Uuid::new_v4()),
            federation_jwt_claims: Some(json!({"sub": "external-identity"})),
            federation_generation: Some(1),
            time_expires: Some(now + chrono::Duration::minutes(5)),
            time_deleted: None,
        };
        let serialized =
            serde_json::to_value(api::ServiceAccountToken::from(token.clone()))
                .unwrap();
        assert_eq!(
            serialized,
            json!({
                "id": token.id,
                "time_created": now,
                "time_last_used": now,
                "time_expires": token.time_expires,
            })
        );
    }
}
