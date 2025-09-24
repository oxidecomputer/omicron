// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use nexus_db_schema::schema::scim_client_bearer_token;
use nexus_types::external_api::views;
use uuid::Uuid;

/// A SCIM client sends requests to a SCIM provider (in this case, Nexus) using
/// some sort of authentication. Nexus currently only supports Bearer token auth
/// from SCIM clients, and these tokens are stored here.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = scim_client_bearer_token)]
pub struct ScimClientBearerToken {
    pub id: Uuid,

    pub time_created: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub time_expires: Option<DateTime<Utc>>,

    pub silo_id: Uuid,

    pub bearer_token: String,
}

impl From<ScimClientBearerToken> for views::ScimClientBearerToken {
    fn from(t: ScimClientBearerToken) -> views::ScimClientBearerToken {
        views::ScimClientBearerToken {
            id: t.id,
            time_created: t.time_created,
            time_expires: t.time_expires,
        }
    }
}

impl From<ScimClientBearerToken> for views::ScimClientBearerTokenValue {
    fn from(t: ScimClientBearerToken) -> views::ScimClientBearerTokenValue {
        views::ScimClientBearerTokenValue {
            id: t.id,
            time_created: t.time_created,
            time_expires: t.time_expires,
            bearer_token: t.bearer_token,
        }
    }
}
