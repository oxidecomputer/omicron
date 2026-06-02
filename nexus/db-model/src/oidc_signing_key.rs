// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! POC: a single live OIDC signing key used to mint instance-identity tokens.

use chrono::DateTime;
use chrono::Utc;
use nexus_db_schema::schema::oidc_signing_key;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// An OIDC signing key (RSA, PEM-encoded private + public) stored in the
/// database. The mint path reads the single live key from here per request.
#[derive(Clone, Queryable, Insertable, Selectable, Serialize, Deserialize)]
#[diesel(table_name = oidc_signing_key)]
pub struct OidcSigningKey {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    /// Key id advertised in the JWT header.
    pub kid: String,
    /// Signature algorithm, e.g. `RS256`.
    pub algorithm: String,
    /// Public key, PEM-encoded.
    pub public_key: Vec<u8>,
    /// Private key, PEM-encoded.
    pub private_key: Vec<u8>,
    /// OIDC issuer (`iss` claim) minted tokens carry.
    pub issuer: String,
    /// Audience (`aud` claim) minted tokens carry.
    pub audience: String,
    /// Token lifetime in seconds (drives the `exp` claim).
    pub token_ttl_secs: i64,
}

impl OidcSigningKey {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Uuid,
        kid: String,
        algorithm: String,
        public_key: Vec<u8>,
        private_key: Vec<u8>,
        issuer: String,
        audience: String,
        token_ttl_secs: i64,
    ) -> Self {
        let now = Utc::now();
        Self {
            id,
            time_created: now,
            time_modified: now,
            time_deleted: None,
            kid,
            algorithm,
            public_key,
            private_key,
            issuer,
            audience,
            token_ttl_secs,
        }
    }
}

impl std::fmt::Debug for OidcSigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcSigningKey")
            .field("id", &self.id)
            .field("time_created", &self.time_created)
            .field("time_modified", &self.time_modified)
            .field("time_deleted", &self.time_deleted)
            .field("kid", &self.kid)
            .field("algorithm", &self.algorithm)
            .field("public_key", &self.public_key)
            .field("private_key", &"<redacted>")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("token_ttl_secs", &self.token_ttl_secs)
            .finish()
    }
}
