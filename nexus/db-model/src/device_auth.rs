// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Data structures and token generation routines for the OAuth 2.0
//! Device Authorization Grant flow. See the module-level documentation in
//! [device_auth.rs](nexus/src/app/device_auth.rs) for an overview of how these are
//! used.

use nexus_db_schema::schema::{device_access_token, device_auth_request};

use chrono::{DateTime, Duration, Utc};
use nexus_types::external_api::views;
use omicron_uuid_kinds::{
    AccessTokenKind, GenericUuid, SiloUserKind, SiloUserUuid, TypedUuid,
};
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng};
use std::num::NonZeroU32;
use uuid::Uuid;

use crate::SqlU32;
use crate::typed_uuid::DbTypedUuid;

/// Default timeout in seconds for client to authenticate for a token request.
const CLIENT_AUTHENTICATION_TIMEOUT: i64 = 300;

/// Initial record of an OAuth 2.0 Device Authorization Grant.
/// Does *not* include a token; that is only granted after the
/// `user_code` has been verified and login has succeeded.
/// See RFC 8628 §§3.1-3.2.
#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = device_auth_request)]
pub struct DeviceAuthRequest {
    pub client_id: Uuid,
    pub device_code: String,
    pub user_code: String,
    pub time_created: DateTime<Utc>,
    pub time_expires: DateTime<Utc>,

    /// TTL requested by the user
    pub token_ttl_seconds: Option<SqlU32>,
}

impl DeviceAuthRequest {
    // We need the host to construct absolute verification URIs.
    pub fn into_response(
        self,
        tls: bool,
        host: &str,
    ) -> views::DeviceAuthResponse {
        let scheme = if tls { "https" } else { "http" };
        views::DeviceAuthResponse {
            verification_uri: format!("{scheme}://{host}/device/verify"),
            user_code: self.user_code,
            device_code: self.device_code,
            expires_in: self
                .time_expires
                .signed_duration_since(self.time_created)
                .num_seconds() as u16,
        }
    }
}

/// Neither the device code nor the access token is meant to be
/// human-readable, so we use 20 random bytes (160 bits), hex-encoded.
const TOKEN_LENGTH: usize = 20;

/// Generate a random token/device code.
// TODO: this should be merged with session::generate_session_token,
// and probably also the key generation in the disk creation saga.
fn generate_token() -> String {
    let mut bytes: [u8; TOKEN_LENGTH] = [0; TOKEN_LENGTH];
    let mut rng = StdRng::from_os_rng();
    rng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// The user code *is* meant to be human-readable, and in particular to be
/// easily human-typable (with a keyboard). We might sample from something
/// like the EFF word list, but for now we'll use the 20-letter alphabet
/// suggested in RFC 8628 §6.1 (User Code Recommendations); q.v. also for
/// a discussion of entropy requirements. On input, use codes should be
/// uppercased, and characters not in this alphabet should be stripped.
const USER_CODE_ALPHABET: [char; 20] = [
    'B', 'C', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S',
    'T', 'V', 'W', 'X', 'Z',
];
const USER_CODE_LENGTH: usize = 8;
const USER_CODE_WORD_LENGTH: usize = 4;

/// Generate a short random user code like `BQPX-FGQR`.
fn generate_user_code() -> String {
    let rng = StdRng::from_os_rng();
    let dist = rand::distr::slice::Choose::new(&USER_CODE_ALPHABET[..])
        .expect("non-empty slice");
    let chars: Vec<char> = rng
        .sample_iter(dist)
        .take(USER_CODE_LENGTH)
        .map(char::to_owned)
        .collect();
    chars[..]
        .chunks_exact(USER_CODE_WORD_LENGTH)
        .map(|x| x.iter().collect::<String>())
        .collect::<Vec<String>>()
        .join("-")
}

impl DeviceAuthRequest {
    pub fn new(
        client_id: Uuid,
        requested_ttl_seconds: Option<NonZeroU32>,
    ) -> Self {
        let now = Utc::now();
        Self {
            client_id,
            device_code: generate_token(),
            user_code: generate_user_code(),
            time_created: now,
            time_expires: now
                + Duration::seconds(CLIENT_AUTHENTICATION_TIMEOUT),
            token_ttl_seconds: requested_ttl_seconds
                .map(|ttl| ttl.get().into()),
        }
    }

    pub fn id(&self) -> String {
        self.user_code.clone()
    }
}

/// An access token granted in response to a successful device authorization flow.
// TODO-security: wrap token in an opaque struct to avoid accidental leaks.
#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = device_access_token)]
pub struct DeviceAccessToken {
    pub id: DbTypedUuid<AccessTokenKind>,
    pub token: String,
    pub client_id: Uuid,
    pub device_code: String,
    silo_user_id: DbTypedUuid<SiloUserKind>,
    pub time_requested: DateTime<Utc>,
    pub time_created: DateTime<Utc>,
    pub time_expires: Option<DateTime<Utc>>,
}

impl DeviceAccessToken {
    pub fn new(
        client_id: Uuid,
        device_code: String,
        time_requested: DateTime<Utc>,
        silo_user_id: SiloUserUuid,
        time_expires: Option<DateTime<Utc>>,
    ) -> Self {
        let now = Utc::now();
        assert!(time_requested <= now);
        assert!(time_expires.map_or(true, |t| t > now));

        Self {
            id: TypedUuid::new_v4().into(),
            token: generate_token(),
            client_id,
            device_code,
            silo_user_id: silo_user_id.into(),
            time_requested,
            time_created: now,
            time_expires,
        }
    }

    pub fn id(&self) -> TypedUuid<AccessTokenKind> {
        self.id.0
    }

    pub fn expires(mut self, time: DateTime<Utc>) -> Self {
        self.time_expires = Some(time);
        self
    }

    pub fn silo_user_id(&self) -> SiloUserUuid {
        self.silo_user_id.into()
    }
}

impl From<DeviceAccessToken> for views::DeviceAccessTokenGrant {
    fn from(access_token: DeviceAccessToken) -> Self {
        Self {
            access_token: format!("oxide-token-{}", access_token.token),
            token_type: views::DeviceAccessTokenType::Bearer,
            token_id: access_token.id.into_untyped_uuid(),
            time_expires: access_token.time_expires,
        }
    }
}

impl From<DeviceAccessToken> for views::DeviceAccessToken {
    fn from(access_token: DeviceAccessToken) -> Self {
        Self {
            id: access_token.id.into_untyped_uuid(),
            time_created: access_token.time_created,
            time_expires: access_token.time_expires,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_generate_user_code() {
        let mut codes_seen = HashSet::new();
        for _ in 0..10 {
            let user_code = generate_user_code();
            assert_eq!(user_code.len(), USER_CODE_LENGTH + 1);
            assert!(
                user_code.chars().nth(USER_CODE_WORD_LENGTH).unwrap() == '-'
            );
            assert!(
                user_code
                    .chars()
                    .filter(|x| *x != '-')
                    .all(|x| { USER_CODE_ALPHABET.contains(&x) })
            );
            assert!(!codes_seen.contains(&user_code));
            codes_seen.insert(user_code);
        }
    }
}
