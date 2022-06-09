// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::schema::{client_authentication, client_token};
use chrono::{DateTime, Duration, Utc};
use rand::{distributions::Slice, rngs::StdRng, Rng, RngCore, SeedableRng};
use uuid::Uuid;

/// Default timeout in seconds for client to authenticate for a token request.
const CLIENT_AUTHENTICATION_TIMEOUT: i64 = 300;

/// The initial record of a client authentication request.
/// Does *not* include a token: that is only granted after
/// the `user_code` has been relayed and login has succeeded.
#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = client_authentication)]
pub struct ClientAuthentication {
    pub client_id: Uuid,
    pub device_code: String,
    pub user_code: String,
    pub time_created: DateTime<Utc>,
    pub time_expires: DateTime<Utc>,
}

/// Neither the device code nor the access token is meant to be
/// human-readable, so we use 20 random bytes (160 bits), hex-encoded.
const TOKEN_LENGTH: usize = 20;

/// Generate a random token/device code.
// TODO: this should be merged with session::generate_session_token,
// and probably also the key generation in the disk creation saga.
fn generate_token() -> String {
    let mut bytes: [u8; TOKEN_LENGTH] = [0; TOKEN_LENGTH];
    let mut rng = StdRng::from_entropy();
    rng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// The user code *is* meant to be human-readable, and in particular to be
/// easily human-typable (with a keyboard). We might sample from something
/// like the EFF word list, but for now we'll use the 20-letter alphabet
/// suggested in RFC 8628 §6.1 (User Code Recommendations); q.v. also for
/// a discussion of entropy requirements. On input, use codes should be
/// uppercased, and characters not in this alphabet should be stripped.
// TODO-security: user code tries should be rate-limited
const USER_CODE_ALPHABET: [char; 20] = [
    'B', 'C', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S',
    'T', 'V', 'W', 'X', 'Z',
];
const USER_CODE_LENGTH: usize = 8;
const USER_CODE_WORD_LENGTH: usize = 4;

/// Generate a short random user code like `BQPX-FGQR`.
fn generate_user_code() -> String {
    let rng = StdRng::from_entropy();
    let dist = Slice::new(&USER_CODE_ALPHABET[..]).expect("non-empty slice");
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

impl ClientAuthentication {
    pub fn new(client_id: Uuid) -> Self {
        let now = Utc::now();
        Self {
            client_id,
            device_code: generate_token(),
            user_code: generate_user_code(),
            time_created: now,
            time_expires: now
                + Duration::seconds(CLIENT_AUTHENTICATION_TIMEOUT),
        }
    }
}

/// A token granted after user authentication.
#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = client_token)]
pub struct ClientToken {
    pub token: String,
    pub client_id: Uuid,
    pub device_code: String,
    pub silo_user_id: Uuid,
    pub time_created: DateTime<Utc>,
}

impl ClientToken {
    pub fn new(
        client_id: Uuid,
        device_code: String,
        silo_user_id: Uuid,
    ) -> Self {
        Self {
            token: generate_token(),
            client_id,
            device_code,
            silo_user_id,
            time_created: Utc::now(),
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
            assert!(user_code.chars().filter(|x| *x != '-').all(|x| {
                USER_CODE_ALPHABET.iter().find(|y| **y == x).is_some()
            }));
            assert!(!codes_seen.contains(&user_code));
            codes_seen.insert(user_code);
        }
    }
}
