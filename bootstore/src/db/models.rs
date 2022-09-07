// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! DB models

use diesel::deserialize::FromSql;
use diesel::prelude::*;
use diesel::serialize::ToSql;
use diesel::FromSqlRow;
use uuid::Uuid;

use super::macros::array_new_type;
use super::macros::bcs_new_type;
use super::schema::*;
use crate::trust_quorum::SerializableShareDistribution;

bcs_new_type!(Share, SerializableShareDistribution);

/// When a [`KeyShareParepare`] message arrives it is stored in a [`KeyShare`]
/// When a [`KeyShareCommit`] message arrives the `committed` field/column is
/// set to true.
#[derive(Debug, Queryable, Insertable)]
pub struct KeyShare {
    pub epoch: i32,
    pub share: Share,
    pub share_digest: Sha3_256Digest,
    pub committed: bool,
}

/// Information about the rack
#[derive(Debug, Queryable, Insertable)]
#[diesel(table_name = rack)]
pub struct Rack {
    pub uuid: String,
}

/// A chacha20poly1305 secret encrypted by a chacha20poly1305 secret key
/// derived from the rack secret for the given epoch with the given salt
///
/// The epoch informs which rack secret should be used to derive the
/// encryptiong key used to encrypt this root secret.
///
/// TODO-security: We probably don't want to log even the encrypted secret, but
/// it's likely useful for debugging right now.
#[derive(Debug, Queryable, Insertable)]
pub struct EncryptedRootSecret {
    /// The epoch of the rack secret rotation or rack reconfiguration
    pub epoch: i32,

    /// Used as the salt parameter to HKDF to derive the encryption
    /// key from the rack secret that protects `key` in this struct.
    pub salt: Salt,

    /// The encrypted root secret for this epoch
    pub secret: EncryptedSecret,

    /// The authentication tag for the encrypted secret
    pub tag: AuthTag,
}

// TODO: These should likely go in a crypto module
// The length of a SHA3-256 digest
pub const DIGEST_LEN: usize = 32;

// The length of a ChaCha20Poly1305 Key
pub const KEY_LEN: usize = 32;

// The length of a ChaCha20Poly1305 authentication tag
pub const TAG_LEN: usize = 16;

array_new_type!(Sha3_256Digest, DIGEST_LEN);
array_new_type!(EncryptedSecret, KEY_LEN);
array_new_type!(Salt, DIGEST_LEN);
array_new_type!(AuthTag, TAG_LEN);

impl From<sprockets_common::Sha3_256Digest> for Sha3_256Digest {
    fn from(digest: sprockets_common::Sha3_256Digest) -> Self {
        Sha3_256Digest(digest.0)
    }
}
