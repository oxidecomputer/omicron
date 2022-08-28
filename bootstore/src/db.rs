// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database layer for the bootstore

use super::db_macros::array_new_type;
use super::db_macros::json_new_type;

use diesel::deserialize::FromSql;
use diesel::prelude::*;
use diesel::serialize::ToSql;
use diesel::FromSqlRow;
use diesel::SqliteConnection;
use slog::Logger;
use slog::{info, o};

use crate::trust_quorum::SerializableShareDistribution;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to open db connection to {path}: {err}")]
    DbOpen { path: String, err: ConnectionError },

    #[error(transparent)]
    Db(#[from] diesel::result::Error),
}

/// The ID of the database used to store blobs.
///
/// We separate them because they are encrypted and accessed differently.
pub enum DbId {
    /// Used pre-rack unlock: Contains key shares and membership data
    TrustQuorum,

    /// Used post-rack unlock: Contains information necessary for setting
    /// up NTP.
    NetworkConfig,
}

pub struct Db {
    log: Logger,
    conn: SqliteConnection,
}

impl Db {
    pub fn open(log: Logger, path: &str) -> Result<Db, Error> {
        let log = log.new(o!(
            "component" => "BootstoreDb"
        ));
        info!(log, "opening database {:?}", path);
        let mut c = SqliteConnection::establish(path)
            .map_err(|err| Error::DbOpen { path: path.into(), err })?;

        // Enable foreign key processing, which is off by default. Without
        // enabling this, there is no referential integrity check between
        // primary and foreign keys in tables.
        diesel::sql_query("PRAGMA foreign_keys = 'ON'").execute(&mut c)?;

        // Enable the WAL.
        diesel::sql_query("PRAGMA journal_mode = 'WAL'").execute(&mut c)?;

        // Force overwriting with 0s on delete
        diesel::sql_query("PRAGMA secure_delete = 'ON'").execute(&mut c)?;

        // Sync to disk after every commit.
        // DO NOT CHANGE THIS SETTING!
        diesel::sql_query("PRAGMA synchronous = 'FULL'").execute(&mut c)?;

        Ok(Db { log, conn: c })
    }
}

json_new_type!(Share, SerializableShareDistribution);

#[derive(Queryable)]
pub struct KeySharePrepare {
    pub epoch: i32,
    pub share: Share,
}

#[derive(Queryable)]
pub struct KeyShareCommit {
    pub epoch: i32,
}

// TODO: These should go in a crypto module
// The length of a SHA3-256 digest
pub const DIGEST_LEN: usize = 32;

// The length of a ChaCha20Poly1305 Key
pub const KEY_LEN: usize = 32;

// The length of a ChaCha20Poly1305 authentication tag
pub const TAG_LEN: usize = 16;

// A chacha20poly1305 secret encrypted by a chacha20poly1305 secret key
// derived from the rack secret for the given epoch with the given salt
//
// The epoch informs which rack secret should be used to derive the
// encryptiong key used to encrypt this root secret.
#[derive(Queryable)]
pub struct EncryptedRootSecret {
    /// The epoch of the rack secret rotation or rack reconfiguration
    pub epoch: i32,

    /// Used as the salt parameter to HKDF to derive the encryption
    /// key from the rack secret that protects `key` in this struct.
    pub salt: Salt,

    /// The encrypted key
    pub key: EncryptedKey,

    /// The authentication tag for the encrypted key
    pub tag: AuthTag,
}

array_new_type!(EncryptedKey, KEY_LEN);
array_new_type!(Salt, DIGEST_LEN);
array_new_type!(AuthTag, TAG_LEN);
