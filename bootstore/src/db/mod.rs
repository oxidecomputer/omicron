// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database layer for the bootstore

mod macros;
mod models;
mod schema;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::SqliteConnection;
use slog::Logger;
use slog::{info, o};

use crate::trust_quorum::SerializableShareDistribution;
use models::KeyShare;
use models::Rack;
use models::Sha3_256Digest;
use models::Share;
use sha3::{Digest, Sha3_256};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to open db connection to {path}: {err}")]
    DbOpen { path: String, err: ConnectionError },

    #[error(transparent)]
    Db(#[from] diesel::result::Error),

    #[error(transparent)]
    Bcs(#[from] bcs::Error),

    // Temporary until the using code is written
    #[allow(dead_code)]
    #[error("Share commit for {epoch} does not match prepare")]
    CommitHashMismatch { epoch: i32 },

    #[error("No shares have been committed")]
    NoSharesCommitted,

    #[error("DB invariant violated: {0}")]
    DbInvariant(String),

    #[error("Already initialized with rack uuid: {0}")]
    AlreadyInitialized(String),
}

pub struct Db {
    log: Logger,

    // We use a String, because sqlite requires paths to be UTF-8,
    // and strings guarantee that constraint.
    path: String,
}

// Temporary until the using code is written
#[allow(dead_code)]
impl Db {
    /// Initialize the database
    ///
    /// Create tables if they don't exist and set pragmas
    pub fn init(
        log: &Logger,
        path: &str,
    ) -> Result<(Db, SqliteConnection), Error> {
        let schema = include_str!("./schema.sql");
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

        // Create tables
        c.batch_execute(&schema)?;

        Ok((Db { log, path: path.to_string() }, c))
    }

    pub fn new_connection(&self) -> Result<SqliteConnection, Error> {
        info!(
            self.log,
            "Creating a new connection to database {:?}", self.path
        );
        SqliteConnection::establish(&self.path)
            .map_err(|err| Error::DbOpen { path: self.path.clone(), err })
    }

    pub fn prepare_share(
        &self,
        tx: &mut SqliteConnection,
        epoch: i32,
        share: SerializableShareDistribution,
    ) -> Result<(), Error> {
        info!(self.log, "Writing key share prepare for {epoch} to the Db");
        use schema::key_shares::dsl;
        // We save the digest so we don't have to deserialize and recompute most of the time.
        // We'd only want to do that for a consistency check occasionally.
        let val = bcs::to_bytes(&share)?;
        let share_digest =
            sprockets_common::Sha3_256Digest(Sha3_256::digest(&val).into())
                .into();
        let prepare = KeyShare {
            epoch,
            share: Share(share),
            share_digest,
            committed: false,
        };
        diesel::insert_into(dsl::key_shares).values(&prepare).execute(tx)?;
        Ok(())
    }

    pub fn commit_share(
        &self,
        conn: &mut SqliteConnection,
        epoch: i32,
        digest: sprockets_common::Sha3_256Digest,
    ) -> Result<(), Error> {
        use schema::key_shares::dsl;
        conn.immediate_transaction(|tx| {
            // We only want to commit if the share digest of the commit is the
            // same as that of the prepare.
            let prepare_digest = dsl::key_shares
                .select(dsl::share_digest)
                .filter(dsl::epoch.eq(epoch))
                .get_result::<Sha3_256Digest>(tx)?;

            if prepare_digest != digest.into() {
                return Err(Error::CommitHashMismatch { epoch });
            }

            diesel::update(dsl::key_shares.filter(dsl::epoch.eq(epoch)))
                .set(dsl::committed.eq(true))
                .execute(tx)?;
            Ok(())
        })
    }

    pub fn initialize(&self, conn: &mut SqliteConnection) -> Result<(), Error> {
        conn.immediate_transaction(|tx| {
            if self.is_initialized(tx)? {
                let uuid = self.get_rack_uuid(tx)?;
                return Err(Error::AlreadyInitialized(uuid));
            }

            Ok(())
        })
    }

    pub fn get_latest_committed_share(
        &self,
        conn: &mut SqliteConnection,
    ) -> Result<(i32, Share), Error> {
        use schema::key_shares::dsl;
        let res = dsl::key_shares
            .select((dsl::epoch, dsl::share))
            .order(dsl::epoch.desc())
            .filter(dsl::committed.eq(true))
            .get_result::<(i32, Share)>(conn)?;
        Ok(res)
    }

    pub fn get_committed_share(
        &self,
        conn: &mut SqliteConnection,
        epoch: i32,
    ) -> Result<Share, Error> {
        use schema::key_shares::dsl;
        let share = dsl::key_shares
            .select(dsl::share)
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::committed.eq(true))
            .get_result::<Share>(conn)?;

        Ok(share)
    }

    /// Return true if there is a commit for epoch 0, false otherwise
    pub fn is_initialized(
        &self,
        tx: &mut SqliteConnection,
    ) -> Result<bool, Error> {
        use schema::key_shares::dsl;
        Ok(dsl::key_shares
            .select(dsl::epoch)
            .filter(dsl::epoch.eq(0))
            .filter(dsl::committed.eq(true))
            .get_result::<i32>(tx)
            .optional()?
            .is_some())
    }

    /// Return true if the persisted rack uuid matches `uuid`, false otherwise
    pub fn has_valid_rack_uuid(
        &self,
        uuid: &str,
        tx: &mut SqliteConnection,
    ) -> Result<bool, Error> {
        use schema::rack::dsl;

        Ok(dsl::rack
            .filter(dsl::uuid.eq(uuid))
            .get_result::<Rack>(tx)
            .optional()?
            .is_some())
    }

    pub fn get_rack_uuid(
        &self,
        tx: &mut SqliteConnection,
    ) -> Result<String, Error> {
        use schema::rack::dsl;

        Ok(dsl::rack.select(dsl::uuid).get_result::<String>(tx)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trust_quorum::{RackSecret, ShareDistribution};
    use omicron_test_utils::dev::test_setup_log;

    // TODO: Fill in with actual member certs
    fn new_shares() -> Vec<ShareDistribution> {
        let member_device_id_certs = vec![];
        let rack_secret_threshold = 3;
        let total_shares = 5;
        let secret = RackSecret::new();
        let (shares, verifier) =
            secret.split(rack_secret_threshold, total_shares).unwrap();

        shares
            .into_iter()
            .map(move |share| ShareDistribution {
                threshold: rack_secret_threshold,
                verifier: verifier.clone(),
                share,
                member_device_id_certs: member_device_id_certs.clone(),
            })
            .collect()
    }

    #[test]
    fn simple_prepare_insert_and_query() {
        use schema::key_shares::dsl;
        let logctx = test_setup_log("test_db");
        let (db, mut conn) = Db::init(&logctx.log, ":memory:").unwrap();
        let shares = new_shares();
        let epoch = 0;
        let expected: SerializableShareDistribution = shares[0].clone().into();
        db.prepare_share(&mut conn, epoch, expected.clone()).unwrap();
        let (share, committed) = dsl::key_shares
            .select((dsl::share, dsl::committed))
            .filter(dsl::epoch.eq(epoch))
            .get_result::<(Share, bool)>(&mut conn)
            .unwrap();
        assert_eq!(share.0, expected);
        assert_eq!(committed, false);
        logctx.cleanup_successful();
    }

    #[test]
    fn commit_fails_without_corresponding_prepare() {
        let logctx = test_setup_log("test_db");
        let (db, mut conn) = Db::init(&logctx.log, ":memory:").unwrap();
        let epoch = 0;
        let digest = sprockets_common::Sha3_256Digest::default();
        let err = db.commit_share(&mut conn, epoch, digest).unwrap_err();
        assert!(matches!(err, Error::Db(diesel::result::Error::NotFound)));
        logctx.cleanup_successful();
    }

    #[test]
    fn commit_fails_with_invalid_hash() {
        let logctx = test_setup_log("test_db");
        let (db, mut conn) = Db::init(&logctx.log, ":memory:").unwrap();
        let shares = new_shares();
        let epoch = 0;
        let expected: SerializableShareDistribution = shares[0].clone().into();
        db.prepare_share(&mut conn, epoch, expected.clone()).unwrap();
        let digest = sprockets_common::Sha3_256Digest::default();
        let err = db.commit_share(&mut conn, epoch, digest).unwrap_err();
        assert!(matches!(err, Error::CommitHashMismatch { epoch: _ }));
        logctx.cleanup_successful();
    }

    #[test]
    fn commit_succeeds_with_correct_hash() {
        let logctx = test_setup_log("test_db");
        let (db, mut conn) = Db::init(&logctx.log, ":memory:").unwrap();
        let shares = new_shares();
        let epoch = 0;
        let expected: SerializableShareDistribution = shares[0].clone().into();
        db.prepare_share(&mut conn, epoch, expected.clone()).unwrap();

        let val = bcs::to_bytes(&expected).unwrap();
        let digest =
            sprockets_common::Sha3_256Digest(Sha3_256::digest(&val).into())
                .into();
        assert!(db.commit_share(&mut conn, epoch, digest).is_ok());

        // Ensure `committed = true`
        use schema::key_shares::dsl;
        let committed = dsl::key_shares
            .select(dsl::committed)
            .filter(dsl::epoch.eq(epoch))
            .get_result::<bool>(&mut conn)
            .unwrap();
        assert_eq!(true, committed);
        logctx.cleanup_successful();
    }
}
