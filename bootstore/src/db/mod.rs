// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database layer for the bootstore

mod error;
mod macros;
pub(crate) mod models;
mod schema;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::SqliteConnection;
use slog::Logger;
use slog::{info, o};
use uuid::Uuid;

use crate::trust_quorum::SerializableShareDistribution;
pub(crate) use error::Error;
use models::KeyShare;
use models::Sha3_256Digest;
use models::Share;

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
        let mut c = SqliteConnection::establish(path).map_err(|err| {
            Error::DbOpen { path: path.into(), err: err.to_string() }
        })?;

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
        SqliteConnection::establish(&self.path).map_err(|err| Error::DbOpen {
            path: self.path.clone(),
            err: err.to_string(),
        })
    }

    /// Write an uncommitted `KeyShare` into the database.
    ///
    /// This command is idempotent.
    ///
    /// The rules for inserting a KeyShare are:
    ///   1. A KeyShare for the given epoch does not exist unless it is identical
    ///   2. A KeyShare for a later epoch does not exist
    ///
    /// Calling this method with an epoch of 0 is a programmer error so we
    /// assert.
    pub fn prepare_share(
        &self,
        conn: &mut SqliteConnection,
        rack_uuid: &Uuid,
        epoch: i32,
        share_distribution: SerializableShareDistribution,
    ) -> Result<(), Error> {
        assert_ne!(0, epoch);
        use schema::key_shares::dsl;
        let prepare = KeyShare::new(epoch, share_distribution)?;
        conn.immediate_transaction(|tx| {
            // Has the rack been initialized?
            if !self.is_initialized(tx)? {
                return Err(Error::RackNotInitialized);
            }

            // Does the rack_uuid match what's stored?
            self.validate_rack_uuid(tx, rack_uuid)?;

            // Check for idempotence
            if let Some(stored_key_share) = dsl::key_shares
                .filter(dsl::epoch.eq(epoch))
                .get_result::<KeyShare>(tx)
                .optional()?
            {
                if prepare == stored_key_share {
                    return Ok(());
                } else {
                    return Err(Error::KeyShareAlreadyExists { epoch });
                }
            }

            // We don't allow shares for old epochs
            if let Some(stored_epoch) = dsl::key_shares
                .select(dsl::epoch)
                .filter(dsl::epoch.gt(epoch))
                .get_result::<i32>(tx)
                .optional()?
            {
                return Err(Error::OldKeySharePrepare { epoch, stored_epoch });
            }

            info!(
                self.log,
                "Writing key share prepare for epoch {epoch} to the Db"
            );
            diesel::insert_into(dsl::key_shares)
                .values(&prepare)
                .execute(tx)?;
            Ok(())
        })
    }

    pub fn commit_share(
        &self,
        conn: &mut SqliteConnection,
        rack_uuid: &Uuid,
        epoch: i32,
        digest: sprockets_common::Sha3_256Digest,
    ) -> Result<(), Error> {
        use schema::key_shares::dsl;
        conn.immediate_transaction(|tx| {
            // Does the rack_uuid match what's stored?
            self.validate_rack_uuid(tx, rack_uuid)?;

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

    pub fn initialize(
        &self,
        conn: &mut SqliteConnection,
        rack_uuid: &Uuid,
        share_distribution: SerializableShareDistribution,
    ) -> Result<(), Error> {
        conn.immediate_transaction(|tx| {
            if self.is_initialized(tx)? {
                // If the rack is initialized, a rack uuid must exist
                let uuid = self.get_rack_uuid(tx)?.unwrap();
                return Err(Error::AlreadyInitialized(uuid));
            }
            let new_uuid = rack_uuid.to_string();
            match self.initialize_rack_uuid(tx, &new_uuid)? {
                Some(old_uuid) => {
                        info!(self.log, "Re-Initializing Rack: Old UUID: {old_uuid}, New UUID: {new_uuid}");
                    self.update_prepare_for_epoch_0(tx, share_distribution)

                },
                None => {
                    info!(self.log, "Initializing Rack for the first time with UUID: {new_uuid}");
                    self.insert_prepare(tx, 0, share_distribution)
                    }
            }

        })
    }

    // Insert an uncommitted key share for a given epoch This should be called
    // inside a transaction by the caller so that the given checks can be
    // performed.
    fn insert_prepare(
        &self,
        tx: &mut SqliteConnection,
        epoch: i32,
        share_distribution: SerializableShareDistribution,
    ) -> Result<(), Error> {
        use schema::key_shares::dsl;
        let prepare = KeyShare::new(epoch, share_distribution)?;
        diesel::insert_into(dsl::key_shares).values(&prepare).execute(tx)?;
        Ok(())
    }

    // During rack initialization we set the rack UUID.
    //
    // We only allow rewriting this UUID in when the rack is not initialized,
    // and so we only call this from the `initialize` method.
    //
    // Since we only allow a single row, we just delete the existing rows
    // and insert the new one.
    //
    // Return the old rack UUID if one exists
    fn initialize_rack_uuid(
        &self,
        tx: &mut SqliteConnection,
        new_uuid: &str,
    ) -> Result<Option<String>, Error> {
        use schema::rack::dsl;
        let old_uuid = self.get_rack_uuid(tx)?;
        diesel::delete(dsl::rack).execute(tx)?;
        diesel::insert_into(dsl::rack)
            .values(dsl::uuid.eq(new_uuid))
            .execute(tx)?;
        Ok(old_uuid)
    }

    // Overwrite a share for epoch 0 during rack initialization
    //
    // This method should only be called from initialize.
    fn update_prepare_for_epoch_0(
        &self,
        tx: &mut SqliteConnection,
        share_distribution: SerializableShareDistribution,
    ) -> Result<(), Error> {
        use schema::key_shares::dsl;
        let prepare = KeyShare::new(0, share_distribution)?;
        diesel::update(dsl::key_shares).set(prepare).execute(tx)?;
        Ok(())
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
            .get_result::<Share>(conn)
            .optional()?;

        match share {
            Some(share) => Ok(share),
            None => Err(Error::KeyShareNotCommitted { epoch }),
        }
    }

    /// Return true if there is a commit for epoch 0, false otherwise
    fn is_initialized(&self, tx: &mut SqliteConnection) -> Result<bool, Error> {
        use schema::key_shares::dsl;
        Ok(dsl::key_shares
            .select(dsl::epoch)
            .filter(dsl::epoch.eq(0))
            .filter(dsl::committed.eq(true))
            .get_result::<i32>(tx)
            .optional()?
            .is_some())
    }

    /// Return `Ok(())` if the persisted rack uuid matches `uuid`, or
    /// `Err(Error::RackUuidMismatch{..})` otherwise.
    fn validate_rack_uuid(
        &self,
        tx: &mut SqliteConnection,
        rack_uuid: &Uuid,
    ) -> Result<(), Error> {
        let rack_uuid = rack_uuid.to_string();
        let stored_rack_uuid = self.get_rack_uuid(tx)?;
        if Some(&rack_uuid) != stored_rack_uuid.as_ref() {
            return Err(Error::RackUuidMismatch {
                expected: rack_uuid,
                actual: stored_rack_uuid,
            });
        }
        Ok(())
    }

    pub fn get_rack_uuid(
        &self,
        tx: &mut SqliteConnection,
    ) -> Result<Option<String>, Error> {
        use schema::rack::dsl;

        Ok(dsl::rack.select(dsl::uuid).get_result::<String>(tx).optional()?)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::trust_quorum::{RackSecret, ShareDistribution};
    use omicron_test_utils::dev::test_setup_log;
    use sha3::{Digest, Sha3_256};

    // TODO: Fill in with actual member certs
    pub fn new_shares() -> Vec<ShareDistribution> {
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
        db.insert_prepare(&mut conn, epoch, expected.clone()).unwrap();
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
        db.commit_share(&mut conn, &Uuid::new_v4(), epoch, digest).unwrap_err();
        logctx.cleanup_successful();
    }

    #[test]
    fn commit_fails_with_invalid_hash() {
        let logctx = test_setup_log("test_db");
        let (db, mut conn) = Db::init(&logctx.log, ":memory:").unwrap();
        let shares = new_shares();
        let epoch = 0;
        let expected: SerializableShareDistribution = shares[0].clone().into();
        let rack_uuid = Uuid::new_v4();
        db.initialize(&mut conn, &rack_uuid, expected.clone()).unwrap();
        let digest = sprockets_common::Sha3_256Digest::default();
        let err =
            db.commit_share(&mut conn, &rack_uuid, epoch, digest).unwrap_err();
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
        let rack_uuid = Uuid::new_v4();
        db.initialize(&mut conn, &rack_uuid, expected.clone()).unwrap();

        let val = bcs::to_bytes(&expected).unwrap();
        let digest =
            sprockets_common::Sha3_256Digest(Sha3_256::digest(&val).into())
                .into();
        assert!(db.commit_share(&mut conn, &rack_uuid, epoch, digest).is_ok());

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
