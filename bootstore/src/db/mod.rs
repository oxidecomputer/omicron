// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database layer for the bootstore
mod macros;
mod models;
mod schema;

use diesel::prelude::*;
use diesel::SqliteConnection;
use slog::Logger;
use slog::{info, o};

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
        diesel::sql_query(schema).execute(&mut c)?;

        Ok(Db { log, conn: c })
    }
}
