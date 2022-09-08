// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! DB related errors

use serde::{Deserialize, Serialize};

#[derive(thiserror::Error, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Error {
    #[error("Failed to open db connection to {path}: {err}")]
    DbOpen { path: String, err: String },

    #[error("Diesel/sqlite error: {err}")]
    Diesel { err: String },

    #[error("BCS serialization error: {err}")]
    Bcs { err: String },

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

    #[error(
        "Tried to Prepare a KeyShare with epoch {epoch}, but found one 
    with later epoch {stored_epoch}"
    )]
    OldKeySharePrepare { epoch: i32, stored_epoch: i32 },

    #[error("A distinct key share already exists for epoch {epoch}")]
    KeyShareAlreadyExists { epoch: i32 },

    #[error("Rack UUID mismatch: Expected: {expected}, Actual: {actual:?}")]
    RackUuidMismatch { expected: String, actual: Option<String> },

    #[error("Rack not initialized")]
    RackNotInitialized,

    #[error("Key share for epoch {epoch} not committed")]
    KeyShareNotCommitted { epoch: i32 },
}

impl From<diesel::result::Error> for Error {
    fn from(err: diesel::result::Error) -> Self {
        Error::Diesel { err: err.to_string() }
    }
}

impl From<bcs::Error> for Error {
    fn from(err: bcs::Error) -> Self {
        Error::Bcs { err: err.to_string() }
    }
}
