// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for interacting with the control plane telemetry database.

// Copyright 2021 Oxide Computer Company

use oximeter::FieldType;
use std::collections::BTreeMap;
use thiserror::Error;

mod client;
pub mod model;
pub mod query;
pub use client::{Client, DbWrite};

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Oximeter core error: {0}")]
    Oximeter(#[from] oximeter::Error),

    /// The telemetry database could not be reached.
    #[error("Telemetry database unavailable: {0}")]
    DatabaseUnavailable(String),

    /// An error interacting with the telemetry database
    #[error("Error interacting with telemetry database: {0}")]
    Database(String),

    /// A schema provided when collecting samples did not match the expected schema
    #[error("Schema mismatch for timeseries '{name}', expected fields {expected:?} found fields {actual:?}")]
    SchemaMismatch {
        name: String,
        expected: BTreeMap<String, FieldType>,
        actual: BTreeMap<String, FieldType>,
    },

    /// An error querying or filtering data
    #[error("Invalid query or data filter: {0}")]
    QueryError(String),
}
