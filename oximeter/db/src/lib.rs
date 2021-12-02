// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for interacting with the control plane telemetry database.

// Copyright 2021 Oxide Computer Company

use oximeter::{Field, FieldType, Sample};
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

    #[error("The field comparison operation '{op}' is not valid for field '{field_name}' with type {field_type}")]
    InvalidSelectionOp { op: String, field_name: String, field_type: FieldType },

    #[error("Timeseries '{timeseries_name}' does not contain a field with name '{field_name}'")]
    NoSuchField { timeseries_name: String, field_name: String },

    #[error("Field '{field_name}' requires a value of type {expected_type}, found {found_type}")]
    IncorrectFieldType {
        field_name: String,
        expected_type: FieldType,
        found_type: FieldType,
    },

    #[error("Unrecognized or misformatted field comparison")]
    UnknownFieldComparison,

    #[error(
        "Invalid field selector '{selector}', must be of the form `name=value`"
    )]
    InvalidFieldSelectorString { selector: String },

    #[error("Invalid value for field '{field_name}' with type {field_type}: '{value}'")]
    InvalidFieldValue {
        field_name: String,
        field_type: FieldType,
        value: String,
    },

    #[error("The field comparison {op} is not valid for the type {ty}")]
    InvalidFieldCmp { op: String, ty: FieldType },
}

pub(crate) type TimeseriesKey = u64;

pub(crate) fn timeseries_key(sample: &Sample) -> TimeseriesKey {
    timeseries_key_for(&sample.target_fields(), &sample.metric_fields())
}

pub(crate) fn timeseries_key_for(
    target_fields: &[Field],
    metric_fields: &[Field],
) -> TimeseriesKey {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    for field in target_fields.iter().chain(metric_fields.iter()) {
        field.hash(&mut hasher);
    }
    hasher.finish()
}

// Timestamp format in the database
const DATABASE_TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.9f";

// The name of the database storing all metric information.
const DATABASE_NAME: &str = "oximeter";

// The output format used for the result of select queries
//
// See https://clickhouse.com/docs/en/interfaces/formats/#jsoneachrow for details.
const DATABASE_SELECT_FORMAT: &str = "JSONEachRow";
