// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for interacting with the control plane telemetry database.

// Copyright 2023 Oxide Computer Company

use crate::query::StringFieldSelector;
use chrono::DateTime;
use chrono::Utc;
use dropshot::EmptyScanParams;
use dropshot::PaginationParams;
pub use oximeter::DatumType;
pub use oximeter::Field;
pub use oximeter::FieldType;
pub use oximeter::Measurement;
pub use oximeter::Sample;
pub use oximeter::TimeseriesName;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use thiserror::Error;

mod client;
pub mod model;
pub mod query;
pub use client::Client;
pub use client::DbWrite;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Oximeter core error: {0}")]
    Oximeter(#[from] oximeter::MetricsError),

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

    #[error("Timeseries not found for: {0}")]
    TimeseriesNotFound(String),

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
        "Invalid field selector '{selector}', must be of the form `name==value`"
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

    #[error("Query must resolve to a single timeseries if limit is specified")]
    InvalidLimitQuery,
}

/// The schema for a timeseries.
///
/// This includes the name of the timeseries, as well as the datum type of its metric and the
/// schema for each field.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct TimeseriesSchema {
    pub timeseries_name: TimeseriesName,
    pub field_schema: Vec<FieldSchema>,
    pub datum_type: DatumType,
    pub created: DateTime<Utc>,
}

impl TimeseriesSchema {
    /// Return the schema for the given field.
    pub fn field_schema<S>(&self, name: S) -> Option<&FieldSchema>
    where
        S: AsRef<str>,
    {
        self.field_schema.iter().find(|field| field.name == name.as_ref())
    }

    /// Return the target and metric component names for this timeseries
    pub fn component_names(&self) -> (&str, &str) {
        self.timeseries_name.component_names()
    }
}

impl PartialEq for TimeseriesSchema {
    fn eq(&self, other: &TimeseriesSchema) -> bool {
        self.timeseries_name == other.timeseries_name
            && self.datum_type == other.datum_type
            && self.field_schema == other.field_schema
    }
}

impl From<model::DbTimeseriesSchema> for TimeseriesSchema {
    fn from(schema: model::DbTimeseriesSchema) -> TimeseriesSchema {
        TimeseriesSchema {
            timeseries_name: TimeseriesName::from_name_and_versions(
                &schema.timeseries_name,
                schema.target_version.try_into().expect("Expected non-zero u8"),
                schema.metric_version.try_into().expect("Expected non-zero u8"),
            )
            .expect("Invalid timeseries name"),
            field_schema: schema.field_schema.into(),
            datum_type: schema.datum_type.into(),
            created: schema.created,
        }
    }
}

/// The target identifies the resource or component about which metric data is produced.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Target {
    pub name: String,
    pub fields: Vec<Field>,
}

/// The metric identifies the measured aspect or feature of a target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct Metric {
    pub name: String,
    pub fields: Vec<Field>,
    pub datum_type: DatumType,
}

/// A list of timestamped measurements from a single timeseries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Timeseries {
    pub timeseries_name: TimeseriesName,
    pub target: Target,
    pub metric: Metric,
    pub measurements: Vec<Measurement>,
}

/// The source from which a field is derived, the target or metric.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FieldSource {
    Target,
    Metric,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize,
)]
pub enum DbFieldSource {
    Target,
    Metric,
}

impl From<DbFieldSource> for FieldSource {
    fn from(src: DbFieldSource) -> Self {
        match src {
            DbFieldSource::Target => FieldSource::Target,
            DbFieldSource::Metric => FieldSource::Metric,
        }
    }
}
impl From<FieldSource> for DbFieldSource {
    fn from(src: FieldSource) -> Self {
        match src {
            FieldSource::Target => DbFieldSource::Target,
            FieldSource::Metric => DbFieldSource::Metric,
        }
    }
}

/// The name and type information for a field of a timeseries schema.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct FieldSchema {
    pub name: String,
    pub ty: FieldType,
    pub source: FieldSource,
}

/// Type used to paginate request to list timeseries schema.
pub type TimeseriesSchemaPaginationParams =
    PaginationParams<EmptyScanParams, TimeseriesName>;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TimeseriesScanParams {
    pub timeseries_name: TimeseriesName,
    pub criteria: Vec<StringFieldSelector>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TimeseriesPageSelector {
    pub params: TimeseriesScanParams,
    pub offset: NonZeroU32,
}

pub(crate) type TimeseriesKey = u64;

pub(crate) fn timeseries_key(sample: &Sample) -> TimeseriesKey {
    timeseries_key_for(sample.target_fields(), sample.metric_fields())
}

pub(crate) fn timeseries_key_for<'a>(
    target_fields: impl Iterator<Item = &'a Field>,
    metric_fields: impl Iterator<Item = &'a Field>,
) -> TimeseriesKey {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    for field in target_fields.chain(metric_fields) {
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

// TODO-remove: This should be removed when the database schema accepts versions
// as well as the timeseries name.
pub(crate) fn unversioned_timeseries_name(name: &TimeseriesName) -> String {
    format!("{}:{}", name.target_name(), name.metric_name())
}
