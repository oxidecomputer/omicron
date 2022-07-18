// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for interacting with the control plane telemetry database.

// Copyright 2021 Oxide Computer Company

use crate::query::StringFieldSelector;
use chrono::{DateTime, Utc};
use dropshot::{EmptyScanParams, PaginationParams};
pub use oximeter::{DatumType, Field, FieldType, Measurement, Sample};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::num::NonZeroU32;
use thiserror::Error;

mod client;
pub mod model;
pub mod query;
pub use client::{Client, DbWrite};

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

    #[error("Invalid timeseries name")]
    InvalidTimeseriesName,
}

/// A timeseries name.
///
/// Timeseries are named by concatenating the names of their target and metric, joined with a
/// colon.
#[derive(
    Debug, Clone, PartialEq, PartialOrd, Ord, Eq, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "&str")]
pub struct TimeseriesName(String);

impl JsonSchema for TimeseriesName {
    fn schema_name() -> String {
        "TimeseriesName".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("The name of a timeseries".to_string()),
                description: Some(
                    "Names are constructed by concatenating the target \
                     and metric names with ':'. Target and metric \
                     names must be lowercase alphanumeric characters \
                     with '_' separating words."
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(schemars::schema::StringValidation {
                pattern: Some(TIMESERIES_NAME_REGEX.to_string()),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

impl std::ops::Deref for TimeseriesName {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for TimeseriesName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::convert::TryFrom<&str> for TimeseriesName {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        validate_timeseries_name(s).map(|s| TimeseriesName(s.to_string()))
    }
}

impl std::convert::TryFrom<String> for TimeseriesName {
    type Error = Error;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        validate_timeseries_name(&s)?;
        Ok(TimeseriesName(s))
    }
}

impl<T> PartialEq<T> for TimeseriesName
where
    T: AsRef<str>,
{
    fn eq(&self, other: &T) -> bool {
        self.0.eq(other.as_ref())
    }
}

fn validate_timeseries_name(s: &str) -> Result<&str, Error> {
    if regex::Regex::new(TIMESERIES_NAME_REGEX).unwrap().is_match(s) {
        Ok(s)
    } else {
        Err(Error::InvalidTimeseriesName)
    }
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
        self.timeseries_name
            .split_once(':')
            .expect("Incorrectly formatted timseries name")
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
            timeseries_name: TimeseriesName::try_from(
                schema.timeseries_name.as_str(),
            )
            .expect("Invalid timeseries name in database"),
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
    pub timeseries_name: String,
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

// Regular expression describing valid timeseries names.
//
// Names are derived from the names of the Rust structs for the target and metric, converted to
// snake case. So the names must be valid identifiers, and generally:
//
//  - Start with lowercase a-z
//  - Any number of alphanumerics
//  - Zero or more of the above, delimited by '-'.
//
// That describes the target/metric name, and the timeseries is two of those, joined with ':'.
const TIMESERIES_NAME_REGEX: &str =
    "(([a-z]+[a-z0-9]*)(_([a-z0-9]+))*):(([a-z]+[a-z0-9]*)(_([a-z0-9]+))*)";

#[cfg(test)]
mod tests {
    use super::TimeseriesName;
    use std::convert::TryFrom;

    #[test]
    fn test_timeseries_name() {
        let name = TimeseriesName::try_from("foo:bar").unwrap();
        assert_eq!(format!("{}", name), "foo:bar");
    }

    #[test]
    fn test_timeseries_name_from_str() {
        assert!(TimeseriesName::try_from("a:b").is_ok());
        assert!(TimeseriesName::try_from("a_a:b_b").is_ok());
        assert!(TimeseriesName::try_from("a0:b0").is_ok());
        assert!(TimeseriesName::try_from("a_0:b_0").is_ok());

        assert!(TimeseriesName::try_from("_:b").is_err());
        assert!(TimeseriesName::try_from("a_:b").is_err());
        assert!(TimeseriesName::try_from("0:b").is_err());
        assert!(TimeseriesName::try_from(":b").is_err());
        assert!(TimeseriesName::try_from("a:").is_err());
        assert!(TimeseriesName::try_from("123").is_err());
    }
}
