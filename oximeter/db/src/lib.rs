// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for interacting with the control plane telemetry database.

// Copyright 2023 Oxide Computer Company

use crate::query::StringFieldSelector;
use chrono::{DateTime, Utc};
use dropshot::{EmptyScanParams, PaginationParams};
pub use oximeter::{DatumType, Field, FieldType, Measurement, Sample};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::io;
use std::num::NonZeroU32;
use std::path::PathBuf;
use thiserror::Error;

mod client;
pub mod model;
pub mod query;
pub use client::{Client, DbWrite};

pub use model::OXIMETER_VERSION;

#[derive(Debug, Error)]
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
    #[error("Schema mismatch for timeseries '{0}'", expected.timeseries_name)]
    SchemaMismatch { expected: TimeseriesSchema, actual: TimeseriesSchema },

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

    #[error("Invalid timeseries name")]
    InvalidTimeseriesName,

    #[error("Query must resolve to a single timeseries if limit is specified")]
    InvalidLimitQuery,

    #[error("Database is not at expected version")]
    DatabaseVersionMismatch { expected: u64, found: u64 },

    #[error("Could not read schema directory")]
    ReadSchemaDir {
        context: String,
        #[source]
        err: io::Error,
    },

    #[error("Could not read SQL file from path")]
    ReadSqlFile {
        context: String,
        #[source]
        err: io::Error,
    },

    #[error("Non-UTF8 schema directory entry")]
    NonUtf8SchemaDirEntry(std::ffi::OsString),

    #[error("Missing desired schema version: {0}")]
    MissingSchemaVersion(u64),

    #[error("Data-modifying operations are not supported in schema updates")]
    SchemaUpdateModifiesData { path: PathBuf, statement: String },

    #[error("Schema update SQL files should contain at most 1 statement")]
    MultipleSqlStatementsInSchemaUpdate { path: PathBuf },

    #[error("Schema update versions must be sequential without gaps")]
    NonSequentialSchemaVersions,
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

impl std::str::FromStr for TimeseriesName {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.try_into()
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
    pub field_schema: BTreeSet<FieldSchema>,
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
    timeseries_key_for(
        &sample.timeseries_name,
        sample.sorted_target_fields(),
        sample.sorted_metric_fields(),
        sample.measurement.datum_type(),
    )
}

// It's critical that values used for derivation of the timeseries_key are stable.
// We use "bcs" to ensure stability of the derivation across hardware and rust toolchain revisions.
fn canonicalize<T: Serialize + ?Sized>(what: &str, value: &T) -> Vec<u8> {
    bcs::to_bytes(value)
        .unwrap_or_else(|_| panic!("Failed to serialize {what}"))
}

fn timeseries_key_for(
    timeseries_name: &str,
    target_fields: &BTreeMap<String, Field>,
    metric_fields: &BTreeMap<String, Field>,
    datum_type: DatumType,
) -> TimeseriesKey {
    // We use HighwayHasher primarily for stability - it should provide a stable
    // hash for the values used to derive the timeseries_key.
    use highway::HighwayHasher;
    use std::hash::{Hash, Hasher};

    // NOTE: The order of these ".hash" calls matters, changing them will change
    // the derivation of the "timeseries_key". We have change-detector tests for
    // modifications like this, but be cautious, making such a change will
    // impact all currently-provisioned databases.
    let mut hasher = HighwayHasher::default();
    canonicalize("timeseries name", timeseries_name).hash(&mut hasher);
    for field in target_fields.values() {
        canonicalize("target field", &field).hash(&mut hasher);
    }
    for field in metric_fields.values() {
        canonicalize("metric field", &field).hash(&mut hasher);
    }
    canonicalize("datum type", &datum_type).hash(&mut hasher);

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
    use super::*;
    use crate::model::DbFieldList;
    use crate::model::DbTimeseriesSchema;
    use std::convert::TryFrom;
    use uuid::Uuid;

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

    // Validates that the timeseries_key stability for a sample is stable.
    #[test]
    fn test_timeseries_key_sample_stability() {
        #[derive(oximeter::Target)]
        pub struct TestTarget {
            pub name: String,
            pub num: i64,
        }

        #[derive(oximeter::Metric)]
        pub struct TestMetric {
            pub id: Uuid,
            pub datum: i64,
        }

        let target = TestTarget { name: String::from("Hello"), num: 1337 };
        let metric = TestMetric { id: Uuid::nil(), datum: 0x1de };
        let sample = Sample::new(&target, &metric).unwrap();
        let key = super::timeseries_key(&sample);

        expectorate::assert_contents(
            "test-output/sample-timeseries-key.txt",
            &format!("{key}"),
        );
    }

    // Validates that the timeseries_key stability for specific fields is
    // stable.
    #[test]
    fn test_timeseries_key_field_stability() {
        use oximeter::{Field, FieldValue};
        use strum::EnumCount;

        let values = [
            ("string", FieldValue::String(String::default())),
            ("i8", FieldValue::I8(-0x0A)),
            ("u8", FieldValue::U8(0x0A)),
            ("i16", FieldValue::I16(-0x0ABC)),
            ("u16", FieldValue::U16(0x0ABC)),
            ("i32", FieldValue::I32(-0x0ABC_0000)),
            ("u32", FieldValue::U32(0x0ABC_0000)),
            ("i64", FieldValue::I64(-0x0ABC_0000_0000_0000)),
            ("u64", FieldValue::U64(0x0ABC_0000_0000_0000)),
            (
                "ipaddr",
                FieldValue::IpAddr(std::net::IpAddr::V4(
                    std::net::Ipv4Addr::LOCALHOST,
                )),
            ),
            ("uuid", FieldValue::Uuid(uuid::Uuid::nil())),
            ("bool", FieldValue::Bool(true)),
        ];

        // Exhaustively testing enums is a bit tricky. Although it's easy to
        // check "all variants of an enum are matched", it harder to test "all
        // variants of an enum have been supplied".
        //
        // We use this as a proxy, confirming that each variant is represented
        // here for the purposes of tracking stability.
        assert_eq!(values.len(), FieldValue::COUNT);

        let mut output = vec![];
        for (name, value) in values {
            let target_fields = BTreeMap::from([(
                "field".to_string(),
                Field { name: name.to_string(), value },
            )]);
            let metric_fields = BTreeMap::new();
            let key = timeseries_key_for(
                "timeseries name",
                &target_fields,
                &metric_fields,
                // ... Not actually, but we are only trying to compare fields here.
                DatumType::Bool,
            );
            output.push(format!("{name} -> {key}"));
        }

        expectorate::assert_contents(
            "test-output/field-timeseries-keys.txt",
            &output.join("\n"),
        );
    }

    // Test that we correctly order field across a target and metric.
    //
    // In an earlier commit, we switched from storing fields in an unordered Vec
    // to using a BTree{Map,Set} to ensure ordering by name. However, the
    // `TimeseriesSchema` type stored all its fields by chaining the sorted
    // fields from the target and metric, without then sorting _across_ them.
    //
    // This was exacerbated by the error reporting, where we did in fact sort
    // all fields across the target and metric, making it difficult to tell how
    // the derived schema was different, if at all.
    //
    // This test generates a sample with a schema where the target and metric
    // fields are sorted within them, but not across them. We check that the
    // derived schema are actually equal, which means we've imposed that
    // ordering when deriving the schema.
    #[test]
    fn test_schema_field_ordering_across_target_metric() {
        let target_field = FieldSchema {
            name: String::from("later"),
            ty: FieldType::U64,
            source: FieldSource::Target,
        };
        let metric_field = FieldSchema {
            name: String::from("earlier"),
            ty: FieldType::U64,
            source: FieldSource::Metric,
        };
        let timeseries_name: TimeseriesName = "foo:bar".parse().unwrap();
        let datum_type = DatumType::U64;
        let field_schema =
            [target_field.clone(), metric_field.clone()].into_iter().collect();
        let expected_schema = TimeseriesSchema {
            timeseries_name,
            field_schema,
            datum_type,
            created: Utc::now(),
        };

        #[derive(oximeter::Target)]
        struct Foo {
            later: u64,
        }
        #[derive(oximeter::Metric)]
        struct Bar {
            earlier: u64,
            datum: u64,
        }

        let target = Foo { later: 1 };
        let metric = Bar { earlier: 2, datum: 10 };
        let sample = Sample::new(&target, &metric).unwrap();
        let derived_schema = model::schema_for(&sample);
        assert_eq!(derived_schema, expected_schema);
    }

    #[test]
    fn test_unsorted_db_fields_are_sorted_on_read() {
        let target_field = FieldSchema {
            name: String::from("later"),
            ty: FieldType::U64,
            source: FieldSource::Target,
        };
        let metric_field = FieldSchema {
            name: String::from("earlier"),
            ty: FieldType::U64,
            source: FieldSource::Metric,
        };
        let timeseries_name: TimeseriesName = "foo:bar".parse().unwrap();
        let datum_type = DatumType::U64;
        let field_schema =
            [target_field.clone(), metric_field.clone()].into_iter().collect();
        let expected_schema = TimeseriesSchema {
            timeseries_name: timeseries_name.clone(),
            field_schema,
            datum_type,
            created: Utc::now(),
        };

        // The fields here are sorted by target and then metric, which is how we
        // used to insert them into the DB. We're checking that they are totally
        // sorted when we read them out of the DB, even though they are not in
        // the extracted model type.
        let db_fields = DbFieldList {
            names: vec![target_field.name.clone(), metric_field.name.clone()],
            types: vec![target_field.ty.into(), metric_field.ty.into()],
            sources: vec![
                target_field.source.into(),
                metric_field.source.into(),
            ],
        };
        let db_schema = DbTimeseriesSchema {
            timeseries_name: timeseries_name.to_string(),
            field_schema: db_fields,
            datum_type: datum_type.into(),
            created: expected_schema.created,
        };
        assert_eq!(expected_schema, TimeseriesSchema::from(db_schema));
    }

    #[test]
    fn test_field_schema_ordering() {
        let mut fields = BTreeSet::new();
        fields.insert(FieldSchema {
            name: String::from("second"),
            ty: FieldType::U64,
            source: FieldSource::Target,
        });
        fields.insert(FieldSchema {
            name: String::from("first"),
            ty: FieldType::U64,
            source: FieldSource::Target,
        });
        let mut iter = fields.iter();
        assert_eq!(iter.next().unwrap().name, "first");
        assert_eq!(iter.next().unwrap().name, "second");
        assert!(iter.next().is_none());
    }
}
