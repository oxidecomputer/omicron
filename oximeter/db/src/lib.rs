// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for interacting with the control plane telemetry database.

// Copyright 2025 Oxide Computer Company

use crate::query::StringFieldSelector;
use anyhow::Context as _;
use chrono::DateTime;
use chrono::Utc;
pub use oximeter::DatumType;
pub use oximeter::Field;
pub use oximeter::FieldType;
pub use oximeter::Measurement;
pub use oximeter::Sample;
pub use oximeter::schema::FieldSchema;
pub use oximeter::schema::FieldSource;
use oximeter::schema::TimeseriesKey;
pub use oximeter::schema::TimeseriesName;
pub use oximeter::schema::TimeseriesSchema;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use slog::Logger;
use std::collections::BTreeMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::path::PathBuf;
use thiserror::Error;

mod client;
pub(crate) mod model;
pub mod native;
#[cfg(any(feature = "oxql", test))]
pub mod oxql;
pub mod query;
#[cfg(any(
    feature = "oxql",
    feature = "sql",
    feature = "native-sql-shell",
    test
))]
pub mod shells;
#[cfg(any(feature = "sql", test))]
pub mod sql;

pub use client::Client;
pub use client::DbWrite;
pub use client::TestDbWrite;
#[cfg(any(feature = "oxql", test))]
pub use client::oxql::OxqlResult;
pub use model::OXIMETER_VERSION;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to create reqwest client")]
    Reqwest(#[from] reqwest::Error),

    #[error("Failed to check out connection to database")]
    Connection(#[from] qorb::pool::Error),

    #[error("Oximeter core error: {0}")]
    Oximeter(#[from] oximeter::MetricsError),

    /// The telemetry database could not be reached.
    #[error("Telemetry database unavailable: {0}")]
    DatabaseUnavailable(String),

    #[error("Missing expected metadata header key '{key}'")]
    MissingHeaderKey { key: String },

    #[error("Invalid or malformed query metadata for key '{key}': {msg}")]
    BadMetadata { key: String, msg: String },

    /// An error interacting with the telemetry database
    #[error("Error interacting with telemetry database: {0}")]
    Database(String),

    /// A schema provided when collecting samples did not match the expected schema
    #[error("Schema mismatch for timeseries '{0}'", expected.timeseries_name)]
    SchemaMismatch {
        expected: Box<TimeseriesSchema>,
        actual: Box<TimeseriesSchema>,
    },

    #[error("Timeseries not found for: {0}")]
    TimeseriesNotFound(String),

    #[error(
        "The field comparison operation '{op}' is not valid for field '{field_name}' with type {field_type}"
    )]
    InvalidSelectionOp { op: String, field_name: String, field_type: FieldType },

    #[error(
        "Timeseries '{timeseries_name}' does not contain a field with name '{field_name}'"
    )]
    NoSuchField { timeseries_name: String, field_name: String },

    #[error(
        "Field '{field_name}' requires a value of type {expected_type}, found {found_type}"
    )]
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

    #[error(
        "Invalid value for field '{field_name}' with type {field_type}: '{value}'"
    )]
    InvalidFieldValue {
        field_name: String,
        field_type: FieldType,
        value: String,
    },

    #[error("The field comparison {op} is not valid for the type {ty}")]
    InvalidFieldCmp { op: String, ty: FieldType },

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

    #[error("Could not read timeseries_to_delete file")]
    ReadTimeseriesToDeleteFile {
        #[source]
        err: io::Error,
    },

    #[cfg(any(feature = "sql", test))]
    #[error("SQL error")]
    Sql(#[from] sql::Error),

    #[cfg(any(feature = "oxql", test))]
    #[error(transparent)]
    Oxql(oxql::Error),

    #[error("Native protocol error")]
    Native(#[from] crate::native::Error),

    #[error("Query unexpectedly contained no data: '{query}'")]
    QueryMissingData { query: String },
}

#[cfg(any(feature = "oxql", test))]
impl From<crate::oxql::Error> for Error {
    fn from(e: crate::oxql::Error) -> Self {
        Error::Oxql(e)
    }
}

/// Alias for a connection to the database claimed from a `qorb` pool.
pub(crate) type Handle = qorb::claim::Handle<crate::native::Connection>;

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

/// Create a client to the timeseries database, and ensure the database exists.
pub async fn make_client(
    address: IpAddr,
    port: u16,
    log: &Logger,
) -> Result<Client, anyhow::Error> {
    let client = Client::new(SocketAddr::new(address, port), &log);
    // TODO https://github.com/oxidecomputer/omicron/issues/7488: There is a db being initialised here as well.
    client
        .init_single_node_db()
        .await
        .context("Failed to initialize timeseries database")?;
    Ok(client)
}

// TODO-cleanup: Add the timeseries version in to the computation of the key.
// This will require a full drop of the database, since we're changing the
// sorting key and the timeseries key on each past sample. See
// https://github.com/oxidecomputer/omicron/issues/5942 for more details.
pub(crate) fn timeseries_key(sample: &Sample) -> TimeseriesKey {
    timeseries_key_for(
        &sample.timeseries_name,
        // sample.timeseries_version
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

// The name of the oximeter cluster, in the case of a replicated database.
//
// This must match what is used in the replicated SQL files when created the
// database itself, and the XML files describing the cluster.
const CLUSTER_NAME: &str = "oximeter_cluster";

// The name of the table storing database version information.
const VERSION_TABLE_NAME: &str = "version";

// During schema upgrades, it is possible to list timeseries that should be
// deleted, rather than deleting the entire database. These must be listed one
// per line, in the file inside the schema version directory with this name.
const TIMESERIES_TO_DELETE_FILE: &str = "timeseries-to-delete.txt";

#[cfg(test)]
mod tests {
    use crate::timeseries_key;
    use crate::timeseries_key_for;
    use oximeter::DatumType;
    use oximeter::Sample;
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use uuid::Uuid;

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
        let key = timeseries_key(&sample);

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
            ("string", FieldValue::String(Cow::Owned(String::default()))),
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
}
