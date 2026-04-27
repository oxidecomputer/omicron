// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Schema-related types for the Oximeter collector.

use super::types::DatumType;
use super::types::FieldType;
use chrono::DateTime;
use chrono::Utc;
use parse_display::Display;
use parse_display::FromStr;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;
use std::num::NonZeroU8;

pub type TimeseriesKey = u64;

/// Default version for timeseries schema, 1.
pub const fn default_schema_version() -> NonZeroU8 {
    unsafe { NonZeroU8::new_unchecked(1) }
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
    pub field_type: FieldType,
    pub source: FieldSource,
    pub description: String,
}

/// The source from which a field is derived, the target or metric.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
    FromStr,
    Display,
)]
#[serde(rename_all = "snake_case")]
pub enum FieldSource {
    Target,
    Metric,
}

/// A timeseries name.
///
/// Timeseries are named by concatenating the names of their target and metric, joined with a
/// colon.
#[derive(
    Debug, Clone, PartialEq, PartialOrd, Ord, Eq, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "&str")]
pub struct TimeseriesName(pub(crate) String);

impl JsonSchema for TimeseriesName {
    fn schema_name() -> String {
        "TimeseriesName".to_string()
    }

    fn json_schema(
        _: &mut schemars::r#gen::SchemaGenerator,
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

/// Text descriptions for the target and metric of a timeseries.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct TimeseriesDescription {
    pub target: String,
    pub metric: String,
}

/// Measurement units for timeseries samples.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
// TODO-completeness: Decide whether and how to handle dimensional analysis
// during queries, if needed.
pub enum Units {
    /// No meaningful units, e.g. a dimensionless quanity.
    None,
    Count,
    Bytes,
    Seconds,
    Nanoseconds,
    Volts,
    Amps,
    Watts,
    DegreesCelsius,
    /// Rotations per minute.
    Rpm,
}

/// The schema for a timeseries.
///
/// This includes the name of the timeseries, as well as the datum type of its metric and the
/// schema for each field.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct TimeseriesSchema {
    pub timeseries_name: TimeseriesName,
    pub description: TimeseriesDescription,
    pub field_schema: BTreeSet<FieldSchema>,
    pub datum_type: DatumType,
    pub version: NonZeroU8,
    pub authz_scope: AuthzScope,
    pub units: Units,
    pub created: DateTime<Utc>,
}

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
pub(crate) const TIMESERIES_NAME_REGEX: &str =
    "^(([a-z]+[a-z0-9]*)(_([a-z0-9]+))*):(([a-z]+[a-z0-9]*)(_([a-z0-9]+))*)$";

/// Authorization scope for a timeseries.
///
/// This describes the level at which a user must be authorized to read data
/// from a timeseries. For example, fleet-scoping means the data is only visible
/// to an operator or fleet reader. Project-scoped, on the other hand, indicates
/// that a user will see data limited to the projects on which they have read
/// permissions.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AuthzScope {
    /// Timeseries data is limited to fleet readers.
    Fleet,
    /// Timeseries data is limited to the authorized silo for a user.
    Silo,
    /// Timeseries data is limited to the authorized projects for a user.
    Project,
    /// The timeseries is viewable to all without limitation.
    ViewableToAll,
}
