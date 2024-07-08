// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

//! Tools for working with schema for fields and timeseries.

pub mod codegen;
pub mod ir;

use crate::types::DatumType;
use crate::types::FieldType;
use crate::types::MetricsError;
use crate::types::Sample;
use crate::Metric;
use crate::Target;
use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Write;
use std::num::NonZeroU8;
use std::path::Path;

/// Full path to the directory containing all schema.
///
/// This is defined in this crate as the single source of truth, but not
/// re-exported outside implementation crates (e.g., not via `oximeter` or
/// `oximeter-collector`.
pub const SCHEMA_DIRECTORY: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../oximeter/schema");

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

impl FieldSchema {
    /// Return `true` if this field is copyable.
    pub const fn is_copyable(&self) -> bool {
        self.field_type.is_copyable()
    }
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
    type Error = MetricsError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        validate_timeseries_name(s).map(|s| TimeseriesName(s.to_string()))
    }
}

impl std::convert::TryFrom<String> for TimeseriesName {
    type Error = MetricsError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        validate_timeseries_name(&s)?;
        Ok(TimeseriesName(s))
    }
}

impl std::str::FromStr for TimeseriesName {
    type Err = MetricsError;
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

fn validate_timeseries_name(s: &str) -> Result<&str, MetricsError> {
    if regex::Regex::new(TIMESERIES_NAME_REGEX).unwrap().is_match(s) {
        Ok(s)
    } else {
        Err(MetricsError::InvalidTimeseriesName)
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
// TODO-completeness: Include more units, such as power / temperature.
// TODO-completeness: Decide whether and how to handle dimensional analysis
// during queries, if needed.
pub enum Units {
    Count,
    Bytes,
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

/// Default version for timeseries schema, 1.
pub const fn default_schema_version() -> NonZeroU8 {
    unsafe { NonZeroU8::new_unchecked(1) }
}

impl From<&Sample> for TimeseriesSchema {
    fn from(sample: &Sample) -> Self {
        let timeseries_name = sample
            .timeseries_name
            .parse()
            .expect("expected a legal timeseries name in a sample");
        let mut field_schema = BTreeSet::new();
        for field in sample.target_fields() {
            let schema = FieldSchema {
                name: field.name.clone(),
                field_type: field.value.field_type(),
                source: FieldSource::Target,
                description: String::new(),
            };
            field_schema.insert(schema);
        }
        for field in sample.metric_fields() {
            let schema = FieldSchema {
                name: field.name.clone(),
                field_type: field.value.field_type(),
                source: FieldSource::Metric,
                description: String::new(),
            };
            field_schema.insert(schema);
        }
        let datum_type = sample.measurement.datum_type();
        Self {
            timeseries_name,
            description: Default::default(),
            field_schema,
            datum_type,
            version: default_schema_version(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Count,
            created: Utc::now(),
        }
    }
}

impl TimeseriesSchema {
    /// Construct a timeseries schema from a target and metric.
    pub fn new<T, M>(target: &T, metric: &M) -> Result<Self, MetricsError>
    where
        T: Target,
        M: Metric,
    {
        let timeseries_name = crate::timeseries_name(target, metric)?;
        let mut field_schema = BTreeSet::new();
        for field in target.fields() {
            let schema = FieldSchema {
                name: field.name.clone(),
                field_type: field.value.field_type(),
                source: FieldSource::Target,
                description: String::new(),
            };
            field_schema.insert(schema);
        }
        for field in metric.fields() {
            let schema = FieldSchema {
                name: field.name.clone(),
                field_type: field.value.field_type(),
                source: FieldSource::Metric,
                description: String::new(),
            };
            field_schema.insert(schema);
        }
        let datum_type = metric.datum_type();
        Ok(Self {
            timeseries_name,
            description: Default::default(),
            field_schema,
            datum_type,
            version: default_schema_version(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Count,
            created: Utc::now(),
        })
    }

    /// Construct a timeseries schema from a sample
    pub fn from_sample(sample: &Sample) -> Self {
        Self::from(sample)
    }

    /// Return the schema for the given field.
    pub fn schema_for_field<S>(&self, name: S) -> Option<&FieldSchema>
    where
        S: AsRef<str>,
    {
        self.field_schema.iter().find(|field| field.name == name.as_ref())
    }

    /// Return an iterator over the target fields.
    pub fn target_fields(&self) -> impl Iterator<Item = &FieldSchema> {
        self.field_iter(FieldSource::Target)
    }

    /// Return an iterator over the metric fields.
    pub fn metric_fields(&self) -> impl Iterator<Item = &FieldSchema> {
        self.field_iter(FieldSource::Metric)
    }

    /// Return an iterator over fields from the given source.
    fn field_iter(
        &self,
        source: FieldSource,
    ) -> impl Iterator<Item = &FieldSchema> {
        self.field_schema.iter().filter(move |field| field.source == source)
    }

    /// Return the target and metric component names for this timeseries
    pub fn component_names(&self) -> (&str, &str) {
        self.timeseries_name
            .split_once(':')
            .expect("Incorrectly formatted timseries name")
    }

    /// Return the name of the target for this timeseries.
    pub fn target_name(&self) -> &str {
        self.component_names().0
    }

    /// Return the name of the metric for this timeseries.
    pub fn metric_name(&self) -> &str {
        self.component_names().1
    }
}

impl PartialEq for TimeseriesSchema {
    fn eq(&self, other: &TimeseriesSchema) -> bool {
        self.timeseries_name == other.timeseries_name
            && self.version == other.version
            && self.datum_type == other.datum_type
            && self.field_schema == other.field_schema
    }
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
const TIMESERIES_NAME_REGEX: &str =
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

/// A set of timeseries schema, useful for testing changes to targets or
/// metrics.
#[derive(Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct SchemaSet {
    #[serde(flatten)]
    inner: BTreeMap<TimeseriesName, TimeseriesSchema>,
}

impl SchemaSet {
    /// Insert a timeseries schema, checking for conflicts.
    ///
    /// This inserts the schema derived from `target` and `metric`. If one
    /// does _not_ already exist in `self` or a _matching_ one exists, `None`
    /// is returned.
    ///
    /// If the derived schema _conflicts_ with one in `self`, the existing
    /// schema is returned.
    pub fn insert_checked<T, M>(
        &mut self,
        target: &T,
        metric: &M,
    ) -> Result<Option<TimeseriesSchema>, MetricsError>
    where
        T: Target,
        M: Metric,
    {
        let new = TimeseriesSchema::new(target, metric)?;
        let name = new.timeseries_name.clone();
        match self.inner.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(new);
                Ok(None)
            }
            Entry::Occupied(entry) => {
                let existing = entry.get();
                if existing == &new {
                    Ok(None)
                } else {
                    Ok(Some(existing.clone()))
                }
            }
        }
    }

    /// Compare the set of schema against the contents of a file.
    ///
    /// This function loads a `SchemaSet` from the provided JSON file, and
    /// asserts that the contained schema matches those in `self`. Note that
    /// equality of `TimeseriesSchema` ignores creation timestamps, so this
    /// compares the "identity" data: timeseries name, field names, field types,
    /// and field sources.
    ///
    /// This is intentionally similar to `expectorate::assert_contents()`. If
    /// the provided file doesn't exist, it's treated as empty. If it does, a
    /// `SchemaSet` is deserialized from it and a comparison between that and
    /// `self` is done.
    ///
    /// You can use `EXPECTORATE=overwrite` to overwrite the existing file,
    /// rather than panicking.
    pub fn assert_contents(&self, path: impl AsRef<Path>) {
        let path = path.as_ref();
        let v = std::env::var_os("EXPECTORATE");
        let overwrite =
            v.as_deref().and_then(std::ffi::OsStr::to_str) == Some("overwrite");
        let expected_contents = serde_json::to_string_pretty(self).unwrap();
        if overwrite {
            if let Err(e) = std::fs::write(path, &expected_contents) {
                panic!(
                    "Failed to write contents to '{}': {}",
                    path.display(),
                    e
                );
            }
        } else {
            // If the file doesn't exist, it's just empty and we'll create an
            // empty set of schema.
            let contents = if !path.exists() {
                String::from("{}")
            } else {
                match std::fs::read_to_string(path) {
                    Err(e) => {
                        panic!("Failed to read '{}': {}", path.display(), e)
                    }
                    Ok(c) => c,
                }
            };
            let other: Self = serde_json::from_str(&contents).unwrap();
            if self == &other {
                return;
            }

            let mut diffs = String::new();
            writeln!(
                &mut diffs,
                "Timeseries schema in \"{}\" do not match\n",
                path.display()
            )
            .unwrap();

            // Print schema in self that are not in the file, or mismatched
            // schema.
            for (name, schema) in self.inner.iter() {
                let Some(other_schema) = other.inner.get(name) else {
                    writeln!(
                        &mut diffs,
                        "File is missing timeseries \"{}\"",
                        name
                    )
                    .unwrap();
                    continue;
                };
                if schema == other_schema {
                    continue;
                }
                writeln!(&mut diffs, "Timeseries \"{name}\" differs").unwrap();

                // Print out any differences in the datum type.
                if schema.datum_type != other_schema.datum_type {
                    writeln!(
                        &mut diffs,
                        " Expected datum type: {}",
                        schema.datum_type
                    )
                    .unwrap();
                    writeln!(
                        &mut diffs,
                        " Actual datum type: {}",
                        other_schema.datum_type
                    )
                    .unwrap();
                }

                // Print fields in self that are not in other, or are mismatched
                for field in schema.field_schema.iter() {
                    let Some(other_field) =
                        other_schema.field_schema.get(field)
                    else {
                        writeln!(
                            &mut diffs,
                            " File is missing {:?} field \"{}\"",
                            field.source, field.name,
                        )
                        .unwrap();
                        continue;
                    };
                    if field == other_field {
                        continue;
                    }

                    writeln!(
                        &mut diffs,
                        " File has mismatched field \"{}\"",
                        field.name
                    )
                    .unwrap();
                    writeln!(
                        &mut diffs,
                        "  Expected type: {}",
                        field.field_type
                    )
                    .unwrap();
                    writeln!(
                        &mut diffs,
                        "  Actual type: {}",
                        other_field.field_type
                    )
                    .unwrap();
                    writeln!(
                        &mut diffs,
                        "  Expected source: {:?}",
                        field.source
                    )
                    .unwrap();
                    writeln!(
                        &mut diffs,
                        "  Actual source: {:?}",
                        other_field.source
                    )
                    .unwrap();
                }

                // Print fields in other that are not in self, fields that are
                // in both but don't match are taken care of in the above loop.
                for other_field in other_schema.field_schema.iter() {
                    if schema.field_schema.contains(other_field) {
                        continue;
                    }

                    writeln!(
                        &mut diffs,
                        " Current set is missing {:?} field \"{}\"",
                        other_field.source, other_field.name,
                    )
                    .unwrap();
                }
            }

            // Print schema that are in the file, but not self. Those that don't
            // match are handled in the above block.
            for key in other.inner.keys() {
                if !self.inner.contains_key(key) {
                    writeln!(
                        &mut diffs,
                        " Current set is missing timeseries \"{}\"",
                        key
                    )
                    .unwrap();
                }
            }
            panic!("{}", diffs);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert!(TimeseriesName::try_from("x.a:b").is_err());
    }

    #[derive(Target)]
    struct MyTarget {
        id: Uuid,
        name: String,
    }

    const ID: Uuid = uuid::uuid!("ca565ef4-65dc-4ab0-8622-7be43ed72105");

    impl Default for MyTarget {
        fn default() -> Self {
            Self { id: ID, name: String::from("name") }
        }
    }

    #[derive(Metric)]
    struct MyMetric {
        happy: bool,
        datum: u64,
    }

    impl Default for MyMetric {
        fn default() -> Self {
            Self { happy: true, datum: 0 }
        }
    }

    #[test]
    fn test_timeseries_schema_from_parts() {
        let target = MyTarget::default();
        let metric = MyMetric::default();
        let schema = TimeseriesSchema::new(&target, &metric).unwrap();

        assert_eq!(schema.timeseries_name, "my_target:my_metric");
        let f = schema.schema_for_field("id").unwrap();
        assert_eq!(f.name, "id");
        assert_eq!(f.field_type, FieldType::Uuid);
        assert_eq!(f.source, FieldSource::Target);

        let f = schema.schema_for_field("name").unwrap();
        assert_eq!(f.name, "name");
        assert_eq!(f.field_type, FieldType::String);
        assert_eq!(f.source, FieldSource::Target);

        let f = schema.schema_for_field("happy").unwrap();
        assert_eq!(f.name, "happy");
        assert_eq!(f.field_type, FieldType::Bool);
        assert_eq!(f.source, FieldSource::Metric);
        assert_eq!(schema.datum_type, DatumType::U64);
    }

    #[test]
    fn test_timeseries_schema_from_sample() {
        let target = MyTarget::default();
        let metric = MyMetric::default();
        let sample = Sample::new(&target, &metric).unwrap();
        let schema = TimeseriesSchema::new(&target, &metric).unwrap();
        let schema_from_sample = TimeseriesSchema::from(&sample);
        assert_eq!(schema, schema_from_sample);
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
            field_type: FieldType::U64,
            source: FieldSource::Target,
            description: String::new(),
        };
        let metric_field = FieldSchema {
            name: String::from("earlier"),
            field_type: FieldType::U64,
            source: FieldSource::Metric,
            description: String::new(),
        };
        let timeseries_name: TimeseriesName = "foo:bar".parse().unwrap();
        let datum_type = DatumType::U64;
        let field_schema =
            [target_field.clone(), metric_field.clone()].into_iter().collect();
        let expected_schema = TimeseriesSchema {
            timeseries_name,
            description: Default::default(),
            field_schema,
            datum_type,
            version: default_schema_version(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Count,
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
        let derived_schema = TimeseriesSchema::from(&sample);
        assert_eq!(derived_schema, expected_schema);
    }

    #[test]
    fn test_field_schema_ordering() {
        let mut fields = BTreeSet::new();
        fields.insert(FieldSchema {
            name: String::from("second"),
            field_type: FieldType::U64,
            source: FieldSource::Target,
            description: String::new(),
        });
        fields.insert(FieldSchema {
            name: String::from("first"),
            field_type: FieldType::U64,
            source: FieldSource::Target,
            description: String::new(),
        });
        let mut iter = fields.iter();
        assert_eq!(iter.next().unwrap().name, "first");
        assert_eq!(iter.next().unwrap().name, "second");
        assert!(iter.next().is_none());
    }
}
