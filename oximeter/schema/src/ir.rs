// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

//! Serialization of timeseries schema definitions.
//!
//! These types are used as an intermediate representation of schema. The schema
//! are written in TOML; deserialized into these types; and then either
//! inspected or used to generate code that contains the equivalent Rust types
//! and trait implementations.

use chrono::Utc;
use oximeter_types::AuthzScope;
use oximeter_types::DatumType;
use oximeter_types::FieldSchema;
use oximeter_types::FieldSource;
use oximeter_types::FieldType;
use oximeter_types::MetricsError;
use oximeter_types::TimeseriesDescription;
use oximeter_types::TimeseriesName;
use oximeter_types::TimeseriesSchema;
use oximeter_types::Units;
use serde::Deserialize;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroU8;

mod limits {
    pub const MAX_FIELD_NAME_LENGTH: usize = 64;
    pub const MAX_DESCRIPTION_LENGTH: usize = 1024;
    pub const MAX_TIMESERIES_NAME_LENGTH: usize = 128;
}

#[derive(Debug, Deserialize)]
pub struct FieldMetadata {
    #[serde(rename = "type")]
    pub type_: FieldType,
    pub description: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum MetricFields {
    Removed { removed_in: NonZeroU8 },
    Added { added_in: NonZeroU8, fields: Vec<String> },
    Versioned(VersionedFields),
}

#[derive(Debug, Deserialize)]
pub struct VersionedFields {
    pub version: NonZeroU8,
    pub fields: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct TargetDefinition {
    pub name: String,
    pub description: String,
    pub authz_scope: AuthzScope,
    pub versions: Vec<VersionedFields>,
}

#[derive(Debug, Deserialize)]
pub struct MetricDefinition {
    pub name: String,
    pub description: String,
    pub units: Units,
    pub datum_type: DatumType,
    pub versions: Vec<MetricFields>,
}

fn checked_version_deser<'de, D>(d: D) -> Result<NonZeroU8, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let x = NonZeroU8::deserialize(d)?;
    if x.get() == 1 {
        Ok(x)
    } else {
        Err(serde::de::Error::custom(format!(
            "Only version 1 of the timeseries defintion format \
            is currently supported, found version {x}",
        )))
    }
}

#[derive(Debug, Deserialize)]
pub struct TimeseriesDefinition {
    #[serde(deserialize_with = "checked_version_deser")]
    pub format_version: NonZeroU8,
    pub target: TargetDefinition,
    pub metrics: Vec<MetricDefinition>,
    pub fields: BTreeMap<String, FieldMetadata>,
}

impl TimeseriesDefinition {
    pub fn into_schema_list(
        self,
    ) -> Result<Vec<TimeseriesSchema>, MetricsError> {
        if self.target.versions.is_empty() {
            return Err(MetricsError::SchemaDefinition(String::from(
                "At least one target version must be defined",
            )));
        }
        if self.metrics.is_empty() {
            return Err(MetricsError::SchemaDefinition(String::from(
                "At least one metric must be defined",
            )));
        }
        let mut timeseries = BTreeMap::new();
        let target_name = &self.target.name;

        // Validate text length limits on field names and descriptions.
        if self.target.name.is_empty() {
            return Err(MetricsError::SchemaDefinition(String::from(
                "Target name cannot be empty",
            )));
        }
        for (name, metadata) in self.fields.iter() {
            if name.is_empty() {
                return Err(MetricsError::SchemaDefinition(String::from(
                    "Field names cannot be empty",
                )));
            }
            if name.len() > limits::MAX_FIELD_NAME_LENGTH {
                return Err(MetricsError::SchemaDefinition(format!(
                    "Field name '{}' is {} characters, which exceeds the \
                    maximum field name length of {}",
                    name,
                    name.len(),
                    limits::MAX_FIELD_NAME_LENGTH,
                )));
            }
            if metadata.description.is_empty() {
                return Err(MetricsError::SchemaDefinition(format!(
                    "Description of field '{}' cannot be empty",
                    name,
                )));
            }
            if metadata.description.len() > limits::MAX_DESCRIPTION_LENGTH {
                return Err(MetricsError::SchemaDefinition(format!(
                    "Description of field '{}' is {} characters, which \
                    exceeds the maximum description length of {}",
                    name,
                    metadata.description.len(),
                    limits::MAX_DESCRIPTION_LENGTH,
                )));
            }
        }

        // At this point, we do not support actually _modifying_ schema.
        // Instead, we're putting in place infrastructure to support multiple
        // versions, while still requiring all schema to define the first and
        // only the first version.
        //
        // We omit this check in tests, to ensure that the code correctly
        // handles updates.
        #[cfg(not(test))]
        if self.target.versions.len() > 1
            || self
                .target
                .versions
                .iter()
                .any(|fields| fields.version.get() > 1)
        {
            return Err(MetricsError::SchemaDefinition(String::from(
                "Exactly one timeseries version, with version number 1, \
                may currently be specified. Updates will be supported \
                in future releases.",
            )));
        }

        // First create a map from target version to the fields in it.
        //
        // This is used to do O(lg n) lookups into the set of target fields when we
        // iterate through metric versions below, i.e., avoiding quadratic behavior.
        let mut target_fields_by_version = BTreeMap::new();
        for (expected_version, target_fields) in
            (1u8..).zip(self.target.versions.iter())
        {
            if expected_version != target_fields.version.get() {
                return Err(MetricsError::SchemaDefinition(format!(
                    "Target '{}' versions should be sequential \
                    and monotonically increasing (expected {}, found {})",
                    target_name, expected_version, target_fields.version,
                )));
            }

            let fields: BTreeSet<_> =
                target_fields.fields.iter().cloned().collect();
            if fields.len() != target_fields.fields.len() {
                return Err(MetricsError::SchemaDefinition(format!(
                    "Target '{}' version {} lists duplicate field names",
                    target_name, expected_version,
                )));
            }
            if fields.is_empty() {
                return Err(MetricsError::SchemaDefinition(format!(
                    "Target '{}' version {} must have at least one field",
                    target_name, expected_version,
                )));
            }

            if target_fields_by_version
                .insert(expected_version, fields)
                .is_some()
            {
                return Err(MetricsError::SchemaDefinition(format!(
                    "Target '{}' version {} is duplicated",
                    target_name, expected_version,
                )));
            }
        }

        // Start by looping over all the metrics in the definition.
        //
        // As we do so, we'll attach the target definition at the corresponding
        // version, along with running some basic lints and checks.
        for metric in self.metrics.iter() {
            let metric_name = &metric.name;
            if metric_name.is_empty() {
                return Err(MetricsError::SchemaDefinition(String::from(
                    "Metric name cannot be empty",
                )));
            }
            let timeseries_name = TimeseriesName::try_from(format!(
                "{}:{}",
                target_name, metric_name
            ))?;
            if timeseries_name.as_str().len()
                > limits::MAX_TIMESERIES_NAME_LENGTH
            {
                return Err(MetricsError::SchemaDefinition(format!(
                    "Timeseries name '{}' is {} characters, which \
                    exceeds the maximum length of {}",
                    timeseries_name,
                    timeseries_name.len(),
                    limits::MAX_TIMESERIES_NAME_LENGTH,
                )));
            }

            // Store the current version of the metric. This doesn't need to be
            // sequential, but they do need to be monotonic and have a matching
            // target version. We'll fill in any gaps with the last active version
            // of the metric (if any).
            let mut current_version: Option<CurrentVersion> = None;

            // Also store the last used version of the target. This lets users omit
            // an unchanged metric, and we use this value to fill in the implied
            // version of the metric.
            let mut last_target_version: u8 = 0;

            // Iterate through each version of this metric.
            //
            // In general, we expect metrics to be addded in the first version;
            // modified by adding / removing fields; and possibly removed at the
            // end. However, they can be added / removed multiple times, and not
            // added until a later version of the target.
            for metric_fields in metric.versions.iter() {
                // Fill in any gaps from the last target version to this next
                // metric version. This only works once we've filled in at least
                // one version of the metric, and stored the current version /
                // fields.
                if let Some(current) = current_version.as_ref() {
                    let current_fields = current
                        .fields()
                        .expect("Should have some fields if we have any previous version");
                    while last_target_version <= current.version().get() {
                        last_target_version += 1;
                        let Some(target_fields) =
                            target_fields_by_version.get(&last_target_version)
                        else {
                            return Err(MetricsError::SchemaDefinition(
                                format!(
                                    "Metric '{}' version {} does not have \
                                a matching version in the target '{}'",
                                    metric_name,
                                    last_target_version,
                                    target_name,
                                ),
                            ));
                        };
                        let field_schema = construct_field_schema(
                            &self.fields,
                            target_name,
                            target_fields,
                            metric_name,
                            current_fields,
                        )?;
                        let authz_scope = extract_authz_scope(
                            metric_name,
                            self.target.authz_scope,
                            &field_schema,
                        )?;
                        let version =
                            NonZeroU8::new(last_target_version).unwrap();
                        let description = TimeseriesDescription {
                            target: self.target.description.clone(),
                            metric: metric.description.clone(),
                        };
                        let schema = TimeseriesSchema {
                            timeseries_name: timeseries_name.clone(),
                            description,
                            field_schema,
                            datum_type: metric.datum_type,
                            version,
                            authz_scope,
                            units: metric.units,
                            created: Utc::now(),
                        };
                        if let Some(old) = timeseries
                            .insert((timeseries_name.clone(), version), schema)
                        {
                            return Err(MetricsError::SchemaDefinition(
                                format!(
                                    "Timeseries '{}' version {} is duplicated",
                                    old.timeseries_name, old.version,
                                ),
                            ));
                        }
                    }
                }

                // Extract the fields named in this version, checking that they're
                // compatible with the last known version, if any.
                let new_version = extract_metric_fields(
                    metric_name,
                    metric_fields,
                    &current_version,
                )?;
                let version = current_version.insert(new_version);
                let Some(metric_fields) = version.fields() else {
                    continue;
                };

                // Now, insert the _next_ version of the metric with the
                // validated fields we've collected for it.
                last_target_version += 1;
                let Some(target_fields) =
                    target_fields_by_version.get(&last_target_version)
                else {
                    return Err(MetricsError::SchemaDefinition(format!(
                        "Metric '{}' version {} does not have \
                        a matching version in the target '{}'",
                        metric_name, last_target_version, target_name,
                    )));
                };
                let field_schema = construct_field_schema(
                    &self.fields,
                    target_name,
                    target_fields,
                    metric_name,
                    metric_fields,
                )?;
                let authz_scope = extract_authz_scope(
                    metric_name,
                    self.target.authz_scope,
                    &field_schema,
                )?;
                let timeseries_name = TimeseriesName::try_from(format!(
                    "{}:{}",
                    target_name, metric_name
                ))?;
                let version = NonZeroU8::new(last_target_version).unwrap();
                let description = TimeseriesDescription {
                    target: self.target.description.clone(),
                    metric: metric.description.clone(),
                };
                let schema = TimeseriesSchema {
                    timeseries_name: timeseries_name.clone(),
                    description,
                    field_schema,
                    datum_type: metric.datum_type,
                    version,
                    authz_scope,
                    units: metric.units,
                    created: Utc::now(),
                };
                if let Some(old) =
                    timeseries.insert((timeseries_name, version), schema)
                {
                    return Err(MetricsError::SchemaDefinition(format!(
                        "Timeseries '{}' version {} is duplicated",
                        old.timeseries_name, old.version,
                    )));
                }
            }

            // We also allow omitting later versions of metrics if they are
            // unchanged. A target has to specify every version, even if it's the
            // same, but the metrics need only specify differences.
            //
            // Here, look for any target version strictly later than the last metric
            // version, and create a corresponding target / metric pair for it.
            if let Some(last_metric_fields) = metric.versions.last() {
                match last_metric_fields {
                    MetricFields::Removed { .. } => {}
                    MetricFields::Added {
                        added_in: last_metric_version,
                        fields,
                    }
                    | MetricFields::Versioned(VersionedFields {
                        version: last_metric_version,
                        fields,
                    }) => {
                        let metric_field_names: BTreeSet<_> =
                            fields.iter().cloned().collect();
                        let next_version = last_metric_version
                            .get()
                            .checked_add(1)
                            .expect("version < 256");
                        for (version, target_fields) in
                            target_fields_by_version.range(next_version..)
                        {
                            let field_schema = construct_field_schema(
                                &self.fields,
                                target_name,
                                target_fields,
                                metric_name,
                                &metric_field_names,
                            )?;
                            let authz_scope = extract_authz_scope(
                                metric_name,
                                self.target.authz_scope,
                                &field_schema,
                            )?;
                            let timeseries_name = TimeseriesName::try_from(
                                format!("{}:{}", target_name, metric_name),
                            )?;
                            let version = NonZeroU8::new(*version).unwrap();
                            let description = TimeseriesDescription {
                                target: self.target.description.clone(),
                                metric: metric.description.clone(),
                            };
                            let schema = TimeseriesSchema {
                                timeseries_name: timeseries_name.clone(),
                                description,
                                field_schema,
                                datum_type: metric.datum_type,
                                version,
                                authz_scope,
                                units: metric.units,
                                created: Utc::now(),
                            };
                            if let Some(old) = timeseries
                                .insert((timeseries_name, version), schema)
                            {
                                return Err(MetricsError::SchemaDefinition(
                                    format!(
                                    "Timeseries '{}' version {} is duplicated",
                                    old.timeseries_name, old.version,
                                ),
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(timeseries.into_values().collect())
    }
}

#[derive(Clone, Debug)]
enum CurrentVersion {
    Active { version: NonZeroU8, fields: BTreeSet<String> },
    Inactive { removed_in: NonZeroU8 },
}

impl CurrentVersion {
    fn version(&self) -> NonZeroU8 {
        match self {
            CurrentVersion::Active { version, .. } => *version,
            CurrentVersion::Inactive { removed_in } => *removed_in,
        }
    }

    fn fields(&self) -> Option<&BTreeSet<String>> {
        match self {
            CurrentVersion::Active { fields, .. } => Some(fields),
            CurrentVersion::Inactive { .. } => None,
        }
    }
}

/// Load the list of timeseries schema from a schema definition in TOML format.
pub fn load_schema(
    contents: &str,
) -> Result<Vec<TimeseriesSchema>, MetricsError> {
    toml::from_str::<TimeseriesDefinition>(contents)
        .map_err(|e| {
            MetricsError::Toml(
                slog_error_chain::InlineErrorChain::new(&e).to_string(),
            )
        })
        .and_then(TimeseriesDefinition::into_schema_list)
}

// Find schema of a specified version in an iterator, or the latest.
pub(super) fn find_schema_version(
    list: impl Iterator<Item = TimeseriesSchema>,
    version: Option<NonZeroU8>,
) -> Vec<TimeseriesSchema> {
    match version {
        Some(ver) => list.into_iter().filter(|s| s.version == ver).collect(),
        None => {
            let mut last_version = BTreeMap::new();
            for schema in list {
                let metric_name = schema.metric_name().to_string();
                match last_version.entry(metric_name) {
                    Entry::Vacant(entry) => {
                        entry.insert((schema.version, schema.clone()));
                    }
                    Entry::Occupied(mut entry) => {
                        let existing_version = entry.get().0;
                        if existing_version < schema.version {
                            entry.insert((schema.version, schema.clone()));
                        }
                    }
                }
            }
            last_version.into_values().map(|(_ver, schema)| schema).collect()
        }
    }
}

fn extract_authz_scope(
    metric_name: &str,
    authz_scope: AuthzScope,
    field_schema: &BTreeSet<FieldSchema>,
) -> Result<AuthzScope, MetricsError> {
    let check_for_key = |scope: &str| {
        let key = format!("{scope}_id");
        if field_schema.iter().any(|field| {
            field.name == key && field.field_type == FieldType::Uuid
        }) {
            Ok(())
        } else {
            Err(MetricsError::SchemaDefinition(format!(
                "Metric '{}' has '{}' authorization scope, and so must \
                contain a field '{}' of UUID type",
                metric_name, scope, key,
            )))
        }
    };
    match authz_scope {
        AuthzScope::Silo => check_for_key("silo")?,
        AuthzScope::Project => check_for_key("project")?,
        AuthzScope::Fleet | AuthzScope::ViewableToAll => {}
    }
    Ok(authz_scope)
}

fn construct_field_schema(
    all_fields: &BTreeMap<String, FieldMetadata>,
    target_name: &str,
    target_fields: &BTreeSet<String>,
    metric_name: &str,
    metric_field_names: &BTreeSet<String>,
) -> Result<BTreeSet<FieldSchema>, MetricsError> {
    if let Some(dup) = target_fields.intersection(&metric_field_names).next() {
        return Err(MetricsError::SchemaDefinition(format!(
            "Field '{}' is duplicated between target \
            '{}' and metric '{}'",
            dup, target_name, metric_name,
        )));
    }

    let mut field_schema = BTreeSet::new();
    for (field_name, source) in
        target_fields.iter().zip(std::iter::repeat(FieldSource::Target)).chain(
            metric_field_names
                .iter()
                .zip(std::iter::repeat(FieldSource::Metric)),
        )
    {
        let Some(metadata) = all_fields.get(field_name.as_str()) else {
            let (kind, name) = if source == FieldSource::Target {
                ("target", target_name)
            } else {
                ("metric", metric_name)
            };
            return Err(MetricsError::SchemaDefinition(format!(
                "Field '{}' is referenced in the {} '{}', but it \
                does not appear in the set of all fields.",
                field_name, kind, name,
            )));
        };
        validate_field_name(field_name, metadata)?;
        field_schema.insert(FieldSchema {
            name: field_name.to_string(),
            field_type: metadata.type_,
            source,
            description: metadata.description.clone(),
        });
    }
    Ok(field_schema)
}

fn is_snake_case(s: &str) -> bool {
    s == format!("{}", heck::AsSnakeCase(s))
}

fn is_valid_ident_name(s: &str) -> bool {
    syn::parse_str::<syn::Ident>(s).is_ok() && is_snake_case(s)
}

fn validate_field_name(
    field_name: &str,
    metadata: &FieldMetadata,
) -> Result<(), MetricsError> {
    if !is_valid_ident_name(field_name) {
        return Err(MetricsError::SchemaDefinition(format!(
            "Field name '{}' should be a valid identifier in snake_case",
            field_name,
        )));
    }
    if metadata.type_ == FieldType::Uuid
        && !(field_name.ends_with("_id") || field_name == "id")
    {
        return Err(MetricsError::SchemaDefinition(format!(
            "Uuid field '{}' should end with '_id' or equal 'id'",
            field_name,
        )));
    }
    Ok(())
}

fn extract_metric_fields<'a>(
    metric_name: &'a str,
    metric_fields: &'a MetricFields,
    current_version: &Option<CurrentVersion>,
) -> Result<CurrentVersion, MetricsError> {
    let new_version = match metric_fields {
        MetricFields::Removed { removed_in } => {
            match current_version {
                Some(CurrentVersion::Active { version, .. }) => {
                    // This metric was active, and is now being
                    // removed. Bump the version and mark it active,
                    // but there are no fields to return here.
                    if removed_in <= version {
                        return Err(MetricsError::SchemaDefinition(format!(
                            "Metric '{}' is removed in version \
                            {}, which is not strictly after the \
                            current active version, {}",
                            metric_name, removed_in, version,
                        )));
                    }
                    CurrentVersion::Inactive { removed_in: *removed_in }
                }
                Some(CurrentVersion::Inactive { removed_in }) => {
                    return Err(MetricsError::SchemaDefinition(format!(
                        "Metric '{}' was already removed in \
                        version {}, it cannot be removed again",
                        metric_name, removed_in,
                    )));
                }
                None => {
                    return Err(MetricsError::SchemaDefinition(format!(
                        "Metric {} has no previous version, \
                        it cannot be removed.",
                        metric_name,
                    )));
                }
            }
        }
        MetricFields::Added { added_in, fields } => {
            match current_version {
                Some(CurrentVersion::Active { .. }) => {
                    return Err(MetricsError::SchemaDefinition(format!(
                        "Metric '{}' is already active, it \
                        cannot be added again until it is removed.",
                        metric_name,
                    )));
                }
                Some(CurrentVersion::Inactive { removed_in }) => {
                    // The metric is currently inactive, just check
                    // that the newly-active version is greater.
                    if added_in <= removed_in {
                        return Err(MetricsError::SchemaDefinition(format!(
                            "Re-added metric '{}' must appear in a later \
                            version than the one in which it was removed ({})",
                            metric_name, removed_in,
                        )));
                    }
                    CurrentVersion::Active {
                        version: *added_in,
                        fields: to_unique_field_names(metric_name, fields)?,
                    }
                }
                None => {
                    // There was no previous version for this
                    // metric, just add it.
                    CurrentVersion::Active {
                        version: *added_in,
                        fields: to_unique_field_names(metric_name, fields)?,
                    }
                }
            }
        }
        MetricFields::Versioned(new_fields) => {
            match current_version {
                Some(CurrentVersion::Active { version, .. }) => {
                    // The happy-path, we're stepping the version
                    // and possibly modifying the fields.
                    if new_fields.version <= *version {
                        return Err(MetricsError::SchemaDefinition(format!(
                            "Metric '{}' version should increment, \
                            expected at least {}, found {}",
                            metric_name,
                            version.checked_add(1).expect("version < 256"),
                            new_fields.version,
                        )));
                    }
                    CurrentVersion::Active {
                        version: new_fields.version,
                        fields: to_unique_field_names(
                            metric_name,
                            &new_fields.fields,
                        )?,
                    }
                }
                Some(CurrentVersion::Inactive { removed_in }) => {
                    // The metric has been removed in the past, it
                    // needs to be added again first.
                    return Err(MetricsError::SchemaDefinition(format!(
                        "Metric '{}' was removed in version {}, \
                        it must be added again first",
                        metric_name, removed_in,
                    )));
                }
                None => {
                    // The metric never existed, it must be added
                    // first.
                    return Err(MetricsError::SchemaDefinition(format!(
                        "Metric '{}' must be added in at its first \
                        version, and can then be modified",
                        metric_name,
                    )));
                }
            }
        }
    };
    Ok(new_version)
}

fn to_unique_field_names(
    name: &str,
    fields: &[String],
) -> Result<BTreeSet<String>, MetricsError> {
    let set: BTreeSet<_> = fields.iter().cloned().collect();
    if set.len() != fields.len() {
        return Err(MetricsError::SchemaDefinition(format!(
            "Object '{name}' has duplicate fields"
        )));
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_authz_scope_requires_relevant_field() {
        assert!(
            extract_authz_scope("foo", AuthzScope::Project, &BTreeSet::new())
                .is_err(),
            "Project-scoped auth without a project_id field should be an error"
        );

        let schema = std::iter::once(FieldSchema {
            name: "project_id".to_string(),
            field_type: FieldType::Uuid,
            source: FieldSource::Target,
            description: String::new(),
        })
        .collect();
        extract_authz_scope("foo", AuthzScope::Project, &schema).expect(
            "Project-scoped auth with a project_id field should succeed",
        );

        let schema = std::iter::once(FieldSchema {
            name: "project_id".to_string(),
            field_type: FieldType::String,
            source: FieldSource::Target,
            description: String::new(),
        })
        .collect();
        assert!(
            extract_authz_scope("foo", AuthzScope::Project, &schema).is_err(),
            "Project-scoped auth with a project_id field \
            that's not a UUID should be an error",
        );
    }

    fn all_fields() -> BTreeMap<String, FieldMetadata> {
        let mut out = BTreeMap::new();
        for name in ["foo", "bar"] {
            out.insert(
                String::from(name),
                FieldMetadata {
                    type_: FieldType::U8,
                    description: String::new(),
                },
            );
        }
        out
    }

    #[test]
    fn construct_field_schema_fails_with_reference_to_unknown_field() {
        let all = all_fields();
        let target_fields = BTreeSet::new();
        let bad_name = String::from("baz");
        let metric_fields = std::iter::once(bad_name).collect();
        assert!(
            construct_field_schema(
                &all,
                "target",
                &target_fields,
                "metric",
                &metric_fields,
            )
            .is_err(),
            "Should return an error when the metric references a field \
            that doesn't exist in the global field list"
        );
    }

    #[test]
    fn construct_field_schema_fails_with_duplicate_field_names() {
        let all = all_fields();
        let name = String::from("bar");
        let target_fields = std::iter::once(name.clone()).collect();
        let metric_fields = std::iter::once(name).collect();
        assert!(construct_field_schema(
                &all,
                "target",
                &target_fields,
                "metric",
                &metric_fields,
            ).is_err(),
            "Should return an error when the target and metric share a field name"
        );
    }

    #[test]
    fn construct_field_schema_picks_up_correct_fields() {
        let all = all_fields();
        let all_schema = all
            .iter()
            .zip([FieldSource::Metric, FieldSource::Target])
            .map(|((name, md), source)| FieldSchema {
                name: name.clone(),
                field_type: md.type_,
                source,
                description: String::new(),
            })
            .collect();
        let foo = String::from("foo");
        let target_fields = std::iter::once(foo).collect();
        let bar = String::from("bar");
        let metric_fields = std::iter::once(bar).collect();
        assert_eq!(
            construct_field_schema(
                &all,
                "target",
                &target_fields,
                "metric",
                &metric_fields,
            )
            .unwrap(),
            all_schema,
            "Each field is referenced exactly once, so we should return \
            the entire input set of fields"
        );
    }

    #[test]
    fn validate_field_name_disallows_bad_names() {
        for name in ["PascalCase", "with spaces", "12345", "💖"] {
            assert!(
                validate_field_name(
                    name,
                    &FieldMetadata {
                        type_: FieldType::U8,
                        description: String::new()
                    }
                )
                .is_err(),
                "Field named {name} should be invalid"
            );
        }
    }

    #[test]
    fn validate_field_name_verifies_uuid_field_names() {
        assert!(
            validate_field_name(
                "projectid",
                &FieldMetadata {
                    type_: FieldType::Uuid,
                    description: String::new()
                }
            )
            .is_err(),
            "A Uuid field should be required to end in `_id`",
        );
        for name in ["project_id", "id"] {
            assert!(
                validate_field_name(name,
                        &FieldMetadata {
                            type_: FieldType::Uuid,
                            description: String::new()
                        }
                ).is_ok(),
                "A Uuid field should be required to end in '_id' or exactly 'id'",
            );
        }
    }

    #[test]
    fn extract_metric_fields_succeeds_with_gaps_in_versions() {
        let metric_fields = MetricFields::Versioned(VersionedFields {
            version: NonZeroU8::new(10).unwrap(),
            fields: vec![],
        });
        let current_version = Some(CurrentVersion::Active {
            version: NonZeroU8::new(1).unwrap(),
            fields: BTreeSet::new(),
        });
        extract_metric_fields("foo", &metric_fields, &current_version).expect(
            "Extracting metric fields should work with non-sequential \
            but increasing version numbers",
        );
    }

    #[test]
    fn extract_metric_fields_fails_with_non_increasing_versions() {
        let metric_fields = MetricFields::Versioned(VersionedFields {
            version: NonZeroU8::new(1).unwrap(),
            fields: vec![],
        });
        let current_version = Some(CurrentVersion::Active {
            version: NonZeroU8::new(1).unwrap(),
            fields: BTreeSet::new(),
        });
        let res =
            extract_metric_fields("foo", &metric_fields, &current_version);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!("Expected schema definition error, found: {res:#?}");
        };
        assert!(
            msg.contains("should increment"),
            "Should fail when version numbers are non-increasing",
        );
    }

    #[test]
    fn extract_metric_fields_requires_adding_first() {
        let metric_fields = MetricFields::Versioned(VersionedFields {
            version: NonZeroU8::new(1).unwrap(),
            fields: vec![],
        });
        let current_version = None;
        let res =
            extract_metric_fields("foo", &metric_fields, &current_version);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!("Expected schema definition error, found: {res:#?}");
        };
        assert!(
            msg.contains("must be added in at its first version"),
            "Should require that the first version of a metric explicitly \
            adds it in, before modification",
        );

        let metric_fields = MetricFields::Added {
            added_in: NonZeroU8::new(1).unwrap(),
            fields: vec![],
        };
        let current_version = None;
        extract_metric_fields("foo", &metric_fields, &current_version).expect(
            "Should succeed when fields are added_in for their first version",
        );
    }

    #[test]
    fn extract_metric_fields_fails_to_add_existing_metric() {
        let metric_fields = MetricFields::Added {
            added_in: NonZeroU8::new(2).unwrap(),
            fields: vec![],
        };
        let current_version = Some(CurrentVersion::Active {
            version: NonZeroU8::new(1).unwrap(),
            fields: BTreeSet::new(),
        });
        let res =
            extract_metric_fields("foo", &metric_fields, &current_version);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!("Expected schema definition error, found: {res:#?}");
        };
        assert!(
            msg.contains("is already active"),
            "Should fail when adding a metric that's already active",
        );
    }

    #[test]
    fn extract_metric_fields_fails_to_remove_non_existent_metric() {
        let metric_fields =
            MetricFields::Removed { removed_in: NonZeroU8::new(3).unwrap() };
        let current_version = Some(CurrentVersion::Inactive {
            removed_in: NonZeroU8::new(1).unwrap(),
        });
        let res =
            extract_metric_fields("foo", &metric_fields, &current_version);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!("Expected schema definition error, found: {res:#?}");
        };
        assert!(
            msg.contains("was already removed"),
            "Should fail when removing a metric that's already gone",
        );
    }

    #[test]
    fn load_schema_catches_metric_versions_not_added_in() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] }
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { version = 1, fields = [] }
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let res = load_schema(contents);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!("Should fail when metrics are not added in, but found: {res:#?}");
        };
        assert!(
            msg.contains("must be added in at its first"),
            "Error message should indicate that metrics need to be \
            added_in first, then modified"
        );
    }

    #[test]
    fn into_schema_list_fails_with_zero_metrics() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] }
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [] }
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let mut def: TimeseriesDefinition = toml::from_str(contents).unwrap();
        def.metrics.clear();
        let res = def.into_schema_list();
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!("Should fail with zero metrics, but found: {res:#?}");
        };
        assert!(
            msg.contains("At least one metric must"),
            "Error message should indicate that metrics need to be \
            added_in first, then modified"
        );
    }

    #[test]
    fn load_schema_fails_with_nonexistent_target_version() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [] },
            { version = 2, fields = [] }
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let res = load_schema(contents);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!(
                "Should fail when a metric version refers \
                to a non-existent target version, but found: {res:#?}",
            );
        };
        assert!(
            msg.contains("does not have a matching version in the target"),
            "Error message should indicate that the metric \
            refers to a nonexistent version in the target, found: {msg}",
        );
    }

    fn assert_sequential_versions(
        first: &TimeseriesSchema,
        second: &TimeseriesSchema,
    ) {
        assert_eq!(first.timeseries_name, second.timeseries_name);
        assert_eq!(
            first.version.get(),
            second.version.get().checked_sub(1).unwrap()
        );
        assert_eq!(first.datum_type, second.datum_type);
        assert_eq!(first.field_schema, second.field_schema);
    }

    #[test]
    fn load_schema_fills_in_late_implied_metric_versions() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
            { version = 2, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [] }
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let schema = load_schema(contents).unwrap();
        assert_eq!(
            schema.len(),
            2,
            "Should have filled in version 2 of the metric using the \
            corresponding target version",
        );
        assert_sequential_versions(&schema[0], &schema[1]);
    }

    #[test]
    fn load_schema_fills_in_implied_metric_versions_when_last_is_modified() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
            { version = 2, fields = [ "foo" ] },
            { version = 3, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [] },
            { version = 3, fields = [] },
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let schema = load_schema(contents).unwrap();
        assert_eq!(
            schema.len(),
            3,
            "Should have filled in version 2 of the metric using the \
            corresponding target version",
        );
        assert_sequential_versions(&schema[0], &schema[1]);
        assert_sequential_versions(&schema[1], &schema[2]);
    }

    #[test]
    fn load_schema_fills_in_implied_metric_versions() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
            { version = 2, fields = [ "foo" ] },
            { version = 3, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [] },
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let schema = load_schema(contents).unwrap();
        assert_eq!(
            schema.len(),
            3,
            "Should have filled in versions 2 and 3 of the metric using the \
            corresponding target version",
        );
        assert_sequential_versions(&schema[0], &schema[1]);
        assert_sequential_versions(&schema[1], &schema[2]);
    }

    #[test]
    fn load_schema_fills_in_implied_metric_versions_when_last_version_is_removed(
    ) {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
            { version = 2, fields = [ "foo" ] },
            { version = 3, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [] },
            { removed_in = 3 },
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let schema = load_schema(contents).unwrap();
        dbg!(&schema);
        assert_eq!(
            schema.len(),
            2,
            "Should have filled in version 2 of the metric using the \
            corresponding target version",
        );
        assert_sequential_versions(&schema[0], &schema[1]);
    }

    #[test]
    fn load_schema_skips_versions_until_metric_is_added() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
            { version = 2, fields = [ "foo" ] },
            { version = 3, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 3, fields = [] },
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let schema = load_schema(contents).unwrap();
        assert_eq!(
            schema.len(),
            1,
            "Should have only created the last version of the timeseries"
        );
    }

    #[test]
    fn load_schema_fails_with_duplicate_timeseries() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [] },
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let res = load_schema(contents);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!(
                "Expected to fail with duplicated timeseries, but found {res:#?}",
            );
        };
        assert!(
            msg.ends_with("is duplicated"),
            "Message should indicate that a timeseries name / \
            version is duplicated"
        );
    }

    #[test]
    fn only_support_format_version_1() {
        let contents = r#"
        format_version = 2

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [] },
        ]
        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let res = load_schema(contents);
        let Err(MetricsError::Toml(msg)) = &res else {
            panic!(
                "Expected to fail with bad format version, but found {res:#?}",
            );
        };
        assert!(
            msg.contains("Only version 1 of"),
            "Message should indicate that only format version 1 \
            is supported, but found {msg:?}"
        );
    }

    #[test]
    fn ensures_target_has_at_least_one_field() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [ "foo" ] },
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let res = load_schema(contents);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!(
                "Expected to fail when target has zero fields, but found {res:#?}",
            );
        };
        assert_eq!(
            msg, "Target 'target' version 1 must have at least one field",
            "Message should indicate that all targets must \
            have at least one field, but found {msg:?}",
        );
    }

    #[test]
    fn fail_on_very_long_timeseries_name() {
        let contents = r#"
        format_version = 1

        [target]
        name = "veeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeery_long_target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "veeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeery_long_metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [ ] },
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let res = load_schema(contents);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!(
                "Expected to fail when timeseries name is too long, found {res:#?}"
            );
        };
        assert!(
            msg.contains("exceeds the maximum"),
            "Message should complain about a long timeseries name, but found {msg:?}",
        );
    }

    #[test]
    fn fail_on_empty_metric_name() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = ""
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [ ] },
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let res = load_schema(contents);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!(
                "Expected to fail when metric name is empty, found {res:#?}"
            );
        };
        assert_eq!(
            msg, "Metric name cannot be empty",
            "Message should complain about an empty metric name \
            but found {msg:?}",
        );
    }

    #[test]
    fn fail_on_empty_target_name() {
        let contents = r#"
        format_version = 1

        [target]
        name = ""
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [ ] },
        ]

        [fields.foo]
        type = "string"
        description = "a field"
        "#;
        let res = load_schema(contents);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!(
                "Expected to fail when target name is empty, found {res:#?}"
            );
        };
        assert_eq!(
            msg, "Target name cannot be empty",
            "Message should complain about an empty target name \
            but found {msg:?}",
        );
    }

    #[test]
    fn fail_on_very_long_field_names() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [ ] },
        ]

        [fields.this_is_a_reeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeealy_long_field_name]
        type = "string"
        description = "a field"
        "#;
        let res = load_schema(contents);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!(
                "Expected to fail when field name is too long, found {res:#?}"
            );
        };
        assert!(
            msg.contains("which exceeds the maximum field name length"),
            "Message should complain about a field name being \
            too long, but found {msg:?}",
        );
    }

    #[test]
    fn fail_on_empty_descriptions() {
        let contents = r#"
        format_version = 1

        [target]
        name = "target"
        description = "some target"
        authz_scope = "fleet"
        versions = [
            { version = 1, fields = [ "foo" ] },
        ]

        [[metrics]]
        name = "metric"
        description = "some metric"
        datum_type = "u8"
        units = "count"
        versions = [
            { added_in = 1, fields = [ ] },
        ]

        [fields.foo]
        type = "string"
        description = ""
        "#;
        let res = load_schema(contents);
        let Err(MetricsError::SchemaDefinition(msg)) = &res else {
            panic!(
                "Expected to fail when field description is empty, found {res:#?}"
            );
        };
        assert_eq!(
            msg, "Description of field 'foo' cannot be empty",
            "Message should complain about a field description being \
            empty, but found {msg:?}",
        );
    }
}
