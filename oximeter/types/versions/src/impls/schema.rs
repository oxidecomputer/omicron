// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for the schema-related types.

use crate::latest::schema::AuthzScope;
use crate::latest::schema::FieldSchema;
use crate::latest::schema::FieldSource;
use crate::latest::schema::TIMESERIES_NAME_REGEX;
use crate::latest::schema::TimeseriesName;
use crate::latest::schema::TimeseriesSchema;
use crate::latest::schema::Units;
use crate::latest::traits::Metric;
use crate::latest::traits::Target;
use crate::latest::types::MetricsError;
use crate::latest::types::Sample;
use crate::latest::schema::default_schema_version;
use chrono::Utc;
use std::collections::BTreeSet;

impl From<MetricsError> for omicron_common::api::external::Error {
    fn from(e: MetricsError) -> Self {
        omicron_common::api::external::Error::internal_error(&e.to_string())
    }
}

impl FieldSchema {
    /// Return `true` if this field is copyable.
    pub const fn is_copyable(&self) -> bool {
        self.field_type.is_copyable()
    }
}

impl std::ops::Deref for TimeseriesName {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<TimeseriesName> for String {
    fn from(n: TimeseriesName) -> Self {
        n.0
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
        let timeseries_name = crate::impls::timeseries_name(target, metric)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::latest::types::FieldType;
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
        assert!(TimeseriesName::try_from("x.a:b").is_err());
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
