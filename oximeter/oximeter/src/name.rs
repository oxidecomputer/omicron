// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for identifying timeseries

// Copyright 2023 Oxide Computer Company

use crate::Metric;
use crate::MetricsError;
use crate::Target;
use regex::Regex;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::num::NonZeroU8;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct VersionedName {
    name: String,
    version: NonZeroU8,
}

impl std::str::FromStr for VersionedName {
    type Err = MetricsError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !Regex::new(VERSIONED_COMPONENT_NAME_REGEX).unwrap().is_match(s) {
            return Err(MetricsError::InvalidTimeseriesName);
        }
        let Some((name, version)) = s.split_once('@') else {
            return Ok(Self { name: s.to_string(), version: NonZeroU8::MIN });
        };
        let version =
            version.parse().map_err(|_| MetricsError::InvalidTimeseriesName)?;
        Ok(Self { name: name.to_string(), version })
    }
}

impl std::fmt::Display for VersionedName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}@{}", self.name, self.version)
    }
}

/// A timeseries name.
///
/// Timeseries are named by concatenating the names of their target and metric,
/// joined with a colon. Names are also _versioned_, independently for the
/// target and metric, with a non-zero version number. The entire name may be
/// written as `target_name[@target_version]:metric_name[@metric_version]`. The
/// version for each component may be omitted, in which case it defaults to 1.
#[derive(
    Debug, Clone, PartialEq, PartialOrd, Ord, Eq, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "&str", into = "String")]
pub struct TimeseriesName {
    target: VersionedName,
    metric: VersionedName,
}

impl TimeseriesName {
    /// Construct the timeseries name from a Target and Metric.
    pub fn new<T, M>(target: &T, metric: &M) -> Self
    where
        T: Target,
        M: Metric,
    {
        let target = VersionedName {
            name: target.name().to_string(),
            version: target.version(),
        };
        let metric = VersionedName {
            name: metric.name().to_string(),
            version: metric.version(),
        };
        Self { target, metric }
    }

    /// Construct a name from the unversioned timeseries name and its component
    /// versions.
    ///
    /// Note that this does not ensure that there are _no_ versions in the
    /// string name, only that they are either not there or the default of 1. In
    /// either case, they will be overwritten by the versions provided.
    //
    // NOTE: This is really here to support reading this out of ClickHouse.
    pub fn from_name_and_versions(
        unversioned_name: &str,
        target_version: NonZeroU8,
        metric_version: NonZeroU8,
    ) -> Result<Self, MetricsError> {
        let name: TimeseriesName = unversioned_name.parse()?;
        if name.target_version() != NonZeroU8::MIN
            || name.metric_version() != NonZeroU8::MIN
        {
            return Err(MetricsError::InvalidTimeseriesName);
        }
        Ok(Self {
            target: VersionedName {
                name: name.target_name().to_string(),
                version: target_version,
            },
            metric: VersionedName {
                name: name.metric_name().to_string(),
                version: metric_version,
            },
        })
    }

    /// Return the target name.
    pub fn target_name(&self) -> &str {
        &self.target.name
    }

    /// Return the target version.
    pub fn target_version(&self) -> NonZeroU8 {
        self.target.version
    }

    /// Return the metric name.
    pub fn metric_name(&self) -> &str {
        &self.metric.name
    }

    /// Return the metric version.
    pub fn metric_version(&self) -> NonZeroU8 {
        self.metric.version
    }

    /// Return the component names of the target and metric.
    pub fn component_names(&self) -> (&str, &str) {
        (&self.target.name, &self.metric.name)
    }

    /// Return the component versions of the target and metric.
    pub fn component_versions(&self) -> (NonZeroU8, NonZeroU8) {
        (self.target.version, self.metric.version)
    }
}

impl std::fmt::Display for TimeseriesName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}:{}", self.target, self.metric)
    }
}

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
                     with '_' separating words. Each may be followed by \
                     an optional @ and non-zero version number."
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

impl std::str::FromStr for TimeseriesName {
    type Err = MetricsError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        verify_matches_regex(s)?;
        let (target_component, metric_component) = s.split_once(':').unwrap();
        let target = target_component.parse()?;
        let metric = metric_component.parse()?;
        Ok(Self { target, metric })
    }
}

impl std::convert::TryFrom<&str> for TimeseriesName {
    type Error = MetricsError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl std::convert::TryFrom<&String> for TimeseriesName {
    type Error = MetricsError;
    fn try_from(s: &String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl std::convert::TryFrom<String> for TimeseriesName {
    type Error = MetricsError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl From<TimeseriesName> for String {
    fn from(n: TimeseriesName) -> String {
        n.to_string()
    }
}

impl<T> PartialEq<T> for TimeseriesName
where
    T: AsRef<str>,
{
    fn eq(&self, other: &T) -> bool {
        let Ok(other) = other.as_ref().parse::<Self>() else {
            return false;
        };
        self == &other
    }
}

fn verify_matches_regex(s: &str) -> Result<&str, MetricsError> {
    if regex::Regex::new(TIMESERIES_NAME_REGEX).unwrap().is_match(s) {
        Ok(s)
    } else {
        Err(MetricsError::InvalidTimeseriesName)
    }
}

// Regular expression describing valid timeseries names.
//
// Names are derived from the names of the Rust structs for the target and metric, converted to
// snake case. So the names must be valid identifiers, and generally:
//
//  - Start with lowercase a-z
//  - Any number of alphanumerics
//  - Zero or more of the above, delimited by '_'.
//
// That describes the target/metric name, and the timeseries is two of those, joined with ':'.
const TIMESERIES_NAME_REGEX: &str = const_format::formatcp!(
    "({}):({})",
    VERSIONED_COMPONENT_NAME_REGEX,
    VERSIONED_COMPONENT_NAME_REGEX,
);

// The regex for a single component name.
const COMPONENT_NAME_REGEX: &str = "([a-z]+[a-z0-9]*)(_([a-z0-9]+))*";

// The regex for a single versioned component name.
const VERSIONED_COMPONENT_NAME_REGEX: &str = const_format::formatcp!(
    "{}(@(([1-9])|([1-9][0-9])|(1[0-9][0-9])|(2[0-1][0-9])|(22[0-4])))?",
    COMPONENT_NAME_REGEX,
);

#[cfg(test)]
mod tests {
    use super::TimeseriesName;
    use super::VersionedName;
    use std::convert::TryFrom;
    use std::num::NonZeroU8;

    #[test]
    fn test_timeseries_name() {
        let name = TimeseriesName::try_from("foo:bar").unwrap();
        assert_eq!(format!("{}", name), "foo@1:bar@1");
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

        let expected = TimeseriesName {
            target: VersionedName { name: "a".into(), version: NonZeroU8::MIN },
            metric: VersionedName { name: "b".into(), version: NonZeroU8::MIN },
        };
        let parsed: TimeseriesName =
            "a@1:b@1".parse::<TimeseriesName>().unwrap();
        assert_eq!(expected, parsed);

        // Check we get the same when parsing without a version on either
        // component.
        let parsed: TimeseriesName = "a:b@1".parse::<TimeseriesName>().unwrap();
        assert_eq!(expected, parsed);
        let parsed: TimeseriesName = "a@1:b".parse::<TimeseriesName>().unwrap();
        assert_eq!(expected, parsed);
        let parsed: TimeseriesName = "a:b".parse::<TimeseriesName>().unwrap();
        assert_eq!(expected, parsed);

        // Check that parsing a version of zero or negative fails.
        assert!("a:b@".parse::<TimeseriesName>().is_err());
        assert!("a:b@0".parse::<TimeseriesName>().is_err());
        assert!("a:b@-1".parse::<TimeseriesName>().is_err());
        assert!("a:b@1.0".parse::<TimeseriesName>().is_err());
        assert!("a:b@10000".parse::<TimeseriesName>().is_err());

        let parsed: TimeseriesName = "a@2:b".parse::<TimeseriesName>().unwrap();
        assert_ne!(expected, parsed);
        let parsed: TimeseriesName =
            "a@2:b@2".parse::<TimeseriesName>().unwrap();
        assert_ne!(expected, parsed);
        let TimeseriesName {
            target: VersionedName { version: target_version, .. },
            metric: VersionedName { version: metric_version, .. },
        } = "a@2:b@2".parse().unwrap();
        assert_eq!(target_version.get(), 2);
        assert_eq!(metric_version.get(), 2);
    }

    #[test]
    fn test_timeseries_name_from_name_and_versions() {
        let expected = TimeseriesName {
            target: VersionedName {
                name: "a".into(),
                version: 2.try_into().unwrap(),
            },
            metric: VersionedName {
                name: "b".into(),
                version: 2.try_into().unwrap(),
            },
        };
        let other = TimeseriesName::from_name_and_versions(
            "a:b",
            expected.target_version(),
            expected.metric_version(),
        )
        .unwrap();
        assert_eq!(expected, other);

        let other = TimeseriesName::from_name_and_versions(
            "a@1:b",
            expected.target_version(),
            expected.metric_version(),
        )
        .unwrap();
        assert_eq!(expected, other);

        assert!(TimeseriesName::from_name_and_versions(
            "a@2:b",
            expected.target_version(),
            expected.metric_version(),
        )
        .is_err());
    }
}
