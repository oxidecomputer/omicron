// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::AlertClass;
use crate::AlertClassParseError;
use crate::SemverVersion;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{alert_glob, alert_subscription};
use nexus_types::external_api::shared;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{AlertReceiverKind, AlertReceiverUuid};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(
    Clone, Debug, Queryable, Selectable, Insertable, Serialize, Deserialize,
)]
#[diesel(table_name = alert_subscription)]
pub struct AlertRxSubscription {
    pub rx_id: DbTypedUuid<AlertReceiverKind>,
    #[diesel(column_name = alert_class)]
    pub class: AlertClass,
    pub glob: Option<String>,
    pub time_created: DateTime<Utc>,
}

#[derive(
    Clone, Debug, Queryable, Selectable, Insertable, Serialize, Deserialize,
)]
#[diesel(table_name = alert_glob)]
pub struct AlertRxGlob {
    pub rx_id: DbTypedUuid<AlertReceiverKind>,
    #[diesel(embed)]
    pub glob: AlertGlob,
    pub time_created: DateTime<Utc>,
    pub schema_version: Option<SemverVersion>,
}

impl AlertRxGlob {
    pub fn new(rx_id: AlertReceiverUuid, glob: AlertGlob) -> Self {
        Self {
            rx_id: DbTypedUuid(rx_id),
            glob,
            time_created: Utc::now(),
            // When inserting a new glob, set the schema version to NULL,
            // indicating that the glob will need to be processed before alerts
            // can be dispatched.
            schema_version: None,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AlertSubscriptionKind {
    Glob(AlertGlob),
    Exact(AlertClass),
}

impl AlertSubscriptionKind {
    pub fn new(value: String) -> Result<Self, Error> {
        if value.is_empty() {
            return Err(Error::invalid_value(
                "alert_class",
                "must not be empty",
            ));
        }
        if value.contains(char::is_whitespace) {
            return Err(Error::invalid_value(
                "alert_class",
                format!(
                    "invalid alert class {value:?}: alert classes do not \
                     contain whitespace",
                ),
            ));
        }

        if value.contains('*') {
            let regex = AlertGlob::regex_from_glob(&value)?;
            return Ok(Self::Glob(AlertGlob { regex, glob: value }));
        }

        let class = value.parse().map_err(|e: AlertClassParseError| {
            Error::invalid_value("alert_class", e.to_string())
        })?;

        if class == AlertClass::Probe {
            return Err(Error::invalid_value(
                "alert_class",
                "webhook receivers cannot subscribe to probes",
            ));
        }

        Ok(Self::Exact(class))
    }
}

impl TryFrom<AlertSubscriptionKind> for shared::AlertSubscription {
    type Error = Error;
    fn try_from(kind: AlertSubscriptionKind) -> Result<Self, Self::Error> {
        match kind {
            AlertSubscriptionKind::Exact(class) => class.as_str().parse(),
            AlertSubscriptionKind::Glob(AlertGlob { glob, .. }) => {
                glob.try_into()
            }
        }
        .map_err(|e: anyhow::Error| {
            // This is an internal error because any subscription string stored
            // in the database should already have been validated.
            Error::InternalError { internal_message: e.to_string() }
        })
    }
}

impl TryFrom<shared::AlertSubscription> for AlertSubscriptionKind {
    type Error = Error;
    fn try_from(
        subscription: shared::AlertSubscription,
    ) -> Result<Self, Self::Error> {
        Self::new(String::from(subscription))
    }
}

impl fmt::Display for AlertSubscriptionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact(class) => class.fmt(f),
            Self::Glob(glob) => glob.glob.fmt(f),
        }
    }
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Hash,
    Queryable,
    Selectable,
    Insertable,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = alert_glob)]
pub struct AlertGlob {
    pub glob: String,
    pub regex: String,
}

impl FromStr for AlertGlob {
    type Err = Error;
    fn from_str(glob: &str) -> Result<Self, Self::Err> {
        let regex = Self::regex_from_glob(glob)?;
        Ok(Self { glob: glob.to_string(), regex })
    }
}

impl TryFrom<String> for AlertGlob {
    type Error = Error;
    fn try_from(glob: String) -> Result<Self, Self::Error> {
        let regex = Self::regex_from_glob(&glob)?;
        Ok(Self { glob, regex })
    }
}

impl AlertGlob {
    fn regex_from_glob(glob: &str) -> Result<String, Error> {
        let seg2regex =
            |segment: &str, regex: &mut String| -> Result<(), Error> {
                match segment {
                    // Match one segment (i.e. any number of segment characters)
                    "*" => regex.push_str("[^\\.]+"),
                    // Match any number of segments
                    "**" => regex.push_str(".+"),
                    "" => {
                        return Err(Error::invalid_value(
                            "alert_class",
                            format!(
                                "invalid alert class {glob:?}: dot-delimited \
                                 alert class segments must not be empty"
                            ),
                        ));
                    }
                    s if s.contains('*') => {
                        return Err(Error::invalid_value(
                            "alert_class",
                            format!(
                                "invalid alert class {glob:?}: all segments \
                                 must be either '*', '**', or any sequence of \
                                 non-'*' alphanumeric characters",
                            ),
                        ));
                    }
                    // Match the literal segment.
                    s => regex.push_str(s),
                }
                Ok(())
            };

        // The subscription's regex will always be at least as long as the alert
        // class glob, plus start and end anchors.
        let mut regex = String::with_capacity(glob.len());

        regex.push('^'); // Start anchor
        let mut segments = glob.split('.');
        if let Some(segment) = segments.next() {
            seg2regex(segment, &mut regex)?;
            for segment in segments {
                regex.push_str("\\."); // segment separator
                seg2regex(segment, &mut regex)?;
            }
        } else {
            return Err(Error::invalid_value(
                "alert_class",
                "alert class strings must not be empty",
            ));
        };
        regex.push('$'); // End anchor

        Ok(regex)
    }
}

impl AlertRxSubscription {
    pub fn exact(rx_id: AlertReceiverUuid, class: AlertClass) -> Self {
        Self {
            rx_id: DbTypedUuid(rx_id),
            class,
            glob: None,
            time_created: Utc::now(),
        }
    }

    pub fn for_glob(glob: &AlertRxGlob, class: AlertClass) -> Self {
        Self {
            rx_id: glob.rx_id,
            glob: Some(glob.glob.glob.clone()),
            class,
            time_created: Utc::now(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const GLOB_CASES: &[(&str, &str)] = &[
        ("foo.*.bar", "^foo\\.[^\\.]+\\.bar$"),
        ("foo.*", "^foo\\.[^\\.]+$"),
        ("*.foo", "^[^\\.]+\\.foo$"),
        ("foo.**.bar", "^foo\\..+\\.bar$"),
        ("foo.**", "^foo\\..+$"),
        ("foo_bar.*.baz", "^foo_bar\\.[^\\.]+\\.baz$"),
    ];

    #[test]
    fn test_alert_class_glob_to_regex() {
        const NON_GLOB_CASES: &[(&str, &str)] =
            &[("foo.bar", "^foo\\.bar$"), ("foo_bar.baz", "^foo_bar\\.baz$")];
        for (class, regex) in GLOB_CASES.iter().chain(NON_GLOB_CASES.iter()) {
            let glob = match AlertGlob::from_str(dbg!(class)) {
                Ok(glob) => glob,
                Err(error) => panic!(
                    "alert class glob {class:?} should produce the regex
                     {regex:?}, but instead failed to parse: {error}"
                ),
            };
            assert_eq!(
                dbg!(regex),
                dbg!(&glob.regex),
                "alert class {class:?} should produce the regex {regex:?}"
            );
        }
    }

    #[test]
    fn test_valid_subscription_parsing() {
        const EXACT_CASES: &[&str] =
            &["test.foo", "test.foo.bar", "test.foo.baz"];
        for input in EXACT_CASES {
            let parsed = AlertSubscriptionKind::new(dbg!(input).to_string());

            match dbg!(parsed) {
                Ok(AlertSubscriptionKind::Exact(exact)) => {
                    assert_eq!(exact.as_str(), *input)
                }
                Ok(AlertSubscriptionKind::Glob(glob)) => panic!(
                    "expected {input:?} to be an exact subscription, but it \
                     parsed as glob {glob:?}",
                ),
                Err(e) => panic!(
                    "expected {input:?} to be a valid alert class, but it \
                     failed to parse: {e}"
                ),
            }
        }

        for (input, _) in GLOB_CASES {
            let parsed = AlertSubscriptionKind::new(dbg!(input).to_string());

            match dbg!(parsed) {
                Ok(AlertSubscriptionKind::Exact(exact)) => {
                    panic!(
                        "expected {input:?} to be a glob subscription, but it \
                         parsed as an exact subscription {exact:?}",
                    );
                }
                Ok(AlertSubscriptionKind::Glob(glob)) => {
                    match regex::Regex::new(&glob.regex) {
                        Ok(_) => {}
                        Err(e) => panic!(
                            "glob {glob:?} produced an invalid regex: {e}"
                        ),
                    }
                }
                Err(e) => panic!(
                    "expected {input:?} to be a valid alert class, but it \
                     failed to parse: {e}"
                ),
            }
        }
    }

    #[test]
    fn test_invalid_subscription_parsing() {
        const CASES: &[&str] = &[
            "foo..bar",
            ".foo.bar",
            "",
            "..",
            "foo.***",
            "*****",
            "foo.bar*.baz",
            "foo*",
            "foo bar.baz",
            " ",
            " .*",
        ];
        for input in CASES {
            match AlertSubscriptionKind::new(dbg!(input).to_string()) {
                Ok(glob) => panic!(
                    "invalid alert class {input:?} was parsed \
                     successfully as {glob:?}"
                ),
                Err(error) => {
                    dbg!(error);
                }
            }
        }
    }
}
