// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::EventClassParseError;
use crate::Generation;
use crate::Name;
use crate::SemverVersion;
use crate::WebhookEventClass;
use crate::collection::DatastoreCollectionConfig;
use crate::schema::{
    webhook_receiver, webhook_rx_event_glob, webhook_rx_subscription,
    webhook_secret,
};
use crate::schema_versions;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use db_macros::{Asset, Resource};
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{
    GenericUuid, WebhookReceiverKind, WebhookReceiverUuid, WebhookSecretUuid,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;

/// The full configuration of a webhook receiver, including the
/// [`WebhookReceiver`] itself and its subscriptions and secrets.
#[derive(Clone, Debug)]
pub struct WebhookReceiverConfig {
    pub rx: WebhookReceiver,
    pub secrets: Vec<WebhookSecret>,
    pub events: Vec<WebhookSubscriptionKind>,
}

impl TryFrom<WebhookReceiverConfig> for views::WebhookReceiver {
    type Error = Error;
    fn try_from(
        WebhookReceiverConfig { rx, secrets, events }: WebhookReceiverConfig,
    ) -> Result<views::WebhookReceiver, Self::Error> {
        let secrets = secrets
            .iter()
            .map(|WebhookSecret { identity, .. }| views::WebhookSecretId {
                id: identity.id.into_untyped_uuid(),
            })
            .collect();
        let events = events
            .into_iter()
            .map(WebhookSubscriptionKind::into_event_class_string)
            .collect();
        let endpoint =
            rx.endpoint.parse().map_err(|e| Error::InternalError {
                // This is an internal error, as we should not have ever allowed
                // an invalid URL to be inserted into the database...
                internal_message: format!(
                    "invalid webhook URL {:?}: {e}",
                    rx.endpoint,
                ),
            })?;
        Ok(views::WebhookReceiver {
            identity: rx.identity(),
            endpoint,
            secrets,
            events,
        })
    }
}

/// A row in the `webhook_receiver` table.
#[derive(
    Clone,
    Debug,
    Queryable,
    Selectable,
    Resource,
    Insertable,
    Serialize,
    Deserialize,
)]
#[resource(uuid_kind = WebhookReceiverKind)]
#[diesel(table_name = webhook_receiver)]
pub struct WebhookReceiver {
    #[diesel(embed)]
    pub identity: WebhookReceiverIdentity,
    pub endpoint: String,

    /// child resource generation number for secrets, per RFD 192
    pub secret_gen: Generation,
    /// child resource generation number for event subscriptions, per RFD 192
    pub subscription_gen: Generation,
}

// Note that while we have both a `secret_gen` and a `subscription_gen`, we only
// implement `DatastoreCollection` for secrets, not subscriptions. This is
// because subscriptions are updated in a batch, using a transaction, rather
// than via add and delete operations for individual IDs, like secrets.
impl DatastoreCollectionConfig<WebhookSecret> for WebhookReceiver {
    type CollectionId = Uuid;
    type GenerationNumberColumn = webhook_receiver::dsl::secret_gen;
    type CollectionTimeDeletedColumn = webhook_receiver::dsl::time_deleted;
    type CollectionIdColumn = webhook_secret::dsl::rx_id;
}

/// Describes a set of updates for the [`WebhookReceiver`] model.
#[derive(Clone, AsChangeset)]
#[diesel(table_name = webhook_receiver)]
pub struct WebhookReceiverUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub endpoint: Option<String>,
    pub time_modified: DateTime<Utc>,
    pub subscription_gen: Option<Generation>,
}

#[derive(
    Clone,
    Debug,
    Queryable,
    Selectable,
    Insertable,
    Serialize,
    Deserialize,
    Asset,
)]
#[asset(uuid_kind = WebhookSecretKind)]
#[diesel(table_name = webhook_secret)]
pub struct WebhookSecret {
    #[diesel(embed)]
    pub identity: WebhookSecretIdentity,
    #[diesel(column_name = rx_id)]
    pub webhook_receiver_id: DbTypedUuid<WebhookReceiverKind>,
    pub secret: String,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl WebhookSecret {
    pub fn new(rx_id: WebhookReceiverUuid, secret: String) -> Self {
        Self {
            identity: WebhookSecretIdentity::new(WebhookSecretUuid::new_v4()),
            webhook_receiver_id: rx_id.into(),
            secret,
            time_deleted: None,
        }
    }
}

impl From<WebhookSecret> for views::WebhookSecretId {
    fn from(secret: WebhookSecret) -> Self {
        Self { id: secret.identity.id.into_untyped_uuid() }
    }
}

#[derive(
    Clone, Debug, Queryable, Selectable, Insertable, Serialize, Deserialize,
)]
#[diesel(table_name = webhook_rx_subscription)]
pub struct WebhookRxSubscription {
    pub rx_id: DbTypedUuid<WebhookReceiverKind>,
    pub event_class: WebhookEventClass,
    pub glob: Option<String>,
    pub time_created: DateTime<Utc>,
}

#[derive(
    Clone, Debug, Queryable, Selectable, Insertable, Serialize, Deserialize,
)]
#[diesel(table_name = webhook_rx_event_glob)]
pub struct WebhookRxEventGlob {
    pub rx_id: DbTypedUuid<WebhookReceiverKind>,
    #[diesel(embed)]
    pub glob: WebhookGlob,
    pub time_created: DateTime<Utc>,
    pub schema_version: SemverVersion,
}

impl WebhookRxEventGlob {
    pub fn new(rx_id: WebhookReceiverUuid, glob: WebhookGlob) -> Self {
        Self {
            rx_id: DbTypedUuid(rx_id),
            glob,
            time_created: Utc::now(),
            schema_version: schema_versions::SCHEMA_VERSION.into(),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum WebhookSubscriptionKind {
    Glob(WebhookGlob),
    Exact(WebhookEventClass),
}

impl WebhookSubscriptionKind {
    pub fn new(value: String) -> Result<Self, Error> {
        if value.is_empty() {
            return Err(Error::invalid_value(
                "event_class",
                "must not be empty",
            ));
        }
        if value.contains(char::is_whitespace) {
            return Err(Error::invalid_value(
                "event_class",
                format!(
                    "invalid event class {value:?}: event classes do not \
                     contain whitespace",
                ),
            ));
        }

        if value.contains('*') {
            let regex = WebhookGlob::regex_from_glob(&value)?;
            return Ok(Self::Glob(WebhookGlob { regex, glob: value }));
        }

        let class = value.parse().map_err(|e: EventClassParseError| {
            Error::invalid_value("event_class", e.to_string())
        })?;

        if class == WebhookEventClass::Probe {
            return Err(Error::invalid_value(
                "event_class",
                "webhook receivers cannot subscribe to probes",
            ));
        }

        Ok(Self::Exact(class))
    }

    fn into_event_class_string(self) -> String {
        match self {
            Self::Exact(class) => class.to_string(),
            Self::Glob(WebhookGlob { glob, .. }) => glob,
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
#[diesel(table_name = webhook_rx_event_glob)]
pub struct WebhookGlob {
    pub glob: String,
    pub regex: String,
}

impl FromStr for WebhookGlob {
    type Err = Error;
    fn from_str(glob: &str) -> Result<Self, Self::Err> {
        let regex = Self::regex_from_glob(glob)?;
        Ok(Self { glob: glob.to_string(), regex })
    }
}

impl TryFrom<String> for WebhookGlob {
    type Error = Error;
    fn try_from(glob: String) -> Result<Self, Self::Error> {
        let regex = Self::regex_from_glob(&glob)?;
        Ok(Self { glob, regex })
    }
}

impl WebhookGlob {
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
                            "event_class",
                            format!(
                                "invalid event class {glob:?}: dot-delimited \
                                 event class segments must not be empty"
                            ),
                        ));
                    }
                    s if s.contains('*') => {
                        return Err(Error::invalid_value(
                            "event_class",
                            format!(
                                "invalid event class {glob:?}: all segments \
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

        // The subscription's regex will always be at least as long as the event
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
                "event_class",
                "event class strings must not be empty",
            ));
        };
        regex.push('$'); // End anchor

        Ok(regex)
    }
}

impl WebhookRxSubscription {
    pub fn exact(
        rx_id: WebhookReceiverUuid,
        event_class: WebhookEventClass,
    ) -> Self {
        Self {
            rx_id: DbTypedUuid(rx_id),
            event_class,
            glob: None,
            time_created: Utc::now(),
        }
    }

    pub fn for_glob(
        glob: &WebhookRxEventGlob,
        event_class: WebhookEventClass,
    ) -> Self {
        Self {
            rx_id: glob.rx_id,
            glob: Some(glob.glob.glob.clone()),
            event_class,
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
    fn test_event_class_glob_to_regex() {
        const NON_GLOB_CASES: &[(&str, &str)] =
            &[("foo.bar", "^foo\\.bar$"), ("foo_bar.baz", "^foo_bar\\.baz$")];
        for (class, regex) in GLOB_CASES.iter().chain(NON_GLOB_CASES.iter()) {
            let glob = match WebhookGlob::from_str(dbg!(class)) {
                Ok(glob) => glob,
                Err(error) => panic!(
                    "event class glob {class:?} should produce the regex
                     {regex:?}, but instead failed to parse: {error}"
                ),
            };
            assert_eq!(
                dbg!(regex),
                dbg!(&glob.regex),
                "event class {class:?} should produce the regex {regex:?}"
            );
        }
    }

    #[test]
    fn test_valid_subscription_parsing() {
        const EXACT_CASES: &[&str] =
            &["test.foo", "test.foo.bar", "test.foo.baz"];
        for input in EXACT_CASES {
            let parsed = WebhookSubscriptionKind::new(dbg!(input).to_string());

            match dbg!(parsed) {
                Ok(WebhookSubscriptionKind::Exact(exact)) => {
                    assert_eq!(exact.as_str(), *input)
                }
                Ok(WebhookSubscriptionKind::Glob(glob)) => panic!(
                    "expected {input:?} to be an exact subscription, but it \
                     parsed as glob {glob:?}",
                ),
                Err(e) => panic!(
                    "expected {input:?} to be a valid event class, but it \
                     failed to parse: {e}"
                ),
            }
        }

        for (input, _) in GLOB_CASES {
            let parsed = WebhookSubscriptionKind::new(dbg!(input).to_string());

            match dbg!(parsed) {
                Ok(WebhookSubscriptionKind::Exact(exact)) => {
                    panic!(
                        "expected {input:?} to be a glob subscription, but it \
                         parsed as an exact subscription {exact:?}",
                    );
                }
                Ok(WebhookSubscriptionKind::Glob(glob)) => {
                    match regex::Regex::new(&glob.regex) {
                        Ok(_) => {}
                        Err(e) => panic!(
                            "glob {glob:?} produced an invalid regex: {e}"
                        ),
                    }
                }
                Err(e) => panic!(
                    "expected {input:?} to be a valid event class, but it \
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
            match WebhookSubscriptionKind::new(dbg!(input).to_string()) {
                Ok(glob) => panic!(
                    "invalid event class {input:?} was parsed \
                     successfully as {glob:?}"
                ),
                Err(error) => {
                    dbg!(error);
                }
            }
        }
    }
}
