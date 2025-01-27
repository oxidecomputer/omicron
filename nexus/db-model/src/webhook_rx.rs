// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::collection::DatastoreCollectionConfig;
use crate::schema::{
    webhook_delivery, webhook_receiver, webhook_rx_event_glob,
    webhook_rx_secret, webhook_rx_subscription,
};
use crate::schema_versions;
use crate::typed_uuid::DbTypedUuid;
use crate::Generation;
use crate::WebhookDelivery;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use nexus_types::external_api::views;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{
    WebhookReceiverKind, WebhookReceiverUuid, WebhookSecretKind,
    WebhookSecretUuid,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;

/// The full configuration of a webhook receiver, including the
/// [`WebhookReceiver`] itself and its subscriptions and secrets.
pub struct WebhookReceiverConfig {
    pub rx: WebhookReceiver,
    pub secrets: Vec<WebhookRxSecret>,
    pub events: Vec<WebhookSubscriptionKind>,
}

impl TryFrom<WebhookReceiverConfig> for views::Webhook {
    type Error = Error;
    fn try_from(
        WebhookReceiverConfig { rx, secrets, events }: WebhookReceiverConfig,
    ) -> Result<views::Webhook, Self::Error> {
        let secrets = secrets
            .iter()
            .map(|WebhookRxSecret { signature_id, .. }| {
                views::WebhookSecretId { id: signature_id.to_string() }
            })
            .collect();
        let events = events
            .into_iter()
            .map(WebhookSubscriptionKind::into_event_class_string)
            .collect();
        let WebhookReceiver { identity, endpoint, probes_enabled, rcgen: _ } =
            rx;
        let WebhookReceiverIdentity { id, name, description, .. } = identity;
        let endpoint = endpoint.parse().map_err(|e| Error::InternalError {
            // This is an internal error, as we should not have ever allowed
            // an invalid URL to be inserted into the database...
            internal_message: format!("invalid webhook URL {endpoint:?}: {e}",),
        })?;
        Ok(views::Webhook {
            id: id.into(),
            name: name.to_string(),
            description,
            endpoint,
            secrets,
            events,
            disable_probes: !probes_enabled,
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
    pub probes_enabled: bool,

    /// child resource generation number, per RFD 192
    pub rcgen: Generation,
}

impl DatastoreCollectionConfig<WebhookRxSecret> for WebhookReceiver {
    type CollectionId = Uuid;
    type GenerationNumberColumn = webhook_receiver::dsl::rcgen;
    type CollectionTimeDeletedColumn = webhook_receiver::dsl::time_deleted;
    type CollectionIdColumn = webhook_rx_secret::dsl::rx_id;
}

impl DatastoreCollectionConfig<WebhookRxSubscription> for WebhookReceiver {
    type CollectionId = Uuid;
    type GenerationNumberColumn = webhook_receiver::dsl::rcgen;
    type CollectionTimeDeletedColumn = webhook_receiver::dsl::time_deleted;
    type CollectionIdColumn = webhook_rx_subscription::dsl::rx_id;
}

impl DatastoreCollectionConfig<WebhookRxEventGlob> for WebhookReceiver {
    type CollectionId = Uuid;
    type GenerationNumberColumn = webhook_receiver::dsl::rcgen;
    type CollectionTimeDeletedColumn = webhook_receiver::dsl::time_deleted;
    type CollectionIdColumn = webhook_rx_event_glob::dsl::rx_id;
}

impl DatastoreCollectionConfig<WebhookDelivery> for WebhookReceiver {
    type CollectionId = Uuid;
    type GenerationNumberColumn = webhook_receiver::dsl::rcgen;
    type CollectionTimeDeletedColumn = webhook_receiver::dsl::time_deleted;
    type CollectionIdColumn = webhook_delivery::dsl::rx_id;
}

// TODO(eliza): should deliveries/delivery attempts also be treated as children
// of a webhook receiver?

#[derive(
    Clone, Debug, Queryable, Selectable, Insertable, Serialize, Deserialize,
)]
#[diesel(table_name = webhook_rx_secret)]
pub struct WebhookRxSecret {
    pub rx_id: DbTypedUuid<WebhookReceiverKind>,
    pub signature_id: DbTypedUuid<WebhookSecretKind>,
    pub secret: String,
    pub time_created: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl WebhookRxSecret {
    pub fn new(rx_id: WebhookReceiverUuid, secret: String) -> Self {
        Self {
            rx_id: rx_id.into(),
            signature_id: WebhookSecretUuid::new_v4().into(),
            secret,
            time_created: Utc::now(),
            time_deleted: None,
        }
    }
}

#[derive(
    Clone, Debug, Queryable, Selectable, Insertable, Serialize, Deserialize,
)]
#[diesel(table_name = webhook_rx_subscription)]
pub struct WebhookRxSubscription {
    pub rx_id: DbTypedUuid<WebhookReceiverKind>,
    pub event_class: String,
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
    pub schema_version: schema_versions::DbSemverVersion,
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
#[derive(Clone, Debug)]
pub enum WebhookSubscriptionKind {
    Glob(WebhookGlob),
    Exact(String),
}

impl WebhookSubscriptionKind {
    pub fn new(value: String) -> Result<Self, Error> {
        if value.is_empty() {
            return Err(Error::invalid_value(
                "event_class",
                "must not be empty",
            ));
        }
        if value.contains('*') {
            let regex = WebhookGlob::regex_from_glob(&value)?;
            Ok(Self::Glob(WebhookGlob { regex, glob: value }))
        } else {
            Ok(Self::Exact(value))
        }
    }

    fn into_event_class_string(self) -> String {
        match self {
            Self::Exact(class) => class,
            Self::Glob(WebhookGlob { glob, .. }) => glob,
        }
    }
}

#[derive(
    Clone, Debug, Queryable, Selectable, Insertable, Serialize, Deserialize,
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

impl WebhookGlob {
    fn regex_from_glob(glob: &str) -> Result<String, Error> {
        let seg2regex = |segment: &str,
                         regex: &mut String|
         -> Result<(), Error> {
            match segment {
                // Match one segment (i.e. any number of segment characters)
                "*" => regex.push_str("[^\\.]+"),
                // Match any number of segments
                "**" => regex.push_str(".+"),
                s if s.contains('*') => {
                    return Err(Error::invalid_value(
                        "event_class",
                        "invalid event class {glob:?}: all segments must be \
                        either '*', '**', or any sequence of non-'*' characters",
                    ))
                }
                // Match the literal segment.
                s =>  regex.push_str(s),
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
                "must not be empty",
            ));
        };
        regex.push('$'); // End anchor

        Ok(regex)
    }
}

impl WebhookRxSubscription {
    pub fn exact(rx_id: WebhookReceiverUuid, event_class: String) -> Self {
        Self {
            rx_id: DbTypedUuid(rx_id),
            event_class,
            glob: None,
            time_created: Utc::now(),
        }
    }

    pub fn for_glob(glob: &WebhookRxEventGlob, event_class: String) -> Self {
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

    #[test]
    fn test_event_class_glob_to_regex() {
        const CASES: &[(&str, &str)] = &[
            ("foo.bar", "^foo.bar$"),
            ("foo.*.bar", "^foo\\.[^\\.]*\\.bar$"),
            ("foo.*", "^foo\\.[^\\.]*$"),
            ("*.foo", "^[^\\.]*\\.foo$"),
            ("foo.**.bar", "^foo\\..+\\.bar$"),
            ("foo.**", "^foo\\..+$"),
            ("foo_bar.baz", "^foo_bar\\.baz$"),
            ("foo_bar.*.baz", "^foo_bar\\.[^\\.]+\\.baz$"),
        ];
        for (class, regex) in CASES {
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
}
