// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::collection::DatastoreCollectionConfig;
use crate::schema::{webhook_rx, webhook_rx_secret, webhook_rx_subscription};
use crate::typed_uuid::DbTypedUuid;
use crate::Generation;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{WebhookReceiverKind, WebhookReceiverUuid};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;

/// A webhook receiver configuration.
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
#[diesel(table_name = webhook_rx)]
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
    type GenerationNumberColumn = webhook_rx::dsl::rcgen;
    type CollectionTimeDeletedColumn = webhook_rx::dsl::time_deleted;
    type CollectionIdColumn = webhook_rx_secret::dsl::rx_id;
}

impl DatastoreCollectionConfig<WebhookRxSubscription> for WebhookReceiver {
    type CollectionId = Uuid;
    type GenerationNumberColumn = webhook_rx::dsl::rcgen;
    type CollectionTimeDeletedColumn = webhook_rx::dsl::time_deleted;
    type CollectionIdColumn = webhook_rx_subscription::dsl::rx_id;
}

// TODO(eliza): should deliveries/delivery attempts also be treated as children
// of a webhook receiver?

#[derive(
    Clone, Debug, Queryable, Selectable, Insertable, Serialize, Deserialize,
)]
#[diesel(table_name = webhook_rx_secret)]
pub struct WebhookRxSecret {
    pub rx_id: DbTypedUuid<WebhookReceiverKind>,
    pub signature_id: String,
    pub secret: Vec<u8>,
    pub time_created: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}

#[derive(
    Clone, Debug, Queryable, Selectable, Insertable, Serialize, Deserialize,
)]
#[diesel(table_name = webhook_rx_subscription)]
pub struct WebhookRxSubscription {
    pub rx_id: DbTypedUuid<WebhookReceiverKind>,
    #[diesel(embed)]
    pub glob: WebhookGlob,
    pub time_created: DateTime<Utc>,
}

#[derive(
    Clone, Debug, Queryable, Selectable, Insertable, Serialize, Deserialize,
)]
#[diesel(table_name = webhook_rx_subscription)]
pub struct WebhookGlob {
    pub event_class: String,
    pub similar_to: String,
}

impl FromStr for WebhookGlob {
    type Err = Error;
    fn from_str(event_class: &str) -> Result<Self, Self::Err> {
        fn seg2regex(segment: &str, similar_to: &mut String) {
            match segment {
                // Match one segment (i.e. any number of segment characters)
                "*" => similar_to.push_str("[a-zA-Z0-9\\_\\-]+"),
                // Match any number of segments
                "**" => similar_to.push('%'),
                // Match the literal segment.
                // Because `_` his a metacharacter in Postgres' SIMILAR TO
                // regexes, we've gotta go through and escape them.
                s => {
                    // TODO(eliza): validate what characters are in the segment...
                    for s in s.split_inclusive('_') {
                        // Handle the fact that there might not be a `_` in the
                        // string at all
                        if let Some(s) = s.strip_suffix('_') {
                            similar_to.push_str(s);
                            similar_to.push_str("\\_");
                        } else {
                            similar_to.push_str(s);
                        }
                    }
                }
            }
        }

        // The subscription's regex will always be at least as long as the event class.
        let mut similar_to = String::with_capacity(event_class.len());
        let mut segments = event_class.split('.');
        if let Some(segment) = segments.next() {
            seg2regex(segment, &mut similar_to);
            for segment in segments {
                similar_to.push('.'); // segment separator
                seg2regex(segment, &mut similar_to);
            }
        } else {
            return Err(Error::invalid_value(
                "event_class",
                "must not be empty",
            ));
        };

        Ok(Self { event_class: event_class.to_string(), similar_to })
    }
}

impl WebhookRxSubscription {
    pub fn new(rx_id: WebhookReceiverUuid, glob: WebhookGlob) -> Self {
        Self { rx_id: DbTypedUuid(rx_id), glob, time_created: Utc::now() }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_event_class_glob_to_regex() {
        const CASES: &[(&str, &str)] = &[
            ("foo.bar", "foo.bar"),
            ("foo.*.bar", "foo.[a-zA-Z0-9\\_\\-]+.bar"),
            ("foo.*", "foo.[a-zA-Z0-9\\_\\-]+"),
            ("*.foo", "[a-zA-Z0-9\\_\\-]+.foo"),
            ("foo.**.bar", "foo.%.bar"),
            ("foo.**", "foo.%"),
            ("foo_bar.baz", "foo\\_bar.baz"),
            ("foo_bar.*.baz", "foo\\_bar.[a-zA-Z0-9\\_\\-]+.baz"),
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
                dbg!(&glob.similar_to),
                "event class {class:?} should produce the regex {regex:?}"
            );
        }
    }
}
