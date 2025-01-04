// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::{webhook_rx, webhook_rx_secret, webhook_rx_subscription};
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use omicron_uuid_kinds::{WebhookReceiverKind, WebhookReceiverUuid};
use serde::{Deserialize, Serialize};

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
    identity: WebhookReceiverIdentity,
    pub probes_enabled: bool,
    pub endpoint: String,
}

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
    pub event_class: String,
    pub similar_to: String,
    pub time_created: DateTime<Utc>,
}

impl WebhookRxSubscription {
    pub fn new(rx_id: WebhookReceiverUuid, event_class: String) -> Self {
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
            // TODO(eliza): we should probably validate that the event class has
            // at least one segment...
        };

        // `_` is a metacharacter in Postgres' SIMILAR TO regexes, so escape
        // them.

        Self {
            rx_id: DbTypedUuid(rx_id),
            event_class,
            similar_to,
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
            ("foo.bar", "foo.bar"),
            ("foo.*.bar", "foo.[a-zA-Z0-9\\_\\-]+.bar"),
            ("foo.*", "foo.[a-zA-Z0-9\\_\\-]+"),
            ("*.foo", "[a-zA-Z0-9\\_\\-]+.foo"),
            ("foo.**.bar", "foo.%.bar"),
            ("foo.**", "foo.%"),
            ("foo_bar.baz", "foo\\_bar.baz"),
            ("foo_bar.*.baz", "foo\\_bar.[a-zA-Z0-9\\_\\-]+.baz"),
        ];
        let rx_id = WebhookReceiverUuid::new_v4();
        for (class, regex) in CASES {
            let subscription =
                WebhookRxSubscription::new(rx_id, dbg!(class).to_string());
            assert_eq!(
                dbg!(regex),
                dbg!(&subscription.similar_to),
                "event class {class:?} should produce the regex {regex:?}"
            );
        }
    }
}
