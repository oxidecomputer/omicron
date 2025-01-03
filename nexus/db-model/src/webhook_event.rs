// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::webhook_event;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::WebhookEventKind;
use serde::{Deserialize, Serialize};

/// A webhook event.
#[derive(
    Clone,
    Queryable,
    Debug,
    Selectable,
    Serialize,
    Deserialize,
    Insertable,
    PartialEq,
)]
#[diesel(table_name = webhook_event)]
pub struct WebhookEvent {
    /// ID of the event.
    pub id: DbTypedUuid<WebhookEventKind>,

    /// The time this event was created.
    pub time_created: DateTime<Utc>,

    /// The time at which this event was dispatched by creating entries in the
    /// `webhook_delivery` table.
    ///
    /// If this is `None`, this event has yet to be dispatched.
    pub time_dispatched: Option<DateTime<Utc>>,

    /// The class of this event.
    pub event_class: String,

    /// The event's data payload.
    pub event: serde_json::Value,
}
