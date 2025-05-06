// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::WebhookEventClass;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_db_schema::schema::webhook_event;
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
    Asset,
)]
#[diesel(table_name = webhook_event)]
#[asset(uuid_kind = WebhookEventKind)]
pub struct WebhookEvent {
    #[diesel(embed)]
    pub identity: WebhookEventIdentity,

    /// The time at which this event was dispatched by creating entries in the
    /// `webhook_delivery` table.
    ///
    /// If this is `None`, this event has yet to be dispatched.
    pub time_dispatched: Option<DateTime<Utc>>,

    /// The class of this event.
    pub event_class: WebhookEventClass,

    /// The event's data payload.
    pub event: serde_json::Value,

    pub num_dispatched: i64,

    /// The version of the JSON schema for `event`.
    pub payload_schema_version: i64,
}

impl WebhookEvent {
    /// UUID of the singleton event entry for webhook liveness probes.
    pub const PROBE_EVENT_ID: uuid::Uuid =
        uuid::Uuid::from_u128(0x001de000_7768_4000_8000_000000000001);
}
