// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::schema::{webhook_delivery, webhook_delivery_attempt};
use crate::serde_time_delta::optional_time_delta;
use crate::typed_uuid::DbTypedUuid;
use crate::SqlU8;
use crate::WebhookEvent;
use chrono::{DateTime, TimeDelta, Utc};
use omicron_uuid_kinds::{
    WebhookDeliveryKind, WebhookDeliveryUuid, WebhookEventKind,
    WebhookReceiverKind, WebhookReceiverUuid,
};
use serde::Deserialize;
use serde::Serialize;

impl_enum_type!(
    #[derive(SqlType, Debug, Clone)]
    #[diesel(postgres_type(name = "webhook_delivery_result", schema = "public"))]
    pub struct WebhookDeliveryResultEnum;

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
    )]
    #[diesel(sql_type = WebhookDeliveryResultEnum)]
    pub enum WebhookDeliveryResult;

    FailedHttpError => b"failed_http_error"
    FailedUnreachable => b"failed_unreachable"
    FailedTimeout => b"failed_timeout"
    Succeeded => b"succeeded"
);

/// A webhook delivery dispatch entry.
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
#[diesel(table_name = webhook_delivery)]
pub struct WebhookDelivery {
    /// ID of this dispatch entry.
    pub id: DbTypedUuid<WebhookDeliveryKind>,

    /// ID of the event dispatched to this receiver (foreign key into
    /// `webhook_event`).
    pub event_id: DbTypedUuid<WebhookEventKind>,

    /// ID of the receiver to which this event is dispatched (foreign key into
    /// `webhook_rx`).
    pub rx_id: DbTypedUuid<WebhookReceiverKind>,

    /// The data payload as sent to this receiver.
    pub payload: serde_json::Value,

    /// Attempt count
    pub attempts: SqlU8,

    /// The time at which this dispatch entry was created.
    pub time_created: DateTime<Utc>,

    /// The time at which the webhook message was either delivered successfully
    /// or permanently failed.
    pub time_completed: Option<DateTime<Utc>>,
}

impl WebhookDelivery {
    pub fn new(event: &WebhookEvent, rx_id: &WebhookReceiverUuid) -> Self {
        Self {
            // N.B.: perhaps we ought to use timestamp-based UUIDs for these?
            id: WebhookDeliveryUuid::new_v4().into(),
            event_id: event.id,
            rx_id: (*rx_id).into(),
            payload: event.event.clone(),
            attempts: SqlU8::new(0),
            time_created: Utc::now(),
            time_completed: None,
        }
    }
}

/// An individual delivery attempt for a [`WebhookDelivery`].
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
#[diesel(table_name = webhook_delivery_attempt)]
pub struct WebhookDeliveryAttempt {
    /// ID of the delivery entry (foreign key into `webhook_delivery`).
    pub delivery_id: DbTypedUuid<WebhookDeliveryKind>,

    /// Attempt number (retry count).
    pub attempt: SqlU8,

    pub result: WebhookDeliveryResult,

    pub response_status: Option<i16>,

    #[serde(with = "optional_time_delta")]
    pub response_duration: Option<TimeDelta>,

    pub time_created: DateTime<Utc>,
}
