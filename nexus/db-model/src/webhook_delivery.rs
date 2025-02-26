// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::schema::{webhook_delivery, webhook_delivery_attempt};
use crate::serde_time_delta::optional_time_delta;
use crate::typed_uuid::DbTypedUuid;
use crate::SqlU8;
use crate::WebhookDeliveryTrigger;
use crate::WebhookEvent;
use crate::WebhookEventClass;
use chrono::{DateTime, TimeDelta, Utc};
use nexus_types::external_api::views;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::{
    OmicronZoneKind, OmicronZoneUuid, WebhookDeliveryKind, WebhookDeliveryUuid,
    WebhookEventKind, WebhookEventUuid, WebhookReceiverKind,
    WebhookReceiverUuid,
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

    /// Describes why this delivery was triggered.
    pub trigger: WebhookDeliveryTrigger,

    /// The data payload as sent to this receiver.
    pub payload: serde_json::Value,

    /// Attempt count
    pub attempts: SqlU8,

    /// The time at which this dispatch entry was created.
    pub time_created: DateTime<Utc>,

    /// The time at which the webhook message was either delivered successfully
    /// or permanently failed.
    pub time_completed: Option<DateTime<Utc>>,

    pub deliverator_id: Option<DbTypedUuid<OmicronZoneKind>>,
    pub time_delivery_started: Option<DateTime<Utc>>,
}

impl WebhookDelivery {
    pub fn new(
        event: &WebhookEvent,
        rx_id: &WebhookReceiverUuid,
        trigger: WebhookDeliveryTrigger,
    ) -> Self {
        Self {
            // N.B.: perhaps we ought to use timestamp-based UUIDs for these?
            id: WebhookDeliveryUuid::new_v4().into(),
            event_id: event.id,
            rx_id: (*rx_id).into(),
            trigger,
            payload: event.event.clone(),
            attempts: SqlU8::new(0),
            time_created: Utc::now(),
            time_completed: None,
            deliverator_id: None,
            time_delivery_started: None,
        }
    }

    pub fn new_probe(
        rx_id: &WebhookReceiverUuid,
        deliverator_id: &OmicronZoneUuid,
    ) -> Self {
        Self {
            // Just kinda make something up...
            id: WebhookDeliveryUuid::new_v4().into(),

            // XXX(eliza): hmm, should we just have one UUID for all probe events
            // and treat them as redeliveries of one thing? Why or why not?
            // UUIDs are basically free, right? On the other hand, if we care about
            // not having the event UUID not point to an entry in the events table
            // that doesn't exist, perhaps we'd rather just put one entry in there
            // for probes rather than create a new one for each probe...
            event_id: WebhookEventUuid::new_v4().into(),
            rx_id: (*rx_id).into(),
            trigger: WebhookDeliveryTrigger::Probe,
            payload: serde_json::json!({}),
            attempts: SqlU8::new(1),
            time_created: Utc::now(),
            time_completed: None,
            deliverator_id: Some((*deliverator_id).into()),
            time_delivery_started: Some(Utc::now()),
        }
    }

    pub fn to_api_delivery(
        &self,
        event_class: WebhookEventClass,
        attempt: Option<&WebhookDeliveryAttempt>,
    ) -> views::WebhookDelivery {
        views::WebhookDelivery {
            id: self.id.into_untyped_uuid(),
            webhook_id: self.rx_id.into(),
            event_class: event_class.as_str().to_owned(),
            event_id: self.event_id.into(),
            state: attempt
                .map(|attempt| attempt.result.into())
                .unwrap_or(views::WebhookDeliveryState::Pending),
            trigger: self.trigger.into(),
            response: attempt.and_then(WebhookDeliveryAttempt::response_view),
            time_sent: attempt.map(|attempt| attempt.time_created),
            attempt: attempt
                .map(|attempt| attempt.attempt.0 as usize)
                .unwrap_or(1),
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

impl WebhookDeliveryAttempt {
    fn response_view(&self) -> Option<views::WebhookDeliveryResponse> {
        Some(views::WebhookDeliveryResponse {
            status: self.response_status? as u16, // i hate that this has to signed in the database...
            duration_ms: self.response_duration?.num_milliseconds() as usize,
        })
    }
}

impl From<WebhookDeliveryResult> for views::WebhookDeliveryState {
    fn from(result: WebhookDeliveryResult) -> Self {
        match result {
            WebhookDeliveryResult::FailedHttpError => {
                views::WebhookDeliveryState::FailedHttpError
            }
            WebhookDeliveryResult::FailedTimeout => {
                views::WebhookDeliveryState::FailedTimeout
            }
            WebhookDeliveryResult::FailedUnreachable => {
                views::WebhookDeliveryState::FailedUnreachable
            }
            WebhookDeliveryResult::Succeeded => {
                views::WebhookDeliveryState::Delivered
            }
        }
    }
}
