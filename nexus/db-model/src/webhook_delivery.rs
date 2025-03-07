// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::{webhook_delivery, webhook_delivery_attempt};
use crate::serde_time_delta::optional_time_delta;
use crate::typed_uuid::DbTypedUuid;
use crate::SqlU8;
use crate::WebhookDeliveryAttemptResult;
use crate::WebhookDeliveryState;
use crate::WebhookDeliveryTrigger;
use crate::WebhookEvent;
use crate::WebhookEventClass;
use chrono::{DateTime, TimeDelta, Utc};
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::{
    OmicronZoneKind, OmicronZoneUuid, WebhookDeliveryKind, WebhookDeliveryUuid,
    WebhookEventKind, WebhookEventUuid, WebhookReceiverKind,
    WebhookReceiverUuid,
};
use serde::Deserialize;
use serde::Serialize;

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

    pub state: WebhookDeliveryState,

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
            event_id: event.id().into(),
            rx_id: (*rx_id).into(),
            trigger,
            payload: event.event.clone(),
            attempts: SqlU8::new(0),
            time_created: Utc::now(),
            time_completed: None,
            deliverator_id: None,
            time_delivery_started: None,
            state: WebhookDeliveryState::Pending,
        }
    }

    pub fn new_probe(
        rx_id: &WebhookReceiverUuid,
        deliverator_id: &OmicronZoneUuid,
    ) -> Self {
        Self {
            // Just kinda make something up...
            id: WebhookDeliveryUuid::new_v4().into(),
            // There's a singleton entry in the `webhook_event` table for
            // probes, so that we can reference a real event ID but need not
            // create a bunch of duplicate empty events every time a probe is sent.
            event_id: WebhookEventUuid::from_untyped_uuid(
                WebhookEvent::PROBE_EVENT_ID,
            )
            .into(),
            rx_id: (*rx_id).into(),
            trigger: WebhookDeliveryTrigger::Probe,
            state: WebhookDeliveryState::Pending,
            payload: serde_json::json!({}),
            attempts: SqlU8::new(0),
            time_created: Utc::now(),
            time_completed: None,
            deliverator_id: Some((*deliverator_id).into()),
            time_delivery_started: Some(Utc::now()),
        }
    }

    pub fn to_api_delivery(
        &self,
        event_class: WebhookEventClass,
        attempts: &[WebhookDeliveryAttempt],
    ) -> views::WebhookDelivery {
        let mut view = views::WebhookDelivery {
            id: self.id.into_untyped_uuid(),
            webhook_id: self.rx_id.into(),
            event_class: event_class.as_str().to_owned(),
            event_id: self.event_id.into(),
            state: self.state.into(),
            trigger: self.trigger.into(),
            attempts: attempts
                .iter()
                .map(views::WebhookDeliveryAttempt::from)
                .collect(),
        };
        // Make sure attempts are in order; each attempt entry also includes an
        // attempt number, which should be used authoritatively to determine the
        // ordering of attempts, but it seems nice to also sort the list,
        // because we can...
        view.attempts.sort_by_key(|a| a.attempt);
        view
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

    /// ID of the receiver to which this event is dispatched (foreign key into
    /// `webhook_rx`).
    pub rx_id: DbTypedUuid<WebhookReceiverKind>,

    pub result: WebhookDeliveryAttemptResult,

    pub response_status: Option<i16>,

    #[serde(with = "optional_time_delta")]
    pub response_duration: Option<TimeDelta>,

    pub time_created: DateTime<Utc>,

    pub deliverator_id: DbTypedUuid<OmicronZoneKind>,
}

impl WebhookDeliveryAttempt {
    fn response_view(&self) -> Option<views::WebhookDeliveryResponse> {
        Some(views::WebhookDeliveryResponse {
            status: self.response_status? as u16, // i hate that this has to signed in the database...
            duration_ms: self.response_duration?.num_milliseconds() as usize,
        })
    }
}

impl From<&'_ WebhookDeliveryAttempt> for views::WebhookDeliveryAttempt {
    fn from(attempt: &WebhookDeliveryAttempt) -> Self {
        let response = attempt.response_view();
        Self {
            attempt: attempt.attempt.0 as usize,
            result: attempt.result.into(),
            time_sent: attempt.time_created,
            response,
        }
    }
}
