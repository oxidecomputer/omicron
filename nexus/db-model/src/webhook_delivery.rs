// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Alert;
use crate::AlertClass;
use crate::AlertDeliveryState;
use crate::AlertDeliveryTrigger;
use crate::SqlU8;
use crate::SqlU16;
use crate::WebhookDeliveryAttemptResult;
use crate::serde_time_delta::optional_time_delta;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, TimeDelta, Utc};
use nexus_db_schema::schema::{webhook_delivery, webhook_delivery_attempt};
use nexus_types::external_api::views;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::{
    AlertKind, AlertReceiverKind, AlertReceiverUuid, AlertUuid,
    OmicronZoneKind, OmicronZoneUuid, WebhookDeliveryAttemptKind,
    WebhookDeliveryKind, WebhookDeliveryUuid,
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
    /// `alert`).
    #[diesel(column_name = alert_id)]
    pub alert_id: DbTypedUuid<AlertKind>,

    /// ID of the receiver to which this event is dispatched (foreign key into
    /// `webhook_rx`).
    pub rx_id: DbTypedUuid<AlertReceiverKind>,

    /// Describes why this delivery was triggered.
    pub triggered_by: AlertDeliveryTrigger,

    /// Attempt count
    pub attempts: SqlU8,

    /// The time at which this dispatch entry was created.
    pub time_created: DateTime<Utc>,

    /// The time at which the webhook message was either delivered successfully
    /// or permanently failed.
    pub time_completed: Option<DateTime<Utc>>,

    pub state: AlertDeliveryState,

    pub deliverator_id: Option<DbTypedUuid<OmicronZoneKind>>,

    pub time_leased: Option<DateTime<Utc>>,
}

impl WebhookDelivery {
    pub fn new(
        alert_id: &AlertUuid,
        rx_id: &AlertReceiverUuid,
        triggered_by: AlertDeliveryTrigger,
    ) -> Self {
        Self {
            id: WebhookDeliveryUuid::new_v4().into(),
            alert_id: (*alert_id).into(),
            rx_id: (*rx_id).into(),
            triggered_by,
            attempts: SqlU8::new(0),
            time_created: Utc::now(),
            time_completed: None,
            deliverator_id: None,
            time_leased: None,
            state: AlertDeliveryState::Pending,
        }
    }

    pub fn new_probe(
        rx_id: &AlertReceiverUuid,
        deliverator_id: &OmicronZoneUuid,
    ) -> Self {
        Self {
            // Just kinda make something up...
            id: WebhookDeliveryUuid::new_v4().into(),
            // There's a singleton entry in the `webhook_event` table for
            // probes, so that we can reference a real event ID but need not
            // create a bunch of duplicate empty events every time a probe is sent.
            alert_id: AlertUuid::from_untyped_uuid(Alert::PROBE_ALERT_ID)
                .into(),
            rx_id: (*rx_id).into(),
            triggered_by: AlertDeliveryTrigger::Probe,
            state: AlertDeliveryState::Pending,
            attempts: SqlU8::new(0),
            time_created: Utc::now(),
            time_completed: None,
            deliverator_id: Some((*deliverator_id).into()),
            time_leased: Some(Utc::now()),
        }
    }

    pub fn to_api_delivery(
        &self,
        alert_class: AlertClass,
        attempts: &[WebhookDeliveryAttempt],
    ) -> views::AlertDelivery {
        let mut attempts: Vec<_> =
            attempts.iter().map(views::WebhookDeliveryAttempt::from).collect();
        // Make sure attempts are in order; each attempt entry also includes an
        // attempt number, which should be used authoritatively to determine the
        // ordering of attempts, but it seems nice to also sort the list,
        // because we can...
        attempts.sort_by_key(|a| a.attempt);
        views::AlertDelivery {
            id: self.id.into_untyped_uuid(),
            receiver_id: self.rx_id.into(),
            alert_class: alert_class.as_str().to_owned(),
            alert_id: self.alert_id.into(),
            state: self.state.into(),
            trigger: self.triggered_by.into(),
            attempts: views::AlertDeliveryAttempts::Webhook(attempts),
            time_started: self.time_created,
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
    pub id: DbTypedUuid<WebhookDeliveryAttemptKind>,

    /// ID of the delivery entry (foreign key into `webhook_delivery`).
    pub delivery_id: DbTypedUuid<WebhookDeliveryKind>,

    /// Attempt number (retry count).
    pub attempt: SqlU8,

    /// ID of the receiver to which this alert is dispatched (foreign key into
    /// `webhook_rx`).
    pub rx_id: DbTypedUuid<AlertReceiverKind>,

    pub result: WebhookDeliveryAttemptResult,

    pub response_status: Option<SqlU16>,

    #[serde(with = "optional_time_delta")]
    pub response_duration: Option<TimeDelta>,

    pub time_created: DateTime<Utc>,

    pub deliverator_id: DbTypedUuid<OmicronZoneKind>,
}

impl WebhookDeliveryAttempt {
    fn response_view(&self) -> Option<views::WebhookDeliveryResponse> {
        Some(views::WebhookDeliveryResponse {
            status: self.response_status?.into(),
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
