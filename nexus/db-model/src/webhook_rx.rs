// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::{webhook_rx, webhook_rx_secret, webhook_rx_subscription};
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use omicron_uuid_kinds::WebhookReceiverKind;
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
    pub time_created: DateTime<Utc>,
}
