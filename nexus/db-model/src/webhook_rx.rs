// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::AlertRxGlob;
use crate::AlertRxSubscription;
use crate::AlertSubscriptionKind;
use crate::Generation;
use crate::Name;
use crate::collection::DatastoreCollectionConfig;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use db_macros::{Asset, Resource};
use nexus_db_schema::schema::{
    alert_glob, alert_receiver, alert_subscription, webhook_secret,
};
use nexus_types::external_api::shared;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{
    AlertReceiverKind, AlertReceiverUuid, GenericUuid, WebhookSecretUuid,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The full configuration of a webhook alert receiver, including the
/// [`AlertReceiver`], its subscriptions, and secrets.
#[derive(Clone, Debug)]
pub struct WebhookReceiverConfig {
    pub rx: AlertReceiver,
    pub secrets: Vec<WebhookSecret>,
    pub subscriptions: Vec<AlertSubscriptionKind>,
}

impl TryFrom<WebhookReceiverConfig> for views::WebhookReceiver {
    type Error = Error;
    fn try_from(
        WebhookReceiverConfig { rx, secrets, subscriptions }: WebhookReceiverConfig,
    ) -> Result<views::WebhookReceiver, Self::Error> {
        let secrets = secrets.iter().map(views::WebhookSecret::from).collect();
        let subscriptions = subscriptions
            .into_iter()
            .map(shared::AlertSubscription::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let endpoint =
            rx.endpoint.parse().map_err(|e| Error::InternalError {
                // This is an internal error, as we should not have ever allowed
                // an invalid URL to be inserted into the database...
                internal_message: format!(
                    "invalid webhook URL {:?}: {e}",
                    rx.endpoint,
                ),
            })?;
        Ok(views::WebhookReceiver {
            identity: rx.identity(),
            subscriptions,
            config: views::WebhookReceiverConfig { secrets, endpoint },
        })
    }
}

/// A row in the `alert_receiver` table.
// XXX(eliza): Note that this presently contains both generic "alert receiver
// stuff" (i.e. the identity and subscription rcgen) *and*
// webhook-receiver-specific stuff (endpoint, secret rcgen). If/when we
// introduce other kinds of alert receivers, we will want to split that out into
// a webhook-specific table.
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
#[resource(uuid_kind = AlertReceiverKind)]
#[diesel(table_name = alert_receiver)]
pub struct AlertReceiver {
    #[diesel(embed)]
    pub identity: AlertReceiverIdentity,
    pub endpoint: String,

    /// child resource generation number for secrets, per RFD 192
    pub secret_gen: Generation,
    /// child resource generation number for event subscriptions, per RFD 192
    pub subscription_gen: Generation,
}

impl DatastoreCollectionConfig<WebhookSecret> for AlertReceiver {
    type CollectionId = Uuid;
    type GenerationNumberColumn = alert_receiver::dsl::secret_gen;
    type CollectionTimeDeletedColumn = alert_receiver::dsl::time_deleted;
    type CollectionIdColumn = webhook_secret::dsl::rx_id;
}

impl DatastoreCollectionConfig<AlertRxSubscription> for AlertReceiver {
    type CollectionId = Uuid;
    type GenerationNumberColumn = alert_receiver::dsl::subscription_gen;
    type CollectionTimeDeletedColumn = alert_receiver::dsl::time_deleted;
    type CollectionIdColumn = alert_subscription::dsl::rx_id;
}

impl DatastoreCollectionConfig<AlertRxGlob> for AlertReceiver {
    type CollectionId = Uuid;
    type GenerationNumberColumn = alert_receiver::dsl::subscription_gen;
    type CollectionTimeDeletedColumn = alert_receiver::dsl::time_deleted;
    type CollectionIdColumn = alert_glob::dsl::rx_id;
}

/// Describes a set of updates for the [`alert_receiver`] table to update a
/// webhook receiver configuration.
#[derive(Clone, AsChangeset)]
#[diesel(table_name = alert_receiver)]
pub struct WebhookReceiverUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub endpoint: Option<String>,
    pub time_modified: DateTime<Utc>,
}

#[derive(
    Clone,
    Debug,
    Queryable,
    Selectable,
    Insertable,
    Serialize,
    Deserialize,
    Asset,
)]
#[asset(uuid_kind = WebhookSecretKind)]
#[diesel(table_name = webhook_secret)]
pub struct WebhookSecret {
    #[diesel(embed)]
    pub identity: WebhookSecretIdentity,
    #[diesel(column_name = rx_id)]
    pub alert_receiver_id: DbTypedUuid<AlertReceiverKind>,
    pub secret: String,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl WebhookSecret {
    pub fn new(rx_id: AlertReceiverUuid, secret: String) -> Self {
        Self {
            identity: WebhookSecretIdentity::new(WebhookSecretUuid::new_v4()),
            alert_receiver_id: rx_id.into(),
            secret,
            time_deleted: None,
        }
    }
}

impl From<&'_ WebhookSecret> for views::WebhookSecret {
    fn from(secret: &WebhookSecret) -> Self {
        Self {
            id: secret.identity.id.into_untyped_uuid(),
            time_created: secret.identity.time_created,
        }
    }
}

impl From<WebhookSecret> for views::WebhookSecret {
    fn from(secret: WebhookSecret) -> Self {
        Self::from(&secret)
    }
}
