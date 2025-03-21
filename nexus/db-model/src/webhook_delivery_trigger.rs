// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::external_api::views;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    #[derive(SqlType, Debug, Clone)]
    #[diesel(postgres_type(name = "webhook_delivery_trigger", schema = "public"))]
    pub struct WebhookDeliveryTriggerEnum;

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Serialize,
        Deserialize,
        AsExpression,
        FromSqlRow,
        strum::VariantArray,
    )]
    #[diesel(sql_type = WebhookDeliveryTriggerEnum)]
    #[serde(rename_all = "snake_case")]
    pub enum WebhookDeliveryTrigger;

    Event => b"event"
    Resend => b"resend"
    Probe => b"probe"

);

impl WebhookDeliveryTrigger {
    pub const ALL: &'static [Self] = <Self as strum::VariantArray>::VARIANTS;
}

impl fmt::Display for WebhookDeliveryTrigger {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Forward to the canonical implementation in nexus-types.
        views::WebhookDeliveryTrigger::from(*self).fmt(f)
    }
}

impl From<WebhookDeliveryTrigger> for views::WebhookDeliveryTrigger {
    fn from(trigger: WebhookDeliveryTrigger) -> Self {
        match trigger {
            WebhookDeliveryTrigger::Event => Self::Event,
            WebhookDeliveryTrigger::Resend => Self::Resend,
            WebhookDeliveryTrigger::Probe => Self::Probe,
        }
    }
}

impl From<views::WebhookDeliveryTrigger> for WebhookDeliveryTrigger {
    fn from(trigger: views::WebhookDeliveryTrigger) -> Self {
        match trigger {
            views::WebhookDeliveryTrigger::Event => Self::Event,
            views::WebhookDeliveryTrigger::Resend => Self::Resend,
            views::WebhookDeliveryTrigger::Probe => Self::Probe,
        }
    }
}

impl diesel::query_builder::QueryId for WebhookDeliveryTriggerEnum {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
