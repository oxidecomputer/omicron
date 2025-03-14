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
    #[diesel(postgres_type(name = "webhook_delivery_state", schema = "public"))]
    pub struct WebhookDeliveryStateEnum;

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
    #[diesel(sql_type = WebhookDeliveryStateEnum)]
    #[serde(rename_all = "snake_case")]
    pub enum WebhookDeliveryState;

    Pending => b"pending"
    Failed => b"failed"
    Delivered => b"delivered"

);

impl fmt::Display for WebhookDeliveryState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Forward to the canonical implementation in nexus-types.
        views::WebhookDeliveryState::from(*self).fmt(f)
    }
}

impl From<WebhookDeliveryState> for views::WebhookDeliveryState {
    fn from(trigger: WebhookDeliveryState) -> Self {
        match trigger {
            WebhookDeliveryState::Pending => Self::Pending,
            WebhookDeliveryState::Failed => Self::Failed,
            WebhookDeliveryState::Delivered => Self::Delivered,
        }
    }
}

impl From<views::WebhookDeliveryState> for WebhookDeliveryState {
    fn from(trigger: views::WebhookDeliveryState) -> Self {
        match trigger {
            views::WebhookDeliveryState::Pending => Self::Pending,
            views::WebhookDeliveryState::Failed => Self::Failed,
            views::WebhookDeliveryState::Delivered => Self::Delivered,
        }
    }
}

impl diesel::query_builder::QueryId for WebhookDeliveryStateEnum {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
