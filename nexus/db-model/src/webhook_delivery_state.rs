// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::external_api::views;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::str::FromStr;

impl_enum_type!(
    WebhookDeliveryStateEnum:

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

impl FromStr for WebhookDeliveryState {
    type Err = omicron_common::api::external::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        views::WebhookDeliveryState::from_str(s).map(Into::into)
    }
}
