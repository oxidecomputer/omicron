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
    AlertDeliveryStateEnum:

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
    pub enum AlertDeliveryState;

    Pending => b"pending"
    Failed => b"failed"
    Delivered => b"delivered"

);

impl fmt::Display for AlertDeliveryState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Forward to the canonical implementation in nexus-types.
        views::AlertDeliveryState::from(*self).fmt(f)
    }
}

impl From<AlertDeliveryState> for views::AlertDeliveryState {
    fn from(trigger: AlertDeliveryState) -> Self {
        match trigger {
            AlertDeliveryState::Pending => Self::Pending,
            AlertDeliveryState::Failed => Self::Failed,
            AlertDeliveryState::Delivered => Self::Delivered,
        }
    }
}

impl From<views::AlertDeliveryState> for AlertDeliveryState {
    fn from(trigger: views::AlertDeliveryState) -> Self {
        match trigger {
            views::AlertDeliveryState::Pending => Self::Pending,
            views::AlertDeliveryState::Failed => Self::Failed,
            views::AlertDeliveryState::Delivered => Self::Delivered,
        }
    }
}

impl FromStr for AlertDeliveryState {
    type Err = omicron_common::api::external::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        views::AlertDeliveryState::from_str(s).map(Into::into)
    }
}
