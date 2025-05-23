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
    AlertDeliveryTriggerEnum:

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
    pub enum AlertDeliveryTrigger;

    Alert => b"alert"
    Resend => b"resend"
    Probe => b"probe"

);

impl AlertDeliveryTrigger {
    pub const ALL: &'static [Self] = <Self as strum::VariantArray>::VARIANTS;
}

impl fmt::Display for AlertDeliveryTrigger {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Forward to the canonical implementation in nexus-types.
        views::AlertDeliveryTrigger::from(*self).fmt(f)
    }
}

impl From<AlertDeliveryTrigger> for views::AlertDeliveryTrigger {
    fn from(trigger: AlertDeliveryTrigger) -> Self {
        match trigger {
            AlertDeliveryTrigger::Alert => Self::Alert,
            AlertDeliveryTrigger::Resend => Self::Resend,
            AlertDeliveryTrigger::Probe => Self::Probe,
        }
    }
}

impl From<views::AlertDeliveryTrigger> for AlertDeliveryTrigger {
    fn from(trigger: views::AlertDeliveryTrigger) -> Self {
        match trigger {
            views::AlertDeliveryTrigger::Alert => Self::Alert,
            views::AlertDeliveryTrigger::Resend => Self::Resend,
            views::AlertDeliveryTrigger::Probe => Self::Probe,
        }
    }
}

impl FromStr for AlertDeliveryTrigger {
    type Err = omicron_common::api::external::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        views::AlertDeliveryTrigger::from_str(s).map(Into::into)
    }
}
