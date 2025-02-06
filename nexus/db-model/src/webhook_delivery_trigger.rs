// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
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
    )]
    #[diesel(sql_type = WebhookDeliveryTriggerEnum)]
    #[serde(rename_all = "snake_case")]
    pub enum WebhookDeliveryTrigger;

    Dispatch => b"dispatch"
    Resend => b"resend"
);

impl WebhookDeliveryTrigger {
    pub fn as_str(&self) -> &'static str {
        // TODO(eliza): it would be really nice if these strings were all
        // declared a single time, rather than twice (in both `impl_enum_type!`
        // and here)...
        match self {
            Self::Dispatch => "dispatch",
            Self::Resend => "resend",
        }
    }
}

impl fmt::Display for WebhookDeliveryTrigger {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
