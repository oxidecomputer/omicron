// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::external_api::views;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    WebhookDeliveryAttemptResultEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        strum::VariantArray,
    )]
    pub enum WebhookDeliveryAttemptResult;

    FailedHttpError => b"failed_http_error"
    FailedUnreachable => b"failed_unreachable"
    FailedTimeout => b"failed_timeout"
    Succeeded => b"succeeded"
);

impl WebhookDeliveryAttemptResult {
    pub fn is_failed(&self) -> bool {
        // Use canonical implementation from the API type.
        views::WebhookDeliveryAttemptResult::from(*self).is_failed()
    }
}

impl fmt::Display for WebhookDeliveryAttemptResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use canonical format from the API type.
        views::WebhookDeliveryAttemptResult::from(*self).fmt(f)
    }
}

impl From<WebhookDeliveryAttemptResult>
    for views::WebhookDeliveryAttemptResult
{
    fn from(result: WebhookDeliveryAttemptResult) -> Self {
        match result {
            WebhookDeliveryAttemptResult::FailedHttpError => {
                Self::FailedHttpError
            }
            WebhookDeliveryAttemptResult::FailedTimeout => Self::FailedTimeout,
            WebhookDeliveryAttemptResult::FailedUnreachable => {
                Self::FailedUnreachable
            }
            WebhookDeliveryAttemptResult::Succeeded => Self::Succeeded,
        }
    }
}
