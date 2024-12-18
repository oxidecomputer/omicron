// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use serde::Deserialize;
use serde::Serialize;

impl_enum_type!(
    #[derive(SqlType, Debug, Clone)]
    #[diesel(postgres_type(name = "webhook_msg_delivery_result", schema = "public"))]
    pub struct WebhookDeliveryResultEnum;

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
    )]
    #[diesel(sql_type = WebhookDeliveryResultEnum)]
    pub enum WebhookDeliveryResult;

    FailedHttpError => b"failed_http_error"
    FailedUnreachable => b"failed_unreachable"
    Succeeded => b"succeeded"
);
