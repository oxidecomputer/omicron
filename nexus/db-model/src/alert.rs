// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::AlertClass;
use crate::SqlU32;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_db_schema::schema::alert;
use serde::{Deserialize, Serialize};

/// A webhook event.
#[derive(
    Clone,
    Queryable,
    Debug,
    Selectable,
    Serialize,
    Deserialize,
    Insertable,
    PartialEq,
    Asset,
)]
#[diesel(table_name = alert)]
#[asset(uuid_kind = AlertKind)]
pub struct Alert {
    #[diesel(embed)]
    pub identity: AlertIdentity,

    /// The time at which this alert was dispatched by creating entries in the
    /// `webhook_delivery` table.
    ///
    /// If this is `None`, this alert has yet to be dispatched.
    pub time_dispatched: Option<DateTime<Utc>>,

    /// The class of this alert.
    #[diesel(column_name = alert_class)]
    pub class: AlertClass,

    /// The alert's data payload.
    pub payload: serde_json::Value,

    pub num_dispatched: i64,

    /// The version of the JSON schema for `payload`.
    pub schema_version: SqlU32,
}

impl Alert {
    /// UUID of the singleton event entry for alert receiver liveness probes.
    pub const PROBE_ALERT_ID: uuid::Uuid =
        uuid::Uuid::from_u128(0x001de000_7768_4000_8000_000000000001);
}
