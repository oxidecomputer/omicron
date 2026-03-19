// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::AlertClass;
use crate::DbTypedUuid;
use chrono::{DateTime, Utc};
use db_macros::Asset;
use nexus_db_schema::schema::alert;
use nexus_types::fm::case;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::CaseKind;
use omicron_uuid_kinds::CaseUuid;
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

    /// The ID of the fault management case that created this alert, if any.
    pub case_id: Option<DbTypedUuid<CaseKind>>,
}

impl Alert {
    /// UUID of the singleton event entry for alert receiver liveness probes.
    pub const PROBE_ALERT_ID: uuid::Uuid =
        uuid::Uuid::from_u128(0x001de000_7768_4000_8000_000000000001);

    /// Returns an `Alert` model representing a newly-created alert, with the
    /// provided ID, alert class, and JSON payload.
    pub fn new(
        id: impl Into<AlertUuid>,
        class: impl Into<AlertClass>,
        payload: impl Into<serde_json::Value>,
    ) -> Self {
        Self {
            identity: AlertIdentity::new(id.into()),
            time_dispatched: None,
            class: class.into(),
            payload: payload.into(),
            num_dispatched: 0,
            case_id: None,
        }
    }

    pub fn for_fm_alert_request(
        req: &case::AlertRequest,
        case_id: CaseUuid,
    ) -> Self {
        let &case::AlertRequest {
            id,
            class,
            ref payload,
            // Ignore the sitrep ID fields, as they are not included in the
            // alert model.
            requested_sitrep_id: _,
        } = req;

        Self {
            case_id: Some(case_id.into()),
            ..Self::new(id, class, payload.clone())
        }
    }
}
