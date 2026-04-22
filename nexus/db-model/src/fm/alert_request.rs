// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management alert requests.

use crate::AlertClass;
use crate::DbTypedUuid;
use nexus_db_schema::schema::fm_alert_request;
use nexus_types::fm;
use omicron_uuid_kinds::{AlertKind, CaseKind, SitrepKind};

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_alert_request)]
pub struct AlertRequest {
    pub id: DbTypedUuid<AlertKind>,
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub requested_sitrep_id: DbTypedUuid<SitrepKind>,
    pub case_id: DbTypedUuid<CaseKind>,
    #[diesel(column_name = "alert_class")]
    pub class: AlertClass,
    pub payload: serde_json::Value,
    /// A human-readable comment added by the diagnosis engine to explain why
    /// it is requesting this alert.
    ///
    /// Sitrep comments are intended for debugging purposes only; i.e., they
    /// are visible to Oxide support via OMDB, but are not presented to the
    /// operator. The contents of comment fields are not stable, and a DE may
    /// emit a different comment string for an analogous determination across
    /// different software versions.
    pub comment: String,
}

impl AlertRequest {
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        case_id: impl Into<DbTypedUuid<CaseKind>>,
        req: fm::case::AlertRequest,
    ) -> Self {
        let fm::case::AlertRequest {
            id,
            requested_sitrep_id,
            payload,
            class,
            comment,
        } = req;
        AlertRequest {
            id: id.into(),
            sitrep_id: sitrep_id.into(),
            requested_sitrep_id: requested_sitrep_id.into(),
            case_id: case_id.into(),
            class: class.into(),
            payload,
            comment,
        }
    }
}

impl From<AlertRequest> for fm::case::AlertRequest {
    fn from(req: AlertRequest) -> Self {
        fm::case::AlertRequest {
            id: req.id.into(),
            requested_sitrep_id: req.requested_sitrep_id.into(),
            payload: req.payload,
            class: req.class.into(),
            comment: req.comment,
        }
    }
}
