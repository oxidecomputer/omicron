// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management alert requests.

use crate::AlertClass;
use crate::DbTypedUuid;
use nexus_db_schema::schema::fm_alert_request;
use nexus_types::fm;
use omicron_uuid_kinds::{
    AlertKind, CaseKind, CaseUuid, SitrepKind, SitrepUuid,
};

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_alert_request)]
pub struct AlertRequest {
    pub id: DbTypedUuid<AlertKind>,
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub requested_sitrep_id: DbTypedUuid<SitrepKind>,
    pub case_id: DbTypedUuid<CaseKind>,
    #[diesel(column_name = "class")]
    pub class: AlertClass,
    pub payload: serde_json::Value,
}

impl AlertRequest {
    pub fn new(
        current_sitrep_id: SitrepUuid,
        case_id: CaseUuid,
        req: fm::AlertRequest,
    ) -> Self {
        let fm::AlertRequest { id, requested_sitrep_id, payload, class } = req;
        AlertRequest {
            id: id.into(),
            sitrep_id: current_sitrep_id.into(),
            requested_sitrep_id: requested_sitrep_id.into(),
            case_id: case_id.into(),
            class: class.into(),
            payload,
        }
    }
}

impl TryFrom<AlertRequest> for fm::AlertRequest {
    type Error = <fm::AlertClass as TryFrom<AlertClass>>::Error;
    fn try_from(req: AlertRequest) -> Result<Self, Self::Error> {
        Ok(fm::AlertRequest {
            id: req.id.into(),
            requested_sitrep_id: req.requested_sitrep_id.into(),
            payload: req.payload,
            class: req.class.try_into()?,
        })
    }
}
