// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management support bundle requests.

use crate::DbTypedUuid;
use nexus_db_schema::schema::fm_support_bundle_request;
use nexus_types::fm;
use omicron_uuid_kinds::{CaseKind, SitrepKind, SupportBundleKind};

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_support_bundle_request)]
pub struct SupportBundleRequest {
    pub id: DbTypedUuid<SupportBundleKind>,
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub requested_sitrep_id: DbTypedUuid<SitrepKind>,
    pub case_id: DbTypedUuid<CaseKind>,
}

impl SupportBundleRequest {
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        case_id: impl Into<DbTypedUuid<CaseKind>>,
        req: fm::case::SupportBundleRequest,
    ) -> Self {
        let fm::case::SupportBundleRequest {
            id,
            requested_sitrep_id,
            data_selection: _,
        } = req;
        SupportBundleRequest {
            id: id.into(),
            sitrep_id: sitrep_id.into(),
            requested_sitrep_id: requested_sitrep_id.into(),
            case_id: case_id.into(),
        }
    }
}

impl From<SupportBundleRequest> for fm::case::SupportBundleRequest {
    fn from(req: SupportBundleRequest) -> Self {
        fm::case::SupportBundleRequest {
            id: req.id.into(),
            requested_sitrep_id: req.requested_sitrep_id.into(),
            // data_selection is reconstructed from per-variant tables
            // by the sitrep read path, not stored on this row.
            data_selection: None,
        }
    }
}
