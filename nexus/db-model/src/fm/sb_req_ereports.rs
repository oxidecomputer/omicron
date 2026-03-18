// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! DB model for the `fm_sb_req_ereports` per-variant table.

use crate::DbTypedUuid;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::fm_sb_req_ereports;
use nexus_types::support_bundle::EreportFilters;
use omicron_uuid_kinds::{SitrepKind, SupportBundleKind};

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_sb_req_ereports)]
pub struct SbReqEreports {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub only_serials: Vec<String>,
    pub only_classes: Vec<String>,
}

impl SbReqEreports {
    pub fn new(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        request_id: impl Into<DbTypedUuid<SupportBundleKind>>,
        filters: &EreportFilters,
    ) -> Self {
        SbReqEreports {
            sitrep_id: sitrep_id.into(),
            request_id: request_id.into(),
            start_time: filters.start_time,
            end_time: filters.end_time,
            only_serials: filters.only_serials.clone(),
            only_classes: filters.only_classes.clone(),
        }
    }

    /// Reconstruct the `EreportFilters` from the DB row.
    pub fn into_ereport_filters(self) -> EreportFilters {
        EreportFilters {
            start_time: self.start_time,
            end_time: self.end_time,
            only_serials: self.only_serials,
            only_classes: self.only_classes,
        }
    }
}
