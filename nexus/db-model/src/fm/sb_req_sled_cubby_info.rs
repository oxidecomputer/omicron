// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! DB model for the `fm_sb_req_sled_cubby_info` per-variant table.

use crate::DbTypedUuid;
use nexus_db_schema::schema::fm_sb_req_sled_cubby_info;
use omicron_uuid_kinds::{SitrepKind, SupportBundleKind};

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_sb_req_sled_cubby_info)]
pub struct SbReqSledCubbyInfo {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
}
