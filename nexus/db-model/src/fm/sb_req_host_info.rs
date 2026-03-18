// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! DB model for the `fm_sb_req_host_info` per-variant table.

use crate::DbTypedUuid;
use nexus_db_schema::schema::fm_sb_req_host_info;
use nexus_types::support_bundle::SledSelection;
use omicron_uuid_kinds::{
    GenericUuid, SitrepKind, SledUuid, SupportBundleKind,
};
use std::collections::HashSet;

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_sb_req_host_info)]
pub struct SbReqHostInfo {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub request_id: DbTypedUuid<SupportBundleKind>,
    pub all_sleds: bool,
    pub sled_ids: Vec<uuid::Uuid>,
}

impl SbReqHostInfo {
    pub fn new(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        request_id: impl Into<DbTypedUuid<SupportBundleKind>>,
        sleds: &HashSet<SledSelection>,
    ) -> Self {
        let all_sleds = sleds.contains(&SledSelection::All);
        let sled_ids: Vec<uuid::Uuid> = sleds
            .iter()
            .filter_map(|s| match s {
                SledSelection::Specific(id) => Some(id.into_untyped_uuid()),
                SledSelection::All => None,
            })
            .collect();
        SbReqHostInfo {
            sitrep_id: sitrep_id.into(),
            request_id: request_id.into(),
            all_sleds,
            sled_ids,
        }
    }

    /// Reconstruct the `HashSet<SledSelection>` from the DB row.
    pub fn into_sled_selections(self) -> HashSet<SledSelection> {
        let mut set = HashSet::new();
        if self.all_sleds {
            set.insert(SledSelection::All);
        }
        for id in self.sled_ids {
            set.insert(SledSelection::Specific(SledUuid::from_untyped_uuid(
                id,
            )));
        }
        set
    }
}
