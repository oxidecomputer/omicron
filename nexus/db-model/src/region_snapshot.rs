// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::region_snapshot;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Database representation of a Region's snapshot.
///
/// A region snapshot represents a snapshot of a region, taken during the higher
/// level virtual disk snapshot operation.
#[derive(
    Queryable,
    Insertable,
    Debug,
    Clone,
    Selectable,
    Serialize,
    Deserialize,
    PartialEq,
)]
#[diesel(table_name = region_snapshot)]
pub struct RegionSnapshot {
    // unique identifier of this region snapshot
    pub dataset_id: Uuid,
    pub region_id: Uuid,
    pub snapshot_id: Uuid,

    /// used for identifying volumes that reference this
    pub snapshot_addr: String,

    /// how many volumes reference this?
    pub volume_references: i64,

    /// true if part of a volume's `resources_to_clean_up` already
    // this column was added in `schema/crdb/6.0.0/up1.sql` with a default of
    // false, so instruct serde to deserialize default as false if an old
    // serialized version of RegionSnapshot is being deserialized.
    #[serde(default)]
    pub deleting: bool,
}

impl RegionSnapshot {
    pub fn new(
        dataset_id: Uuid,
        region_id: Uuid,
        snapshot_id: Uuid,
        snapshot_addr: String,
    ) -> Self {
        RegionSnapshot {
            dataset_id,
            region_id,
            snapshot_id,
            snapshot_addr,

            volume_references: 0,
            deleting: false,
        }
    }
}
