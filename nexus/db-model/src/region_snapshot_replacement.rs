// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::schema::region_snapshot_replacement;
use crate::RegionSnapshot;
use chrono::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "region_snapshot_replacement_state", schema = "public"))]
    pub struct RegionSnapshotReplacementStateEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = RegionSnapshotReplacementStateEnum)]
    pub enum RegionSnapshotReplacementState;

    // Enum values
    Requested => b"requested"
    Allocating => b"allocating"
    ReplacementDone => b"replacement_done"
    DeletingOldVolume => b"deleting_old_volume"
    Running => b"running"
    Complete => b"complete"
);

// FromStr impl required for use with clap (aka omdb)
impl std::str::FromStr for RegionSnapshotReplacementState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "requested" => Ok(RegionSnapshotReplacementState::Requested),
            "allocating" => Ok(RegionSnapshotReplacementState::Allocating),
            "replacement_done" => {
                Ok(RegionSnapshotReplacementState::ReplacementDone)
            }
            "deleting_old_volume" => {
                Ok(RegionSnapshotReplacementState::DeletingOldVolume)
            }
            "running" => Ok(RegionSnapshotReplacementState::Running),
            "complete" => Ok(RegionSnapshotReplacementState::Complete),
            _ => Err(format!("unrecognized value {} for enum", s)),
        }
    }
}

/// Database representation of a RegionSnapshot replacement request.
///
/// This record stores the data related to the operations required for Nexus to
/// orchestrate replacing a region snapshot. It transitions through the
/// following states:
///
/// ```text
///      Requested   <--              ---
///                    |              |
///          |         |              |
///          v         |              |  responsibility of region snapshot
///                    |              |  replacement start saga
///      Allocating  --               |
///                                   |
///          |                        |
///          v                        ---
///                                   ---
///    ReplacementDone  <--           |
///                       |           |
///          |            |           |
///          v            |           | responsibility of region snapshot
///                       |           | replacement garbage collect saga
///  DeletingOldVolume  --            |
///                                   |
///          |                        |
///          v                        ---
///                                   ---
///       Running                     |
///                                   | set in region snapshot replacement
///          |                        | finish background task
///          v                        |
///                                   |
///      Complete                     ---
/// ```
///
/// which are captured in the RegionSnapshotReplacementState enum. Annotated on
/// the right are which sagas are responsible for which state transitions. The
/// state transitions themselves are performed by these sagas and all involve a
/// query that:
///
///  - checks that the starting state (and other values as required) make sense
///  - updates the state while setting a unique operating_saga_id id (and any
///    other fields as appropriate)
///
/// As multiple background tasks will be waking up, checking to see what sagas
/// need to be triggered, and requesting that these region snapshot replacement
/// sagas run, this is meant to block multiple sagas from running at the same
/// time in an effort to cut down on interference - most will unwind at the
/// first step of performing this state transition instead of somewhere in the
/// middle.
///
/// See also: RegionSnapshotReplacementStep records
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
#[diesel(table_name = region_snapshot_replacement)]
pub struct RegionSnapshotReplacement {
    pub id: Uuid,

    pub request_time: DateTime<Utc>,

    // These are a copy of fields from the corresponding region snapshot record
    pub old_dataset_id: Uuid,
    pub old_region_id: Uuid,
    pub old_snapshot_id: Uuid,

    /// A synthetic volume that only is used to later delete the old snapshot
    pub old_snapshot_volume_id: Option<Uuid>,

    pub new_region_id: Option<Uuid>,

    pub replacement_state: RegionSnapshotReplacementState,

    pub operating_saga_id: Option<Uuid>,
}

impl RegionSnapshotReplacement {
    pub fn for_region_snapshot(region_snapshot: &RegionSnapshot) -> Self {
        Self::new(
            region_snapshot.dataset_id,
            region_snapshot.region_id,
            region_snapshot.snapshot_id,
        )
    }

    pub fn new(
        old_dataset_id: Uuid,
        old_region_id: Uuid,
        old_snapshot_id: Uuid,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            request_time: Utc::now(),
            old_dataset_id,
            old_region_id,
            old_snapshot_id,
            old_snapshot_volume_id: None,
            new_region_id: None,
            replacement_state: RegionSnapshotReplacementState::Requested,
            operating_saga_id: None,
        }
    }
}
