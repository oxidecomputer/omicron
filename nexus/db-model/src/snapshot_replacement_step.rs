// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::schema::snapshot_replacement_step;
use chrono::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "snapshot_replacement_step_state", schema = "public"))]
    pub struct SnapshotReplacementStepStateEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = SnapshotReplacementStepStateEnum)]
    pub enum SnapshotReplacementStepState;

    // Enum values
    Requested => b"requested"
    Running => b"running"
    Complete => b"complete"
    VolumeDeleted => b"volume_deleted"
);

// FromStr impl required for use with clap (aka omdb)
impl std::str::FromStr for SnapshotReplacementStepState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "requested" => Ok(SnapshotReplacementStepState::Requested),
            "running" => Ok(SnapshotReplacementStepState::Running),
            "complete" => Ok(SnapshotReplacementStepState::Complete),
            "volume_deleted" => Ok(SnapshotReplacementStepState::VolumeDeleted),
            _ => Err(format!("unrecognized value {} for enum", s)),
        }
    }
}

/// Database representation of a Snapshot replacement update step.
///
/// During snapshot replacement, after the read-only target has been replaced in
/// the snapshot volume's construction request, Nexus needs to update each
/// running Upstairs that constructed an Upstairs using that old target. Each
/// volume that needs updating is recorded as a snapshot replacement step
/// record. The snapshot replacement finish saga can be run when all snapshot
/// replacement steps are completed. This record transitions through the
/// following states:
///
/// ```text
///      Requested   <--             ---
///                    |             |
///          |         |             |
///          v         |             | responsibility of snapshot replacement
///                    |             | step saga
///       Running    --              |
///                                  |
///          |                       |
///          v                       |
///                                  ---
///      Complete                    ---
///                                  |
///          |                       | responsibility of snapshot replacement
///          v                       | step garbage collect saga
///                                  |
///    VolumeDeleted                 ---
/// ```
///
/// See also: SnapshotReplacement records
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
#[diesel(table_name = snapshot_replacement_step)]
pub struct SnapshotReplacementStep {
    pub id: Uuid,

    pub request_id: Uuid,
    pub request_time: DateTime<Utc>,

    /// A volume that references the snapshot
    pub volume_id: Uuid,

    /// A synthetic volume that only is used to later delete the old snapshot
    pub old_snapshot_volume_id: Option<Uuid>,

    pub replacement_state: SnapshotReplacementStepState,

    pub operating_saga_id: Option<Uuid>,
}

impl SnapshotReplacementStep {
    pub fn new(request_id: Uuid, volume_id: Uuid) -> Self {
        Self {
            id: Uuid::new_v4(),
            request_id,
            request_time: Utc::now(),
            volume_id,
            old_snapshot_volume_id: None,
            replacement_state: SnapshotReplacementStepState::Requested,
            operating_saga_id: None,
        }
    }
}
