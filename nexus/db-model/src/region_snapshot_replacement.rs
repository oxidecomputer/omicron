// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::RegionSnapshot;
use crate::schema::region_snapshot_replacement;
use crate::typed_uuid::DbTypedUuid;
use chrono::DateTime;
use chrono::Utc;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::VolumeKind;
use omicron_uuid_kinds::VolumeUuid;
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
    Completing => b"completing"
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
            "completing" => Ok(RegionSnapshotReplacementState::Completing),
            "complete" => Ok(RegionSnapshotReplacementState::Complete),
            _ => Err(format!("unrecognized value {} for enum", s)),
        }
    }
}

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "read_only_target_replacement_type", schema = "public"))]
    pub struct ReadOnlyTargetReplacementTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = ReadOnlyTargetReplacementTypeEnum)]
    pub enum ReadOnlyTargetReplacementType;

    // Enum values
    RegionSnapshot => b"region_snapshot"
    ReadOnlyRegion => b"read_only_region"
);

// FromStr impl required for use with clap (aka omdb)
impl std::str::FromStr for ReadOnlyTargetReplacementType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "region_snapshot" => {
                Ok(ReadOnlyTargetReplacementType::RegionSnapshot)
            }
            "read_only_region" => {
                Ok(ReadOnlyTargetReplacementType::ReadOnlyRegion)
            }
            _ => Err(format!("unrecognized value {} for enum", s)),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ReadOnlyTargetReplacement {
    RegionSnapshot {
        dataset_id: DbTypedUuid<DatasetKind>,
        region_id: Uuid,
        snapshot_id: Uuid,
    },

    ReadOnlyRegion {
        region_id: Uuid,
    },
}

impl slog::KV for ReadOnlyTargetReplacement {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match &self {
            ReadOnlyTargetReplacement::RegionSnapshot {
                dataset_id,
                region_id,
                snapshot_id,
            } => {
                serializer.emit_str("type".into(), "region_snapshot")?;
                serializer
                    .emit_str("dataset_id".into(), &dataset_id.to_string())?;
                serializer
                    .emit_str("region_id".into(), &region_id.to_string())?;
                serializer
                    .emit_str("snapshot_id".into(), &snapshot_id.to_string())?;
            }

            ReadOnlyTargetReplacement::ReadOnlyRegion { region_id } => {
                serializer.emit_str("type".into(), "read_only_region")?;
                serializer
                    .emit_str("region_id".into(), &region_id.to_string())?;
            }
        }

        Ok(())
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
///       Running  <--                |
///                  |                |
///          |       |                |
///          v       |                |
///                  |                | responsibility of region snapshot
///     Completing --                 | replacement finish saga
///                                   |
///          |                        |
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
    // or the corresponding read-only region
    old_dataset_id: Option<DbTypedUuid<DatasetKind>>,
    old_region_id: Uuid,
    old_snapshot_id: Option<Uuid>,

    /// A synthetic volume that only is used to later delete the old snapshot
    pub old_snapshot_volume_id: Option<DbTypedUuid<VolumeKind>>,

    pub new_region_id: Option<Uuid>,

    pub replacement_state: RegionSnapshotReplacementState,

    pub operating_saga_id: Option<Uuid>,

    /// In order for the newly created region not to be deleted inadvertently,
    /// an additional reference count bump is required. This volume should live
    /// as long as this request so that all necessary replacements can be
    /// completed.
    pub new_region_volume_id: Option<DbTypedUuid<VolumeKind>>,

    pub replacement_type: ReadOnlyTargetReplacementType,
}

impl RegionSnapshotReplacement {
    pub fn for_region_snapshot(region_snapshot: &RegionSnapshot) -> Self {
        Self::new_from_region_snapshot(
            region_snapshot.dataset_id(),
            region_snapshot.region_id,
            region_snapshot.snapshot_id,
        )
    }

    pub fn new_from_region_snapshot(
        old_dataset_id: DatasetUuid,
        old_region_id: Uuid,
        old_snapshot_id: Uuid,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            request_time: Utc::now(),
            old_dataset_id: Some(old_dataset_id.into()),
            old_region_id,
            old_snapshot_id: Some(old_snapshot_id),
            old_snapshot_volume_id: None,
            new_region_id: None,
            new_region_volume_id: None,
            replacement_state: RegionSnapshotReplacementState::Requested,
            operating_saga_id: None,
            replacement_type: ReadOnlyTargetReplacementType::RegionSnapshot,
        }
    }

    pub fn new_from_read_only_region(old_region_id: Uuid) -> Self {
        Self {
            id: Uuid::new_v4(),
            request_time: Utc::now(),
            old_dataset_id: None,
            old_region_id,
            old_snapshot_id: None,
            old_snapshot_volume_id: None,
            new_region_id: None,
            new_region_volume_id: None,
            replacement_state: RegionSnapshotReplacementState::Requested,
            operating_saga_id: None,
            replacement_type: ReadOnlyTargetReplacementType::ReadOnlyRegion,
        }
    }

    pub fn old_snapshot_volume_id(&self) -> Option<VolumeUuid> {
        self.old_snapshot_volume_id.map(|v| v.into())
    }

    pub fn new_region_volume_id(&self) -> Option<VolumeUuid> {
        self.new_region_volume_id.map(|v| v.into())
    }

    pub fn replacement_type(&self) -> ReadOnlyTargetReplacement {
        match &self.replacement_type {
            ReadOnlyTargetReplacementType::RegionSnapshot => {
                ReadOnlyTargetReplacement::RegionSnapshot {
                    dataset_id: self.old_dataset_id.unwrap(),
                    region_id: self.old_region_id,
                    snapshot_id: self.old_snapshot_id.unwrap(),
                }
            }

            ReadOnlyTargetReplacementType::ReadOnlyRegion => {
                ReadOnlyTargetReplacement::ReadOnlyRegion {
                    region_id: self.old_region_id,
                }
            }
        }
    }
}
