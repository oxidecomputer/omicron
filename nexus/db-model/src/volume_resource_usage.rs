// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::typed_uuid::DbTypedUuid;
use nexus_db_schema::schema::volume_resource_usage;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::VolumeKind;
use omicron_uuid_kinds::VolumeUuid;
use uuid::Uuid;

impl_enum_type!(
    VolumeResourceUsageTypeEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq, Eq, Hash)]
    pub enum VolumeResourceUsageType;

    ReadOnlyRegion => b"read_only_region"
    RegionSnapshot => b"region_snapshot"
);

/// Crucible volumes are created by layering read-write regions over a hierarchy
/// of read-only resources. Originally only a region snapshot could be used as a
/// read-only resource for a volume. With the introduction of read-only regions
/// (created during the region snapshot replacement process) this is no longer
/// true.
///
/// Read-only resources can be used by many volumes, and because of this they
/// need to have a reference count so they can be deleted when they're not
/// referenced anymore. The region_snapshot table used a `volume_references`
/// column, which counts how many uses there are. The region table does not have
/// this column, and more over a simple integer works for reference counting but
/// does not tell you _what_ volume that use is from. This can be determined
/// (see omdb's validate volume references command) but it's information that is
/// tossed out, as Nexus knows what volumes use what resources! Instead of
/// throwing away that knowledge and only incrementing and decrementing an
/// integer, record what read-only resources a volume uses in this table.
///
/// Note: users should not use this object directly, and instead use the
/// [`VolumeResourceUsage`] enum, which is type-safe and will convert to and
/// from a [`VolumeResourceUsageRecord`] when interacting with the DB.
#[derive(
    Queryable, Insertable, Debug, Clone, Selectable, PartialEq, Eq, Hash,
)]
#[diesel(table_name = volume_resource_usage)]
pub struct VolumeResourceUsageRecord {
    pub usage_id: Uuid,

    pub volume_id: DbTypedUuid<VolumeKind>,

    pub usage_type: VolumeResourceUsageType,

    pub region_id: Option<Uuid>,

    pub region_snapshot_dataset_id: Option<DbTypedUuid<DatasetKind>>,
    pub region_snapshot_region_id: Option<Uuid>,
    pub region_snapshot_snapshot_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub enum VolumeResourceUsage {
    ReadOnlyRegion {
        region_id: Uuid,
    },

    RegionSnapshot {
        dataset_id: DatasetUuid,
        region_id: Uuid,
        snapshot_id: Uuid,
    },
}

impl VolumeResourceUsageRecord {
    pub fn new(volume_id: VolumeUuid, usage: VolumeResourceUsage) -> Self {
        match usage {
            VolumeResourceUsage::ReadOnlyRegion { region_id } => {
                VolumeResourceUsageRecord {
                    usage_id: Uuid::new_v4(),
                    volume_id: volume_id.into(),
                    usage_type: VolumeResourceUsageType::ReadOnlyRegion,

                    region_id: Some(region_id),

                    region_snapshot_dataset_id: None,
                    region_snapshot_region_id: None,
                    region_snapshot_snapshot_id: None,
                }
            }

            VolumeResourceUsage::RegionSnapshot {
                dataset_id,
                region_id,
                snapshot_id,
            } => VolumeResourceUsageRecord {
                usage_id: Uuid::new_v4(),
                volume_id: volume_id.into(),
                usage_type: VolumeResourceUsageType::RegionSnapshot,

                region_id: None,

                region_snapshot_dataset_id: Some(dataset_id.into()),
                region_snapshot_region_id: Some(region_id),
                region_snapshot_snapshot_id: Some(snapshot_id),
            },
        }
    }

    pub fn volume_id(&self) -> VolumeUuid {
        self.volume_id.into()
    }
}

impl TryFrom<VolumeResourceUsageRecord> for VolumeResourceUsage {
    type Error = String;

    fn try_from(
        record: VolumeResourceUsageRecord,
    ) -> Result<VolumeResourceUsage, String> {
        match record.usage_type {
            VolumeResourceUsageType::ReadOnlyRegion => {
                let Some(region_id) = record.region_id else {
                    return Err("valid read-only region usage record".into());
                };

                Ok(VolumeResourceUsage::ReadOnlyRegion { region_id })
            }

            VolumeResourceUsageType::RegionSnapshot => {
                let Some(dataset_id) = record.region_snapshot_dataset_id else {
                    return Err("valid region snapshot usage record".into());
                };

                let Some(region_id) = record.region_snapshot_region_id else {
                    return Err("valid region snapshot usage record".into());
                };

                let Some(snapshot_id) = record.region_snapshot_snapshot_id
                else {
                    return Err("valid region snapshot usage record".into());
                };

                Ok(VolumeResourceUsage::RegionSnapshot {
                    dataset_id: dataset_id.into(),
                    region_id,
                    snapshot_id,
                })
            }
        }
    }
}
