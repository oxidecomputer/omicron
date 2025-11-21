// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::typed_uuid::DbTypedUuid;
use nexus_db_schema::schema::disk_type_crucible;
use nexus_types::external_api::params;
use omicron_uuid_kinds::VolumeKind;
use omicron_uuid_kinds::VolumeUuid;
use serde::{Deserialize, Serialize};
use std::net::SocketAddrV6;
use uuid::Uuid;

/// A Disk can be backed using Crucible, a distributed network-replicated block
/// storage service.
#[derive(
    Queryable, Insertable, Clone, Debug, Selectable, Serialize, Deserialize,
)]
#[diesel(table_name = disk_type_crucible)]
pub struct DiskTypeCrucible {
    disk_id: Uuid,

    /// Root volume of the disk
    volume_id: DbTypedUuid<VolumeKind>,

    /// id for the snapshot from which this Disk was created (None means a blank
    /// disk)
    #[diesel(column_name = origin_snapshot)]
    pub create_snapshot_id: Option<Uuid>,

    /// id for the image from which this Disk was created (None means a blank
    /// disk)
    #[diesel(column_name = origin_image)]
    pub create_image_id: Option<Uuid>,

    /// If this disk is attached to a Pantry for longer than the lifetime of a
    /// saga, then this field will contain the serialized SocketAddrV6 of that
    /// Pantry.
    pub pantry_address: Option<String>,
}

impl DiskTypeCrucible {
    pub fn new(
        disk_id: Uuid,
        volume_id: VolumeUuid,
        params: &params::DiskCreate,
    ) -> Self {
        let create_snapshot_id = match params.disk_source {
            params::DiskSource::Snapshot { snapshot_id } => Some(snapshot_id),
            _ => None,
        };

        // XXX further enum here for different image types?
        let create_image_id = match params.disk_source {
            params::DiskSource::Image { image_id } => Some(image_id),
            _ => None,
        };

        Self {
            disk_id,
            volume_id: volume_id.into(),
            create_snapshot_id,
            create_image_id,
            pantry_address: None,
        }
    }

    pub fn pantry_address(&self) -> Option<SocketAddrV6> {
        self.pantry_address.as_ref().map(|x| x.parse().unwrap())
    }

    pub fn volume_id(&self) -> VolumeUuid {
        self.volume_id.into()
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = disk_type_crucible)]
#[diesel(treat_none_as_null = true)]
pub struct DiskTypeCrucibleUpdate {
    pub pantry_address: Option<String>,
}
