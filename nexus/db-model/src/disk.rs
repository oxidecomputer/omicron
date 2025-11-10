// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{BlockSize, ByteCount, DiskState, Generation};
use crate::typed_uuid::DbTypedUuid;
use crate::unsigned::SqlU8;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use nexus_db_schema::schema::disk;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_common::api::internal;
use omicron_uuid_kinds::VolumeKind;
use omicron_uuid_kinds::VolumeUuid;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::net::SocketAddrV6;
use uuid::Uuid;

/// A Disk (network block device).
#[derive(
    Queryable,
    Insertable,
    Clone,
    Debug,
    Selectable,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = disk)]
pub struct Disk {
    #[diesel(embed)]
    identity: DiskIdentity,

    /// child resource generation number, per RFD 192
    rcgen: Generation,

    /// id for the project containing this Disk
    pub project_id: Uuid,

    /// Root volume of the disk
    volume_id: DbTypedUuid<VolumeKind>,

    /// runtime state of the Disk
    #[diesel(embed)]
    pub runtime_state: DiskRuntimeState,

    /// The PCI slot (within the bank of slots reserved to disks) to which this
    /// disk should be attached if its attached instance is started, or None
    /// if there is no such assignment.
    ///
    /// Slot assignments are managed entirely in Nexus and aren't modified by
    /// runtime state changes in the sled agent, so this field is part of the
    /// "main" disk struct and not the runtime state (even though the attachment
    /// state and slot assignment will often change together).
    pub slot: Option<SqlU8>,

    /// size of the Disk
    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,

    /// size of blocks (512, 2048, or 4096)
    pub block_size: BlockSize,

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

impl Disk {
    pub fn new(
        disk_id: Uuid,
        project_id: Uuid,
        volume_id: VolumeUuid,
        params: params::DiskCreate,
        block_size: BlockSize,
        runtime_initial: DiskRuntimeState,
    ) -> Result<Self, anyhow::Error> {
        let identity = DiskIdentity::new(disk_id, params.identity);

        let create_snapshot_id = match params.disk_source {
            params::DiskSource::Snapshot { snapshot_id } => Some(snapshot_id),
            _ => None,
        };

        // XXX further enum here for different image types?
        let create_image_id = match params.disk_source {
            params::DiskSource::Image { image_id } => Some(image_id),
            _ => None,
        };

        Ok(Self {
            identity,
            rcgen: external::Generation::new().into(),
            project_id,
            volume_id: volume_id.into(),
            runtime_state: runtime_initial,
            slot: None,
            size: params.size.into(),
            block_size,
            create_snapshot_id,
            create_image_id,
            pantry_address: None,
        })
    }

    pub fn state(&self) -> DiskState {
        self.runtime_state.state()
    }

    pub fn runtime(&self) -> DiskRuntimeState {
        self.runtime_state.clone()
    }

    pub fn id(&self) -> Uuid {
        self.identity.id
    }

    pub fn pantry_address(&self) -> Option<SocketAddrV6> {
        self.pantry_address.as_ref().map(|x| x.parse().unwrap())
    }

    pub fn volume_id(&self) -> VolumeUuid {
        self.volume_id.into()
    }
}

/// Conversion to the external API type.
impl Into<external::Disk> for Disk {
    fn into(self) -> external::Disk {
        let device_path = format!("/mnt/{}", self.name().as_str());
        external::Disk {
            identity: self.identity(),
            project_id: self.project_id,
            snapshot_id: self.create_snapshot_id,
            image_id: self.create_image_id,
            size: self.size.into(),
            block_size: self.block_size.into(),
            state: self.state().into(),
            device_path,
        }
    }
}

#[derive(
    AsChangeset,
    Clone,
    Debug,
    Queryable,
    Insertable,
    Selectable,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = disk)]
// When "attach_instance_id" is set to None, we'd like to
// clear it from the DB, rather than ignore the update.
#[diesel(treat_none_as_null = true)]
pub struct DiskRuntimeState {
    /// runtime state of the Disk
    pub disk_state: String,
    pub attach_instance_id: Option<Uuid>,
    /// generation number for this state
    #[diesel(column_name = state_generation)]
    #[serde(rename = "gen")]
    pub generation: Generation,
    /// timestamp for this information
    #[diesel(column_name = time_state_updated)]
    pub time_updated: DateTime<Utc>,
}

impl DiskRuntimeState {
    pub fn new() -> Self {
        Self {
            disk_state: external::DiskState::Creating.label().to_string(),
            attach_instance_id: None,
            generation: external::Generation::new().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn attach(self, instance_id: Uuid) -> Self {
        Self {
            disk_state: external::DiskState::Attached(instance_id)
                .label()
                .to_string(),
            attach_instance_id: Some(instance_id),
            generation: self.generation.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn detach(self) -> Self {
        Self {
            disk_state: external::DiskState::Detached.label().to_string(),
            attach_instance_id: None,
            generation: self.generation.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn maintenance(self) -> Self {
        Self {
            disk_state: external::DiskState::Maintenance.label().to_string(),
            attach_instance_id: None,
            generation: self.generation.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn import_ready(self) -> Self {
        Self {
            disk_state: external::DiskState::ImportReady.label().to_string(),
            attach_instance_id: None,
            generation: self.generation.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn importing_from_url(self) -> Self {
        Self {
            disk_state: external::DiskState::ImportingFromUrl
                .label()
                .to_string(),
            attach_instance_id: None,
            generation: self.generation.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn importing_from_bulk_writes(self) -> Self {
        Self {
            disk_state: external::DiskState::ImportingFromBulkWrites
                .label()
                .to_string(),
            attach_instance_id: None,
            generation: self.generation.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn finalizing(self) -> Self {
        Self {
            disk_state: external::DiskState::Finalizing.label().to_string(),
            attach_instance_id: None,
            generation: self.generation.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn state(&self) -> DiskState {
        // TODO: If we could store disk state in-line, we could avoid the
        // unwrap. Would prefer to parse it as such.
        DiskState::new(
            external::DiskState::try_from((
                self.disk_state.as_str(),
                self.attach_instance_id,
            ))
            .unwrap(),
        )
    }

    pub fn faulted(self) -> Self {
        Self {
            disk_state: external::DiskState::Faulted.label().to_string(),
            attach_instance_id: None,
            generation: self.generation.next().into(),
            time_updated: Utc::now(),
        }
    }
}

/// Conversion from the internal API type.
impl From<internal::nexus::DiskRuntimeState> for DiskRuntimeState {
    fn from(runtime: internal::nexus::DiskRuntimeState) -> Self {
        Self {
            disk_state: runtime.disk_state.label().to_string(),
            attach_instance_id: runtime
                .disk_state
                .attached_instance_id()
                .map(|id| *id),
            generation: runtime.generation.into(),
            time_updated: runtime.time_updated,
        }
    }
}

/// Conversion to the internal API type.
impl Into<internal::nexus::DiskRuntimeState> for DiskRuntimeState {
    fn into(self) -> internal::nexus::DiskRuntimeState {
        internal::nexus::DiskRuntimeState {
            disk_state: self.state().into(),
            generation: self.generation.into(),
            time_updated: self.time_updated,
        }
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = disk)]
#[diesel(treat_none_as_null = true)]
pub struct DiskUpdate {
    pub pantry_address: Option<String>,
}
