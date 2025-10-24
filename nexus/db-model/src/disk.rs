// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::BlockSize;
use super::ByteCount;
use super::DiskState;
use super::Generation;
use super::impl_enum_type;
use crate::unsigned::SqlU8;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use nexus_db_schema::schema::disk;
use nexus_types::external_api::params;
use omicron_common::api::external;
use omicron_common::api::internal;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use uuid::Uuid;

impl_enum_type!(
    DiskTypeEnum:

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum DiskType;

    // Enum values
    Crucible => b"crucible"
    LocalStorage => b"local_storage"
);

/// A Disk, where how the blocks are stored depend on the disk_type.
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

    /// Information unique to each type of disk is stored in a separate table
    /// (where rows are matched based on the disk_id field in that table) and
    /// combined into a higher level `datastore::Disk` enum.
    ///
    /// For `Crucible` disks, see the DiskTypeCrucible model. For `LocalStorage`
    /// disks, see the DiskTypeLocalStorage model.
    pub disk_type: DiskType,
}

impl Disk {
    pub fn new(
        disk_id: Uuid,
        project_id: Uuid,
        params: &params::DiskCreate,
        block_size: BlockSize,
        runtime_initial: DiskRuntimeState,
        disk_type: DiskType,
    ) -> Self {
        let identity = DiskIdentity::new(disk_id, params.identity().clone());

        Self {
            identity,
            rcgen: external::Generation::new().into(),
            project_id,
            runtime_state: runtime_initial,
            slot: None,
            size: params.size().into(),
            block_size,
            disk_type,
        }
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

    pub fn slot(&self) -> Option<u8> {
        self.slot.map(Into::into)
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
    pub gen: Generation,
    /// timestamp for this information
    #[diesel(column_name = time_state_updated)]
    pub time_updated: DateTime<Utc>,
}

impl DiskRuntimeState {
    pub fn new() -> Self {
        Self {
            disk_state: external::DiskState::Creating.label().to_string(),
            attach_instance_id: None,
            gen: external::Generation::new().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn attach(self, instance_id: Uuid) -> Self {
        Self {
            disk_state: external::DiskState::Attached(instance_id)
                .label()
                .to_string(),
            attach_instance_id: Some(instance_id),
            gen: self.gen.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn detach(self) -> Self {
        Self {
            disk_state: external::DiskState::Detached.label().to_string(),
            attach_instance_id: None,
            gen: self.gen.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn maintenance(self) -> Self {
        Self {
            disk_state: external::DiskState::Maintenance.label().to_string(),
            attach_instance_id: None,
            gen: self.gen.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn import_ready(self) -> Self {
        Self {
            disk_state: external::DiskState::ImportReady.label().to_string(),
            attach_instance_id: None,
            gen: self.gen.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn importing_from_url(self) -> Self {
        Self {
            disk_state: external::DiskState::ImportingFromUrl
                .label()
                .to_string(),
            attach_instance_id: None,
            gen: self.gen.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn importing_from_bulk_writes(self) -> Self {
        Self {
            disk_state: external::DiskState::ImportingFromBulkWrites
                .label()
                .to_string(),
            attach_instance_id: None,
            gen: self.gen.next().into(),
            time_updated: Utc::now(),
        }
    }

    pub fn finalizing(self) -> Self {
        Self {
            disk_state: external::DiskState::Finalizing.label().to_string(),
            attach_instance_id: None,
            gen: self.gen.next().into(),
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
            gen: self.gen.next().into(),
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
            gen: runtime.gen.into(),
            time_updated: runtime.time_updated,
        }
    }
}

/// Conversion to the internal API type.
impl Into<internal::nexus::DiskRuntimeState> for DiskRuntimeState {
    fn into(self) -> internal::nexus::DiskRuntimeState {
        internal::nexus::DiskRuntimeState {
            disk_state: self.state().into(),
            gen: self.gen.into(),
            time_updated: self.time_updated,
        }
    }
}
