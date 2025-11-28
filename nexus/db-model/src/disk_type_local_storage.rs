// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::ByteCount;
use crate::typed_uuid::DbTypedUuid;
use nexus_db_schema::schema::disk_type_local_storage;
use omicron_common::api::external;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A Disk can be backed using a zvol slice from the local storage dataset
/// present on each zpool of a sled.
#[derive(
    Queryable, Insertable, Clone, Debug, Selectable, Serialize, Deserialize,
)]
#[diesel(table_name = disk_type_local_storage)]
pub struct DiskTypeLocalStorage {
    disk_id: Uuid,

    /// For zvols inside a parent dataset, there's an overhead that must be
    /// accounted for when setting a quota and reservation on that parent
    /// dataset. Record at model creation time how much overhead is required for
    /// the parent `local_storage` dataset slice in order to fit the child
    /// volume.
    required_dataset_overhead: ByteCount,

    local_storage_dataset_allocation_id: Option<DbTypedUuid<DatasetKind>>,
}

impl DiskTypeLocalStorage {
    /// Creates a new `DiskTypeLocalStorage`. Returns Err if the computed
    /// required dataset overhead does not fit in a `ByteCount`.
    pub fn new(
        disk_id: Uuid,
        size: external::ByteCount,
    ) -> Result<DiskTypeLocalStorage, external::ByteCountRangeError> {
        // For zvols, there's an overhead that must be accounted for, and it
        // empirically seems to be about 65M per 1G for volblocksize=4096.
        // Multiple the disk size by something a little over this value.

        let one_gb = external::ByteCount::from_gibibytes_u32(1).to_bytes();
        let gbs = size.to_bytes() / one_gb;
        let overhead: u64 =
            external::ByteCount::from_mebibytes_u32(70).to_bytes() * gbs;

        // Don't unwrap this - the size of this disk is a parameter set by an
        // API call, and we don't want to panic on out of range input.
        let required_dataset_overhead =
            external::ByteCount::try_from(overhead)?;

        Ok(DiskTypeLocalStorage {
            disk_id,
            required_dataset_overhead: required_dataset_overhead.into(),
            local_storage_dataset_allocation_id: None,
        })
    }

    pub fn required_dataset_overhead(&self) -> external::ByteCount {
        self.required_dataset_overhead.into()
    }

    pub fn local_storage_dataset_allocation_id(&self) -> Option<DatasetUuid> {
        self.local_storage_dataset_allocation_id.map(Into::into)
    }
}
