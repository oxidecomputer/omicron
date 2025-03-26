// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ByteCount;
use crate::SqlU16;
use crate::schema::region;
use crate::typed_uuid::DbTypedUuid;
use db_macros::Asset;
use omicron_common::api::external;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::VolumeKind;
use omicron_uuid_kinds::VolumeUuid;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Database representation of a Region.
///
/// A region represents a portion of a Crucible Downstairs dataset
/// allocated within a volume.
#[derive(
    Queryable,
    Insertable,
    Debug,
    Clone,
    Selectable,
    Asset,
    Serialize,
    Deserialize,
    PartialEq,
)]
#[diesel(table_name = region)]
pub struct Region {
    #[diesel(embed)]
    identity: RegionIdentity,

    dataset_id: DbTypedUuid<DatasetKind>,
    volume_id: DbTypedUuid<VolumeKind>,

    block_size: ByteCount,

    // These are i64 only so that we can derive a diesel table from them. We
    // never expect them to be negative.
    blocks_per_extent: i64,
    extent_count: i64,

    // The port that was returned when the region was created. This field didn't
    // originally exist, so records may not have it filled in.
    port: Option<SqlU16>,

    // A region may be read-only
    read_only: bool,

    // Shared read-only regions require a "deleting" flag to avoid a
    // use-after-free scenario
    deleting: bool,

    // The Agent will reserve space for Downstairs overhead when creating the
    // corresponding ZFS dataset. Nexus has to account for that: store that
    // reservation factor here as it may change in the future, and it can be
    // used during Crucible related accounting.
    reservation_factor: f64,
}

impl Region {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dataset_id: DatasetUuid,
        volume_id: VolumeUuid,
        block_size: ByteCount,
        blocks_per_extent: u64,
        extent_count: u64,
        port: u16,
        read_only: bool,
        reservation_factor: f64,
    ) -> Self {
        Self {
            identity: RegionIdentity::new(Uuid::new_v4()),
            dataset_id: dataset_id.into(),
            volume_id: volume_id.into(),
            block_size,
            blocks_per_extent: blocks_per_extent as i64,
            extent_count: extent_count as i64,
            port: Some(port.into()),
            read_only,
            deleting: false,
            reservation_factor,
        }
    }

    pub fn id(&self) -> Uuid {
        self.identity.id
    }
    pub fn volume_id(&self) -> VolumeUuid {
        self.volume_id.into()
    }
    pub fn dataset_id(&self) -> DatasetUuid {
        self.dataset_id.into()
    }
    pub fn block_size(&self) -> external::ByteCount {
        self.block_size.0
    }
    pub fn blocks_per_extent(&self) -> u64 {
        self.blocks_per_extent as u64
    }
    pub fn extent_count(&self) -> u64 {
        self.extent_count as u64
    }
    pub fn encrypted(&self) -> bool {
        // Per RFD 29, data is always encrypted at rest, and support for
        // external, customer-supplied keys is a non-requirement.
        true
    }
    pub fn port(&self) -> Option<u16> {
        self.port.map(|port| port.into())
    }
    pub fn read_only(&self) -> bool {
        self.read_only
    }
    pub fn deleting(&self) -> bool {
        self.deleting
    }

    /// The size of the Region without accounting for any overhead
    pub fn requested_size(&self) -> u64 {
        self.block_size().to_bytes()
            * self.blocks_per_extent()
            * self.extent_count()
    }

    /// The size the Crucible agent would have reserved during ZFS creation,
    /// which is some factor higher than the requested region size to account
    /// for on-disk overhead.
    pub fn reserved_size(&self) -> u64 {
        (self.requested_size() as f64 * self.reservation_factor).round() as u64
    }
}
