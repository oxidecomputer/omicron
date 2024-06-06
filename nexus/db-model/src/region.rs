// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ByteCount;
use crate::schema::region;
use db_macros::Asset;
use omicron_common::api::external;
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

    dataset_id: Uuid,
    volume_id: Uuid,

    block_size: ByteCount,

    // These are i64 only so that we can derive a diesel table from them. We
    // never expect them to be negative.
    blocks_per_extent: i64,
    extent_count: i64,
}

impl Region {
    pub fn new(
        dataset_id: Uuid,
        volume_id: Uuid,
        block_size: ByteCount,
        blocks_per_extent: u64,
        extent_count: u64,
    ) -> Self {
        Self {
            identity: RegionIdentity::new(Uuid::new_v4()),
            dataset_id,
            volume_id,
            block_size,
            blocks_per_extent: blocks_per_extent as i64,
            extent_count: extent_count as i64,
        }
    }

    pub fn id(&self) -> Uuid {
        self.identity.id
    }
    pub fn volume_id(&self) -> Uuid {
        self.volume_id
    }
    pub fn dataset_id(&self) -> Uuid {
        self.dataset_id
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
}
