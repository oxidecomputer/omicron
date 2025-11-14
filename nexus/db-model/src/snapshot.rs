// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ByteCount, impl_enum_type};
use crate::BlockSize;
use crate::Generation;
use crate::typed_uuid::DbTypedUuid;
use db_macros::Resource;
use nexus_db_schema::schema::snapshot;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_uuid_kinds::VolumeKind;
use omicron_uuid_kinds::VolumeUuid;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    SnapshotStateEnum:

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum SnapshotState;

    Creating => b"creating"
    Ready => b"ready"
    Faulted => b"faulted"
    Destroyed => b"destroyed"
);

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = snapshot)]
pub struct Snapshot {
    #[diesel(embed)]
    pub identity: SnapshotIdentity,

    pub project_id: Uuid,
    // which disk is this a snapshot of
    pub disk_id: Uuid,
    pub volume_id: DbTypedUuid<VolumeKind>,

    // destination of all snapshot blocks
    pub destination_volume_id: DbTypedUuid<VolumeKind>,

    #[diesel(column_name = "gen")]
    #[serde(rename = "gen")]
    pub generation: Generation,
    pub state: SnapshotState,
    pub block_size: BlockSize,

    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,
}

impl From<Snapshot> for views::Snapshot {
    fn from(snapshot: Snapshot) -> Self {
        Self {
            identity: snapshot.identity(),
            project_id: snapshot.project_id,
            disk_id: snapshot.disk_id,
            state: snapshot.state.into(),
            size: snapshot.size.into(),
        }
    }
}

impl From<SnapshotState> for views::SnapshotState {
    fn from(state: SnapshotState) -> Self {
        match state {
            SnapshotState::Creating => Self::Creating,
            SnapshotState::Ready => Self::Ready,
            SnapshotState::Faulted => Self::Faulted,
            SnapshotState::Destroyed => Self::Destroyed,
        }
    }
}

impl Snapshot {
    pub fn volume_id(&self) -> VolumeUuid {
        self.volume_id.into()
    }

    pub fn destination_volume_id(&self) -> VolumeUuid {
        self.destination_volume_id.into()
    }
}
