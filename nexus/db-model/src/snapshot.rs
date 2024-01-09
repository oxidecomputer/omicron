// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{impl_enum_type, ByteCount};
use crate::schema::snapshot;
use crate::BlockSize;
use crate::Generation;
use db_macros::Resource;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "snapshot_state", schema = "public"))]
    pub struct SnapshotStateEnum;

    #[derive(Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = SnapshotStateEnum)]
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
    pub volume_id: Uuid,

    // destination of all snapshot blocks
    pub destination_volume_id: Uuid,

    pub gen: Generation,
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
