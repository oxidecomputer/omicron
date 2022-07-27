// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ByteCount;
use crate::schema::snapshot;
use db_macros::Resource;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    identity: SnapshotIdentity,

    pub project_id: Uuid,
    pub disk_id: Uuid,
    pub volume_id: Uuid,

    #[diesel(column_name = size_bytes)]
    pub size: ByteCount,
}

impl From<Snapshot> for views::Snapshot {
    fn from(snapshot: Snapshot) -> Self {
        Self {
            identity: snapshot.identity(),
            project_id: snapshot.project_id,
            disk_id: snapshot.disk_id,
            size: snapshot.size.into(),
        }
    }
}
