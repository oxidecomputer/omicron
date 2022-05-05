// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::schema::ssh_key;
use crate::external_api::params;
use db_macros::Resource;
use uuid::Uuid;

/// Describes a user's public SSH key within the database.
#[derive(Clone, Debug, Insertable, Queryable, Resource, Selectable)]
#[diesel(table_name = ssh_key)]
pub struct SshKey {
    #[diesel(embed)]
    identity: SshKeyIdentity,

    pub silo_user_id: Uuid,
    pub public_key: String,
}

impl SshKey {
    pub fn new(silo_user_id: Uuid, params: params::SshKeyCreate) -> Self {
        Self::new_with_id(Uuid::new_v4(), silo_user_id, params)
    }

    pub fn new_with_id(
        id: Uuid,
        silo_user_id: Uuid,
        params: params::SshKeyCreate,
    ) -> Self {
        Self {
            identity: SshKeyIdentity::new(id, params.identity),
            silo_user_id,
            public_key: params.public_key,
        }
    }
}
