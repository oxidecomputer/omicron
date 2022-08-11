// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::user_builtin;
use db_macros::Resource;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use uuid::Uuid;

/// Describes a built-in user, as stored in the database
#[derive(Queryable, Insertable, Debug, Resource, Selectable)]
#[diesel(table_name = user_builtin)]
pub struct UserBuiltin {
    #[diesel(embed)]
    pub identity: UserBuiltinIdentity,
}

impl UserBuiltin {
    /// Creates a new database UserBuiltin object.
    pub fn new(id: Uuid, params: params::UserBuiltinCreate) -> Self {
        Self { identity: UserBuiltinIdentity::new(id, params.identity) }
    }
}

impl From<UserBuiltin> for views::UserBuiltin {
    fn from(user: UserBuiltin) -> Self {
        Self { identity: user.identity() }
    }
}
