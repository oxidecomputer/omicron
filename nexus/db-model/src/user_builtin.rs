// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use db_macros::Resource;
use nexus_db_schema::schema::user_builtin;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use omicron_uuid_kinds::BuiltInUserUuid;

/// Describes a built-in user, as stored in the database
#[derive(Queryable, Insertable, Debug, Resource, Selectable)]
#[diesel(table_name = user_builtin)]
#[resource(uuid_kind = BuiltInUserKind)]
pub struct UserBuiltin {
    #[diesel(embed)]
    pub identity: UserBuiltinIdentity,
}

impl UserBuiltin {
    /// Creates a new database UserBuiltin object.
    pub fn new(id: BuiltInUserUuid, params: params::UserBuiltinCreate) -> Self {
        Self { identity: UserBuiltinIdentity::new(id, params.identity) }
    }
}

impl From<UserBuiltin> for views::UserBuiltin {
    fn from(user: UserBuiltin) -> Self {
        Self { identity: user.identity() }
    }
}
