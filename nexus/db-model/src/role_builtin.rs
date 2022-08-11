// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::role_builtin;
use nexus_types::external_api::views;
use omicron_common::api::external::RoleName;

/// Describes a built-in role, as stored in the database
#[derive(Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = role_builtin)]
pub struct RoleBuiltin {
    pub resource_type: String,
    pub role_name: String,
    pub description: String,
}

impl RoleBuiltin {
    /// Creates a new database UserBuiltin object.
    pub fn new(
        resource_type: omicron_common::api::external::ResourceType,
        role_name: &str,
        description: &str,
    ) -> Self {
        Self {
            resource_type: resource_type.to_string(),
            role_name: String::from(role_name),
            description: String::from(description),
        }
    }

    pub fn id(&self) -> (String, String) {
        (self.resource_type.clone(), self.role_name.clone())
    }
}

impl From<RoleBuiltin> for views::Role {
    fn from(role: RoleBuiltin) -> Self {
        Self {
            name: RoleName::new(&role.resource_type, &role.role_name),
            description: role.description,
        }
    }
}
