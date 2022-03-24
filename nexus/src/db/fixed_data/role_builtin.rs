// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in roles

use lazy_static::lazy_static;
use omicron_common::api;

#[derive(Clone)]
pub struct RoleBuiltinConfig {
    pub resource_type: api::external::ResourceType,
    pub role_name: &'static str,
    pub description: &'static str,
}

lazy_static! {
    pub static ref FLEET_ADMIN: RoleBuiltinConfig = RoleBuiltinConfig {
        resource_type: api::external::ResourceType::Fleet,
        role_name: "admin",
        description: "Fleet Administrator",
    };
    pub static ref FLEET_VIEWER: RoleBuiltinConfig = RoleBuiltinConfig {
        resource_type: api::external::ResourceType::Fleet,
        role_name: "viewer",
        description: "Fleet Viewer",
    };
    pub static ref BUILTIN_ROLES: Vec<RoleBuiltinConfig> = vec![
        FLEET_ADMIN.clone(),
        FLEET_VIEWER.clone(),
        RoleBuiltinConfig {
            resource_type: api::external::ResourceType::Fleet,
            role_name: "collaborator",
            description: "Fleet Collaborator",
        },
        RoleBuiltinConfig {
            resource_type: api::external::ResourceType::Organization,
            role_name: "admin",
            description: "Organization Administrator",
        },
        RoleBuiltinConfig {
            resource_type: api::external::ResourceType::Organization,
            role_name: "collaborator",
            description: "Organization Collaborator",
        },
        RoleBuiltinConfig {
            resource_type: api::external::ResourceType::Project,
            role_name: "admin",
            description: "Project Administrator",
        },
        RoleBuiltinConfig {
            resource_type: api::external::ResourceType::Project,
            role_name: "collaborator",
            description: "Project Collaborator",
        },
        RoleBuiltinConfig {
            resource_type: api::external::ResourceType::Project,
            role_name: "viewer",
            description: "Project Viewer",
        },
    ];
}
