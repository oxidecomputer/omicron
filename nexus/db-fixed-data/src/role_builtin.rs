// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in roles

use omicron_common::api;
use std::sync::LazyLock;

#[derive(Clone, Debug)]
pub struct RoleBuiltinConfig {
    pub resource_type: api::external::ResourceType,
    pub role_name: &'static str,
    pub description: &'static str,
}

pub static FLEET_ADMIN: LazyLock<RoleBuiltinConfig> =
    LazyLock::new(|| RoleBuiltinConfig {
        resource_type: api::external::ResourceType::Fleet,
        role_name: "admin",
        description: "Fleet Administrator",
    });

pub static FLEET_AUTHENTICATOR: LazyLock<RoleBuiltinConfig> =
    LazyLock::new(|| RoleBuiltinConfig {
        resource_type: api::external::ResourceType::Fleet,
        role_name: "external-authenticator",
        description: "Fleet External Authenticator",
    });

pub static FLEET_VIEWER: LazyLock<RoleBuiltinConfig> =
    LazyLock::new(|| RoleBuiltinConfig {
        resource_type: api::external::ResourceType::Fleet,
        role_name: "viewer",
        description: "Fleet Viewer",
    });

pub static SILO_ADMIN: LazyLock<RoleBuiltinConfig> =
    LazyLock::new(|| RoleBuiltinConfig {
        resource_type: api::external::ResourceType::Silo,
        role_name: "admin",
        description: "Silo Administrator",
    });
