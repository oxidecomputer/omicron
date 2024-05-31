// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in roles

use omicron_common::api;
use once_cell::sync::Lazy;

#[derive(Clone, Debug)]
pub struct RoleBuiltinConfig {
    pub resource_type: api::external::ResourceType,
    pub role_name: &'static str,
    pub description: &'static str,
}

pub static FLEET_ADMIN: Lazy<RoleBuiltinConfig> =
    Lazy::new(|| RoleBuiltinConfig {
        resource_type: api::external::ResourceType::Fleet,
        role_name: "admin",
        description: "Fleet Administrator",
    });

pub static FLEET_AUTHENTICATOR: Lazy<RoleBuiltinConfig> =
    Lazy::new(|| RoleBuiltinConfig {
        resource_type: api::external::ResourceType::Fleet,
        role_name: "external-authenticator",
        description: "Fleet External Authenticator",
    });

pub static FLEET_VIEWER: Lazy<RoleBuiltinConfig> =
    Lazy::new(|| RoleBuiltinConfig {
        resource_type: api::external::ResourceType::Fleet,
        role_name: "viewer",
        description: "Fleet Viewer",
    });

pub static SILO_ADMIN: Lazy<RoleBuiltinConfig> =
    Lazy::new(|| RoleBuiltinConfig {
        resource_type: api::external::ResourceType::Silo,
        role_name: "admin",
        description: "Silo Administrator",
    });

pub static BUILTIN_ROLES: Lazy<Vec<RoleBuiltinConfig>> = Lazy::new(|| {
    vec![
        FLEET_ADMIN.clone(),
        FLEET_AUTHENTICATOR.clone(),
        FLEET_VIEWER.clone(),
        RoleBuiltinConfig {
            resource_type: api::external::ResourceType::Fleet,
            role_name: "collaborator",
            description: "Fleet Collaborator",
        },
        SILO_ADMIN.clone(),
        RoleBuiltinConfig {
            resource_type: api::external::ResourceType::Silo,
            role_name: "collaborator",
            description: "Silo Collaborator",
        },
        RoleBuiltinConfig {
            resource_type: api::external::ResourceType::Silo,
            role_name: "viewer",
            description: "Silo Viewer",
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
    ]
});

#[cfg(test)]
mod test {
    use super::BUILTIN_ROLES;
    use nexus_db_model::DatabaseString;
    use nexus_types::external_api::shared::{FleetRole, ProjectRole, SiloRole};
    use omicron_common::api::external::ResourceType;
    use strum::IntoEnumIterator;

    #[test]
    fn test_fixed_role_data() {
        // Every role that's defined in the public API as assignable on a
        // resource must have a corresponding entry in BUILTIN_ROLES above.
        // The reverse is not necessarily true because we have some internal
        // roles that are not exposed to end users.
        check_public_roles::<FleetRole>(ResourceType::Fleet);
        check_public_roles::<SiloRole>(ResourceType::Silo);
        check_public_roles::<ProjectRole>(ResourceType::Project);
    }

    fn check_public_roles<T>(resource_type: ResourceType)
    where
        T: std::fmt::Debug + DatabaseString + IntoEnumIterator,
    {
        for variant in T::iter() {
            let role_name = variant.to_database_string();

            let found = BUILTIN_ROLES.iter().find(|role_config| {
                role_config.resource_type == resource_type
                    && role_config.role_name == role_name
            });
            if let Some(found_config) = found {
                println!(
                    "variant: {:?} found fixed data {:?}",
                    variant, found_config
                );
            } else {
                panic!(
                    "found public role {:?} on {:?} with no corresponding \
                    built-in role",
                    role_name, resource_type
                );
            }
        }
    }
}
