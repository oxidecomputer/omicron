// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in users

use omicron_common::api;
use omicron_uuid_kinds::BuiltInUserUuid;
use std::sync::LazyLock;

pub struct UserBuiltinConfig {
    pub id: BuiltInUserUuid,
    pub name: api::external::Name,
    pub description: &'static str,
}

impl UserBuiltinConfig {
    fn new_static(
        id: &str,
        name: &str,
        description: &'static str,
    ) -> UserBuiltinConfig {
        UserBuiltinConfig {
            id: id
                .parse()
                .expect("invalid built-in user uuid for builtin user id"),
            name: name.parse().expect("invalid name for builtin user name"),
            description,
        }
    }
}

/// Internal user used for seeding initial database data
// NOTE: This uuid and name are duplicated in dbinit.sql.
pub static USER_DB_INIT: LazyLock<UserBuiltinConfig> = LazyLock::new(|| {
    UserBuiltinConfig::new_static(
        // "0001" is the first possible user that wouldn't be confused with
        // 0, or root.
        "001de000-05e4-4000-8000-000000000001",
        "db-init",
        "used for seeding initial database data",
    )
});

/// Internal user for performing operations to manage the
/// provisioning of services across the fleet.
pub static USER_SERVICE_BALANCER: LazyLock<UserBuiltinConfig> =
    LazyLock::new(|| {
        UserBuiltinConfig::new_static(
            "001de000-05e4-4000-8000-00000000bac3",
            "service-balancer",
            "used for Nexus-driven service balancing",
        )
    });

/// Internal user used by Nexus when handling internal API requests
pub static USER_INTERNAL_API: LazyLock<UserBuiltinConfig> =
    LazyLock::new(|| {
        UserBuiltinConfig::new_static(
            "001de000-05e4-4000-8000-000000000002",
            "internal-api",
            "used by Nexus when handling internal API requests",
        )
    });

/// Internal user used by Nexus to read privileged control plane data
pub static USER_INTERNAL_READ: LazyLock<UserBuiltinConfig> =
    LazyLock::new(|| {
        UserBuiltinConfig::new_static(
            // "4ead" looks like "read"
            "001de000-05e4-4000-8000-000000004ead",
            "internal-read",
            "used by Nexus to read privileged control plane data",
        )
    });

/// Internal user used by Nexus when recovering sagas
pub static USER_SAGA_RECOVERY: LazyLock<UserBuiltinConfig> =
    LazyLock::new(|| {
        UserBuiltinConfig::new_static(
            // "3a8a" looks a bit like "saga".
            "001de000-05e4-4000-8000-000000003a8a",
            "saga-recovery",
            "used by Nexus when recovering sagas",
        )
    });

/// Internal user used by Nexus when authenticating external requests
pub static USER_EXTERNAL_AUTHN: LazyLock<UserBuiltinConfig> =
    LazyLock::new(|| {
        UserBuiltinConfig::new_static(
            "001de000-05e4-4000-8000-000000000003",
            "external-authn",
            "used by Nexus when authenticating external requests",
        )
    });

#[cfg(test)]
mod test {
    use super::super::assert_valid_typed_uuid;
    use super::USER_DB_INIT;
    use super::USER_EXTERNAL_AUTHN;
    use super::USER_INTERNAL_API;
    use super::USER_INTERNAL_READ;
    use super::USER_SAGA_RECOVERY;
    use super::USER_SERVICE_BALANCER;

    #[test]
    fn test_builtin_user_ids_are_valid() {
        assert_valid_typed_uuid(&USER_SERVICE_BALANCER.id);
        assert_valid_typed_uuid(&USER_DB_INIT.id);
        assert_valid_typed_uuid(&USER_INTERNAL_API.id);
        assert_valid_typed_uuid(&USER_EXTERNAL_AUTHN.id);
        assert_valid_typed_uuid(&USER_INTERNAL_READ.id);
        assert_valid_typed_uuid(&USER_SAGA_RECOVERY.id);
    }
}
