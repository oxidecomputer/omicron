// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in users

use crate::authn;
use lazy_static::lazy_static;
use omicron_common::api;
use uuid::Uuid;

pub struct UserBuiltinConfig {
    pub id: Uuid,
    pub name: api::external::Name,
    pub description: &'static str,
}

impl UserBuiltinConfig {
    fn new_static(
        id: Uuid,
        name: &str,
        description: &'static str,
    ) -> UserBuiltinConfig {
        UserBuiltinConfig {
            id,
            name: name.parse().expect("invalid name for builtin user name"),
            description,
        }
    }
}

lazy_static! {
    /// Internal user used for seeding initial database data
    // NOTE: This name (and the UUID from authn) are duplicated in dbinit.sql.
    pub static ref USER_DB_INIT: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            *authn::USER_DB_INIT_ID,
            "db-init",
            "used for seeding initial database data",
        );

    /// Internal user for performing operations to manage the
    /// provisioning of services across the fleet.
    pub static ref USER_SERVICE_BALANCER: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            *authn::USER_SERVICE_BALANCER_ID,
            "service-balancer",
            "used for Nexus-driven service balancing",
        );

    /// Internal user used by Nexus when handling internal API requests
    pub static ref USER_INTERNAL_API: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            *authn::USER_INTERNAL_API_ID,
            "internal-api",
            "used by Nexus when handling internal API requests",
        );

    /// Internal user used by Nexus to read privileged control plane data
    pub static ref USER_INTERNAL_READ: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            *authn::USER_INTERNAL_READ_ID,
            "internal-read",
            "used by Nexus to read privileged control plane data",
        );

    /// Internal user used by Nexus when recovering sagas
    pub static ref USER_SAGA_RECOVERY: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            *authn::USER_SAGA_RECOVERY_ID,
            "saga-recovery",
            "used by Nexus when recovering sagas",
        );

    /// Internal user used by Nexus when authenticating external requests
    pub static ref USER_EXTERNAL_AUTHN: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            *authn::USER_EXTERNAL_AUTHN_ID,
            "external-authn",
            "used by Nexus when authenticating external requests",
        );
}

#[cfg(test)]
mod test {
    use super::super::assert_valid_uuid;
    use super::USER_DB_INIT;
    use super::USER_EXTERNAL_AUTHN;
    use super::USER_INTERNAL_API;
    use super::USER_INTERNAL_READ;
    use super::USER_SAGA_RECOVERY;
    use super::USER_SERVICE_BALANCER;

    #[test]
    fn test_builtin_user_ids_are_valid() {
        assert_valid_uuid(&USER_SERVICE_BALANCER.id);
        assert_valid_uuid(&USER_DB_INIT.id);
        assert_valid_uuid(&USER_INTERNAL_API.id);
        assert_valid_uuid(&USER_EXTERNAL_AUTHN.id);
        assert_valid_uuid(&USER_INTERNAL_READ.id);
        assert_valid_uuid(&USER_SAGA_RECOVERY.id);
    }
}
