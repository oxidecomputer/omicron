// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in users

use crate::db::fixed_data::silo_builtin::SILO_ID;
use lazy_static::lazy_static;
use omicron_common::api;
use uuid::Uuid;

pub struct UserBuiltinConfig {
    pub id: Uuid,
    pub silo_id: Uuid,
    pub name: api::external::Name,
    pub description: &'static str,
}

impl UserBuiltinConfig {
    fn new_static(
        id: &str,
        silo_id: &str,
        name: &str,
        description: &'static str,
    ) -> UserBuiltinConfig {
        UserBuiltinConfig {
            id: id.parse().expect("invalid uuid for builtin user id"),
            silo_id: silo_id
                .parse()
                .expect("invalid uuid for builtin user silo id"),
            name: name.parse().expect("invalid name for builtin user name"),
            description,
        }
    }
}

lazy_static! {
    /// Internal user used for seeding initial database data
    // NOTE: This uuid and name are duplicated in dbinit.sql.
    pub static ref USER_DB_INIT: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            // "0001" is the first possible user that wouldn't be confused with
            // 0, or root.
            "001de000-05e4-4000-8000-000000000001",
            &SILO_ID.to_string().as_str(),
            "db-init",
            "used for seeding initial database data",
        );

    /// Internal user used by Nexus when handling internal API requests
    pub static ref USER_INTERNAL_API: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            "001de000-05e4-4000-8000-000000000002",
            &SILO_ID.to_string().as_str(),
            "internal-api",
            "used by Nexus when handling internal API requests",
        );

    /// Internal user used by Nexus to read privileged control plane data
    pub static ref USER_INTERNAL_READ: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            // "4ead" looks like "read"
            "001de000-05e4-4000-8000-000000004ead",
            &SILO_ID.to_string().as_str(),
            "internal-read",
            "used by Nexus to read privileged control plane data",
        );

    /// Internal user used by Nexus when recovering sagas
    pub static ref USER_SAGA_RECOVERY: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            // "3a8a" looks a bit like "saga".
            "001de000-05e4-4000-8000-000000003a8a",
            &SILO_ID.to_string().as_str(),
            "saga-recovery",
            "used by Nexus when recovering sagas",
        );

    /// Test user that's granted all privileges, used for automated testing and
    /// local development
    // TODO-security This eventually needs to go, maybe replaced with some kind
    // of deployment-specific customization.
    pub static ref USER_TEST_PRIVILEGED: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            // "4007" looks a bit like "root".
            "001de000-05e4-4000-8000-000000004007",
            &SILO_ID.to_string().as_str(),
            "test-privileged",
            "used for testing with all privileges",
        );

    /// Test user that's granted no privileges, used for automated testing
    pub static ref USER_TEST_UNPRIVILEGED: UserBuiltinConfig =
        UserBuiltinConfig::new_static(
            // 60001 is the decimal uid for "nobody" on Helios.
            "001de000-05e4-4000-8000-000000060001",
            &SILO_ID.to_string().as_str(),
            "test-unprivileged",
            "used for testing with no privileges",
        );
}

#[cfg(test)]
mod test {
    use super::super::assert_valid_uuid;
    use super::USER_DB_INIT;
    use super::USER_INTERNAL_API;
    use super::USER_INTERNAL_READ;
    use super::USER_SAGA_RECOVERY;
    use super::USER_TEST_PRIVILEGED;
    use super::USER_TEST_UNPRIVILEGED;

    #[test]
    fn test_builtin_user_ids_are_valid() {
        assert_valid_uuid(&USER_DB_INIT.id);
        assert_valid_uuid(&USER_INTERNAL_API.id);
        assert_valid_uuid(&USER_INTERNAL_READ.id);
        assert_valid_uuid(&USER_SAGA_RECOVERY.id);
        assert_valid_uuid(&USER_TEST_PRIVILEGED.id);
        assert_valid_uuid(&USER_TEST_UNPRIVILEGED.id);
    }
}
