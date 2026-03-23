// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in Silo Users

use nexus_db_model as model;
use nexus_types::identity::Asset;
use nexus_types::silo::DEFAULT_SILO_ID;
use omicron_common::api::external::ResourceType;
use std::sync::LazyLock;

/// Test user that's granted all privileges, used for automated testing and
/// local development
// TODO-security Once we have a way to bootstrap the initial Silo with the
// initial privileged user, this user should be created in the test suite,
// not automatically at Nexus startup.  See omicron#2305.
pub static USER_TEST_PRIVILEGED: LazyLock<model::SiloUser> =
    LazyLock::new(|| {
        model::SiloUser::new_api_only(
            DEFAULT_SILO_ID,
            // "4007" looks a bit like "root".
            "001de000-05e4-4000-8000-000000004007".parse().unwrap(),
            "privileged".into(),
        )
    });

/// Role assignments needed for the privileged user
pub static ROLE_ASSIGNMENTS_PRIVILEGED: LazyLock<Vec<model::RoleAssignment>> =
    LazyLock::new(|| {
        vec![
            // The "test-privileged" user gets the "admin" role on the sole
            // Fleet as well as the default Silo.
            model::RoleAssignment::new_for_silo_user(
                USER_TEST_PRIVILEGED.id(),
                ResourceType::Fleet,
                *crate::FLEET_ID,
                "admin",
            ),
            model::RoleAssignment::new_for_silo_user(
                USER_TEST_PRIVILEGED.id(),
                ResourceType::Silo,
                DEFAULT_SILO_ID,
                "admin",
            ),
        ]
    });

/// Test user that's granted no privileges, used for automated testing
// TODO-security Once we have a way to bootstrap the initial Silo with the
// initial privileged user, this user should be created in the test suite,
// not automatically at Nexus startup.  See omicron#2305.
pub static USER_TEST_UNPRIVILEGED: LazyLock<model::SiloUser> =
    LazyLock::new(|| {
        model::SiloUser::new_api_only(
            DEFAULT_SILO_ID,
            // 60001 is the decimal uid for "nobody" on Helios.
            "001de000-05e4-4000-8000-000000060001".parse().unwrap(),
            "unprivileged".into(),
        )
    });

#[cfg(test)]
mod test {
    use super::super::assert_valid_typed_uuid;
    use super::USER_TEST_PRIVILEGED;
    use super::USER_TEST_UNPRIVILEGED;
    use nexus_types::identity::Asset;

    #[test]
    fn test_silo_user_ids_are_valid() {
        assert_valid_typed_uuid(&USER_TEST_PRIVILEGED.id());
        assert_valid_typed_uuid(&USER_TEST_UNPRIVILEGED.id());
    }
}
