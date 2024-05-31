// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in Silo Users

use super::role_builtin;
use nexus_db_model as model;
use nexus_types::identity::Asset;
use once_cell::sync::Lazy;

/// Test user that's granted all privileges, used for automated testing and
/// local development
// TODO-security Once we have a way to bootstrap the initial Silo with the
// initial privileged user, this user should be created in the test suite,
// not automatically at Nexus startup.  See omicron#2305.
pub static USER_TEST_PRIVILEGED: Lazy<model::SiloUser> = Lazy::new(|| {
    model::SiloUser::new(
        *crate::silo::DEFAULT_SILO_ID,
        // "4007" looks a bit like "root".
        "001de000-05e4-4000-8000-000000004007".parse().unwrap(),
        "privileged".into(),
    )
});

/// Role assignments needed for the privileged user
pub static ROLE_ASSIGNMENTS_PRIVILEGED: Lazy<Vec<model::RoleAssignment>> =
    Lazy::new(|| {
        vec![
            // The "test-privileged" user gets the "admin" role on the sole
            // Fleet as well as the default Silo.
            model::RoleAssignment::new(
                model::IdentityType::SiloUser,
                USER_TEST_PRIVILEGED.id(),
                role_builtin::FLEET_ADMIN.resource_type,
                *crate::FLEET_ID,
                role_builtin::FLEET_ADMIN.role_name,
            ),
            model::RoleAssignment::new(
                model::IdentityType::SiloUser,
                USER_TEST_PRIVILEGED.id(),
                role_builtin::SILO_ADMIN.resource_type,
                *crate::silo::DEFAULT_SILO_ID,
                role_builtin::SILO_ADMIN.role_name,
            ),
        ]
    });

/// Test user that's granted no privileges, used for automated testing
// TODO-security Once we have a way to bootstrap the initial Silo with the
// initial privileged user, this user should be created in the test suite,
// not automatically at Nexus startup.  See omicron#2305.
pub static USER_TEST_UNPRIVILEGED: Lazy<model::SiloUser> = Lazy::new(|| {
    model::SiloUser::new(
        *crate::silo::DEFAULT_SILO_ID,
        // 60001 is the decimal uid for "nobody" on Helios.
        "001de000-05e4-4000-8000-000000060001".parse().unwrap(),
        "unprivileged".into(),
    )
});

#[cfg(test)]
mod test {
    use super::super::assert_valid_uuid;
    use super::USER_TEST_PRIVILEGED;
    use super::USER_TEST_UNPRIVILEGED;
    use nexus_types::identity::Asset;

    #[test]
    fn test_silo_user_ids_are_valid() {
        assert_valid_uuid(&USER_TEST_PRIVILEGED.id());
        assert_valid_uuid(&USER_TEST_UNPRIVILEGED.id());
    }
}
