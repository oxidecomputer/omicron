// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in Silo Users

use super::role_builtin;
use crate::authn;
use crate::db;
use crate::db::identity::Asset;
use lazy_static::lazy_static;

lazy_static! {
    /// Test user that's granted all privileges, used for automated testing and
    /// local development
    // TODO-security Once we have a way to bootstrap the initial Silo with the
    // initial privileged user, this user should be created in the test suite,
    // not automatically at Nexus startup.
    pub static ref USER_TEST_PRIVILEGED: db::model::SiloUser =
        db::model::SiloUser::new(
            *authn::SILO_ID,
            *authn::USER_TEST_PRIVILEGED_ID,
            "privileged".into(),
        );

    /// Role assignments needed for the privileged user
    pub static ref ROLE_ASSIGNMENTS_PRIVILEGED:
        Vec<db::model::RoleAssignment> = vec![
            // The "test-privileged" user gets the "admin" role on the sole
            // Fleet.  This will grant them all permissions on all resources.
            db::model::RoleAssignment::new(
                db::model::IdentityType::SiloUser,
                USER_TEST_PRIVILEGED.id(),
                role_builtin::FLEET_ADMIN.resource_type,
                *db::fixed_data::FLEET_ID,
                role_builtin::FLEET_ADMIN.role_name,
            ),
        ];

    /// Test user that's granted no privileges, used for automated testing
    // TODO-security Once we have a way to bootstrap the initial Silo with the
    // initial privileged user, this user should be created in the test suite,
    // not automatically at Nexus startup.
    pub static ref USER_TEST_UNPRIVILEGED: db::model::SiloUser =
        db::model::SiloUser::new(
            *authn::SILO_ID,
            *authn::USER_TEST_UNPRIVILEGED_ID,
            "unprivileged".into(),
        );
}

#[cfg(test)]
mod test {
    use super::super::assert_valid_uuid;
    use super::USER_TEST_PRIVILEGED;
    use super::USER_TEST_UNPRIVILEGED;
    use crate::db::identity::Asset;

    #[test]
    fn test_silo_user_ids_are_valid() {
        assert_valid_uuid(&USER_TEST_PRIVILEGED.id());
        assert_valid_uuid(&USER_TEST_UNPRIVILEGED.id());
    }
}
