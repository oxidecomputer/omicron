// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in assignments for built-in users and built-in roles

use super::role_builtin;
use super::user_builtin;
use super::FLEET_ID;
use crate::db::model::RoleAssignmentBuiltin;
use lazy_static::lazy_static;

lazy_static! {
    // The "test-privileged" user gets the "admin" role on the sole Fleet.
    // This will grant them all permissions on all resources.
    pub static ref BUILTIN_ROLE_ASSIGNMENTS: Vec<RoleAssignmentBuiltin> =
        vec![RoleAssignmentBuiltin::new(
            user_builtin::USER_TEST_PRIVILEGED.id,
            role_builtin::FLEET_ADMIN.resource_type,
            *FLEET_ID,
            role_builtin::FLEET_ADMIN.role_name,
        )];
}
