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
    pub static ref BUILTIN_ROLE_ASSIGNMENTS: Vec<RoleAssignmentBuiltin> =
        vec![
            // The "test-privileged" user gets the "admin" role on the sole
            // Fleet.  This will grant them all permissions on all resources.
            RoleAssignmentBuiltin::new(
                user_builtin::USER_TEST_PRIVILEGED.id,
                role_builtin::FLEET_ADMIN.resource_type,
                *FLEET_ID,
                role_builtin::FLEET_ADMIN.role_name,
            ),

            // The same user also needs "fleet authenticator" to be able to
            // create test sessions.
            RoleAssignmentBuiltin::new(
                user_builtin::USER_TEST_PRIVILEGED.id,
                role_builtin::FLEET_AUTHENTICATOR.resource_type,
                *FLEET_ID,
                role_builtin::FLEET_AUTHENTICATOR.role_name,
            ),

            // The "internal-api" user gets the "admin" role on the sole Fleet.
            // This will grant them (nearly) all permissions on all resources.
            // TODO-security We should scope this down (or, really, figure out a
            // better internal authn/authz story).
            RoleAssignmentBuiltin::new(
                user_builtin::USER_INTERNAL_API.id,
                role_builtin::FLEET_ADMIN.resource_type,
                *FLEET_ID,
                role_builtin::FLEET_ADMIN.role_name,
            ),

            // The "internal-read" user gets the "viewer" role on the sole Fleet.
            // This will grant them the ability to read various control plane
            // data (like the list of sleds), which is in turn used to talk to
            // sleds or allocate resources.
            RoleAssignmentBuiltin::new(
                user_builtin::USER_INTERNAL_READ.id,
                role_builtin::FLEET_VIEWER.resource_type,
                *FLEET_ID,
                role_builtin::FLEET_VIEWER.role_name,
            ),

            // The "internal-authenticator" user gets the "authentiator" role on
            // the sole fleet.  This grants them the ability to create sessions.
            RoleAssignmentBuiltin::new(
                user_builtin::USER_EXTERNAL_AUTHN.id,
                role_builtin::FLEET_AUTHENTICATOR.resource_type,
                *FLEET_ID,
                role_builtin::FLEET_AUTHENTICATOR.role_name,
            ),
        ];
}
