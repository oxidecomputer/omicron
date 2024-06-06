// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in assignments for built-in users and built-in roles

use super::role_builtin;
use super::user_builtin;
use super::FLEET_ID;
use nexus_db_model::IdentityType;
use nexus_db_model::RoleAssignment;
use once_cell::sync::Lazy;

pub static BUILTIN_ROLE_ASSIGNMENTS: Lazy<Vec<RoleAssignment>> =
    Lazy::new(|| {
        vec![
            // The "internal-api" user gets the "admin" role on the sole Fleet.
            // This is a pretty elevated privilege.
            // TODO-security We should scope this down (or, really, figure out a
            // better internal authn/authz story).
            RoleAssignment::new(
                IdentityType::UserBuiltin,
                user_builtin::USER_INTERNAL_API.id,
                role_builtin::FLEET_ADMIN.resource_type,
                *FLEET_ID,
                role_builtin::FLEET_ADMIN.role_name,
            ),
            // The "USER_SERVICE_BALANCER" user gets the "admin" role on the
            // Fleet.
            //
            // This is necessary as services exist as resources implied by
            // "FLEET" - if they ever become more fine-grained, this scope
            // could also become smaller.
            RoleAssignment::new(
                IdentityType::UserBuiltin,
                user_builtin::USER_SERVICE_BALANCER.id,
                role_builtin::FLEET_ADMIN.resource_type,
                *FLEET_ID,
                role_builtin::FLEET_ADMIN.role_name,
            ),
            // The "internal-read" user gets the "viewer" role on the sole
            // Fleet.  This will grant them the ability to read various control
            // plane data (like the list of sleds), which is in turn used to
            // talk to sleds or allocate resources.
            RoleAssignment::new(
                IdentityType::UserBuiltin,
                user_builtin::USER_INTERNAL_READ.id,
                role_builtin::FLEET_VIEWER.resource_type,
                *FLEET_ID,
                role_builtin::FLEET_VIEWER.role_name,
            ),
            // The "external-authenticator" user gets the "authenticator" role
            // on the sole fleet.  This grants them the ability to create
            // sessions.
            RoleAssignment::new(
                IdentityType::UserBuiltin,
                user_builtin::USER_EXTERNAL_AUTHN.id,
                role_builtin::FLEET_AUTHENTICATOR.resource_type,
                *FLEET_ID,
                role_builtin::FLEET_AUTHENTICATOR.role_name,
            ),
        ]
    });
