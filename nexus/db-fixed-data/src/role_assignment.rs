// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Built-in assignments for built-in users and built-in roles

use super::FLEET_ID;
use super::user_builtin;
use nexus_db_model::IdentityType;
use nexus_db_model::RoleAssignment;
use omicron_common::api::external::ResourceType;
use std::sync::LazyLock;

pub static BUILTIN_ROLE_ASSIGNMENTS: LazyLock<Vec<RoleAssignment>> =
    LazyLock::new(|| {
        vec![
            // The "internal-api" user gets the "admin" role on the sole Fleet.
            // This is a pretty elevated privilege.
            // TODO-security We should scope this down (or, really, figure out a
            // better internal authn/authz story).
            RoleAssignment::new(
                IdentityType::UserBuiltin,
                user_builtin::USER_INTERNAL_API.id,
                ResourceType::Fleet,
                *FLEET_ID,
                "admin",
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
                ResourceType::Fleet,
                *FLEET_ID,
                "admin",
            ),
            // The "internal-read" user gets the "viewer" role on the sole
            // Fleet.  This will grant them the ability to read various control
            // plane data (like the list of sleds), which is in turn used to
            // talk to sleds or allocate resources.
            RoleAssignment::new(
                IdentityType::UserBuiltin,
                user_builtin::USER_INTERNAL_READ.id,
                ResourceType::Fleet,
                *FLEET_ID,
                "viewer",
            ),
            // The "external-authenticator" user gets the
            // "external-authenticator" role on the sole fleet.  This grants
            // them the ability to create sessions.
            RoleAssignment::new(
                IdentityType::UserBuiltin,
                user_builtin::USER_EXTERNAL_AUTHN.id,
                ResourceType::Fleet,
                *FLEET_ID,
                "external-authenticator",
            ),
        ]
    });
