// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for policy types.

use crate::latest::policy::{IdentityType, RoleAssignment};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloGroupUuid;
use omicron_uuid_kinds::SiloUserUuid;

impl<AllowedRoles> RoleAssignment<AllowedRoles> {
    pub fn for_silo_user(
        silo_user_id: SiloUserUuid,
        role_name: AllowedRoles,
    ) -> Self {
        Self {
            identity_type: IdentityType::SiloUser,
            identity_id: silo_user_id.into_untyped_uuid(),
            role_name,
        }
    }

    pub fn for_silo_group(
        silo_group_id: SiloGroupUuid,
        role_name: AllowedRoles,
    ) -> Self {
        Self {
            identity_type: IdentityType::SiloGroup,
            identity_id: silo_group_id.into_untyped_uuid(),
            role_name,
        }
    }
}

impl<AllowedRoles> RoleAssignment<AllowedRoles> {
    pub fn for_service_account(
        service_account_id: uuid::Uuid,
        role_name: AllowedRoles,
    ) -> Self {
        Self {
            identity_type: IdentityType::ServiceAccount,
            identity_id: service_account_id,
            role_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::latest::policy::{MAX_ROLE_ASSIGNMENTS_PER_RESOURCE, Policy};
    use serde::Deserialize;

    #[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
    #[serde(rename_all = "kebab-case")]
    pub enum DummyRoles {
        Bogus,
    }

    #[test]
    fn test_policy_parsing() {
        // Success case (edge case: max number of role assignments)
        let role_assignment = serde_json::json!({
            "identity_type": "silo_user",
            "identity_id": "75ec4a39-67cf-4549-9e74-44b92947c37c",
            "role_name": "bogus"
        });
        const MAX: usize = MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;
        let okay_input =
            serde_json::Value::Array(vec![role_assignment.clone(); MAX]);
        let policy: Policy<DummyRoles> =
            serde_json::from_value(serde_json::json!({
                "role_assignments": okay_input
            }))
            .expect("unexpectedly failed with okay input");
        assert_eq!(policy.role_assignments[0].role_name, DummyRoles::Bogus);

        // Failure case: too many role assignments
        let bad_input =
            serde_json::Value::Array(vec![role_assignment; MAX + 1]);
        let error =
            serde_json::from_value::<Policy<DummyRoles>>(serde_json::json!({
                "role_assignments": bad_input
            }))
            .expect_err("unexpectedly succeeded with too many items");
        assert_eq!(
            error.to_string(),
            "invalid length 65, expected a list of at most 64 role assignments"
        );
    }
}
