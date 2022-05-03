// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Basic test for role assignments

use dropshot::test_util::ClientTestContext;
use http::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::resource_helpers::create_organization;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::ObjectIdentity;
use omicron_nexus::authn::USER_TEST_UNPRIVILEGED;
use omicron_nexus::db::fixed_data::role_builtin::ORGANIZATION_ADMINISTRATOR;
use omicron_nexus::external_api::shared;
use omicron_nexus::external_api::views;

// TODO-coverage A more comprehensive test would be useful when we have proper
// Silo users
#[nexus_test]
async fn test_role_assignments_basic(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let org_name = "test-org";
    create_organization(client, org_name).await;
    let org_url = format!("/organizations/{}", org_name);

    // Verify the initial policy.
    let initial_policy = policy_fetch(client, &org_url).await;
    assert!(initial_policy.role_assignments.is_empty());

    // Verify that the unprivileged user cannot access this Organization.  This
    // is primarily tested in the separate "unauthorized" test, but we do it
    // here as a control to make sure that the checks below pass for the right
    // reasons.
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &org_url,
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .unwrap();

    // Make a new policy granting the unprivileged user access to this
    // Organization.  This is a little ugly, but we don't have a way of creating
    // silo users yet and it's worth testing this.
    let mut new_policy = initial_policy.clone();
    let role_assignment = shared::RoleAssignment {
        identity_type: shared::IdentityType::UserBuiltin,
        identity_id: USER_TEST_UNPRIVILEGED.id,
        role_name: ORGANIZATION_ADMINISTRATOR.role_name.to_string(),
    };
    new_policy.role_assignments.push(role_assignment.clone());

    // First, make sure the unprivileged user can't grant themselves access!
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &org_url,
        &new_policy,
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .unwrap();

    // And it really didn't work.
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &org_url,
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .unwrap();

    // Okay, really grant them access.
    let updated_policy: shared::Policy = NexusRequest::object_put(
        client,
        &format!("{}/policy", org_url),
        Some(&new_policy),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(updated_policy, new_policy);

    // Double-check that the policy reflects that.
    let latest_policy = policy_fetch(client, &org_url).await;
    assert_eq!(latest_policy.role_assignments.len(), 1);
    assert_eq!(latest_policy.role_assignments[0], role_assignment);

    // Now that user ought to be able to fetch the Organization.  (This is not
    // really a policy test so we're not going to check all possible actions.)
    let org: views::Organization = NexusRequest::object_get(client, &org_url)
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();
    assert_eq!(org.identity().name, org_name);

    // The way we've defined things, the so-called unprivileged user ought to be
    // able to revoke their own access.
    let updated_policy: shared::Policy = NexusRequest::object_put(
        client,
        &format!("{}/policy", org_url),
        Some(&initial_policy),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(updated_policy, initial_policy);

    // Double-check that the policy reflects that.
    let initial_policy = policy_fetch(client, &org_url).await;
    assert!(initial_policy.role_assignments.is_empty());

    // Now check that we enforce the change!
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &org_url,
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn policy_fetch(
    client: &ClientTestContext,
    resource_url: &str,
) -> shared::Policy {
    let policy_url = format!("{}/policy", resource_url);
    NexusRequest::object_get(client, &policy_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}
