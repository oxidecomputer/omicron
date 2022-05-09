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
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::ObjectIdentity;
use omicron_nexus::authn::USER_TEST_UNPRIVILEGED;
use omicron_nexus::authz;
use omicron_nexus::db::fixed_data;
use omicron_nexus::db::identity::Resource;
use omicron_nexus::external_api::shared;
use omicron_nexus::external_api::views;

#[nexus_test]
async fn test_role_assignments_silo(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let silo_name = fixed_data::silo::DEFAULT_SILO.identity().name.to_string();
    let silo_url = format!("/silos/{}", silo_name);
    do_test::<authz::SiloRoles, views::Silo>(
        client,
        &silo_url,
        &silo_name,
        authz::SiloRoles::Admin,
        true,
    )
    .await;
}

#[nexus_test]
async fn test_role_assignments_organization(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let org_name = "test-org";
    create_organization(client, org_name).await;
    let org_url = format!("/organizations/{}", org_name);
    do_test::<authz::OrganizationRoles, views::Organization>(
        client,
        &org_url,
        org_name,
        authz::OrganizationRoles::Admin,
        false,
    )
    .await;
}

#[nexus_test]
async fn test_role_assignments_project(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let org_name = "test-org";
    let project_name = "test-project";
    create_organization(client, org_name).await;
    create_project(client, org_name, project_name).await;
    let project_url =
        format!("/organizations/{}/projects/{}", org_name, project_name);
    do_test::<authz::ProjectRoles, views::Project>(
        client,
        &project_url,
        project_name,
        authz::ProjectRoles::Admin,
        false,
    )
    .await;
}

/// Helper function for running a role assignment test on the given resource
// TODO-coverage A more comprehensive test would be useful when we have proper
// Silo users
// TODO-coverage TODO-security The "always_visible" flag is needed only for
// Silos.  When "always_visible" is false, this function verifies that an
// unprivileged user cannot see the given resource at the start, then creates a
// policy that grants that user access, then checks that they _can_ see it.
// Then it revokes access and checks again that they can't.  For Silos, an
// unprivileged user can always see their own Silo today so this doesn't work as
// expected.  Once we flesh out Silos with more operations (e.g., a "modify"
// operation), we could check enforcement of that instead.
async fn do_test<T, V>(
    client: &dropshot::test_util::ClientTestContext,
    resource_url: &str,
    resource_name: &str,
    admin_role: T,
    always_visible: bool,
) where
    T: Clone
        + std::fmt::Debug
        + PartialEq
        + serde::Serialize
        + serde::de::DeserializeOwned,
    V: serde::de::DeserializeOwned + ObjectIdentity,
{
    // Verify the initial policy.
    let initial_policy = policy_fetch::<T>(client, resource_url).await;
    assert!(initial_policy.role_assignments.is_empty());

    // Verify that the unprivileged user cannot access this resource.  This is
    // primarily tested in the separate "unauthorized" test, but we do it here
    // as a control to make sure that the checks below pass for the right
    // reasons.
    if !always_visible {
        NexusRequest::expect_failure(
            client,
            StatusCode::NOT_FOUND,
            Method::GET,
            resource_url,
        )
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .unwrap();
    }

    // Make a new policy granting the unprivileged user access to this resource.
    // This is a little ugly, but we don't have a way of creating silo users yet
    // and it's worth testing this.
    let mut new_policy = initial_policy.clone();
    let role_assignment = shared::RoleAssignment {
        identity_type: shared::IdentityType::UserBuiltin,
        identity_id: USER_TEST_UNPRIVILEGED.id,
        role_name: admin_role,
    };
    new_policy.role_assignments.push(role_assignment.clone());

    // First, make sure the unprivileged user can't grant themselves access!
    let policy_url = format!("{}/policy", resource_url);
    NexusRequest::expect_failure_with_body(
        client,
        if always_visible {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::NOT_FOUND
        },
        Method::PUT,
        &policy_url,
        &new_policy,
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .unwrap();

    // And it really didn't work.
    if !always_visible {
        NexusRequest::expect_failure(
            client,
            StatusCode::NOT_FOUND,
            Method::GET,
            resource_url,
        )
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .unwrap();
    }

    // Okay, really grant them access.
    let updated_policy: shared::Policy<T> =
        NexusRequest::object_put(client, &policy_url, Some(&new_policy))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(updated_policy, new_policy);

    // Double-check that the policy reflects that.
    let latest_policy = policy_fetch::<T>(client, resource_url).await;
    assert_eq!(latest_policy.role_assignments.len(), 1);
    assert_eq!(latest_policy.role_assignments[0], role_assignment);

    // Now that user ought to be able to fetch the resource.  (This is not
    // really a policy test so we're not going to check all possible actions.)
    if !always_visible {
        let resource: V = NexusRequest::object_get(client, resource_url)
            .authn_as(AuthnMode::UnprivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
        assert_eq!(resource.identity().name, resource_name);
    }

    // The way we've defined things, the so-called unprivileged user ought to be
    // able to revoke their own access.
    let updated_policy: shared::Policy<T> = NexusRequest::object_put(
        client,
        &format!("{}/policy", resource_url),
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
    let initial_policy = policy_fetch::<T>(client, resource_url).await;
    assert!(initial_policy.role_assignments.is_empty());

    // Now check that we enforce the change!
    if !always_visible {
        NexusRequest::expect_failure(
            client,
            StatusCode::NOT_FOUND,
            Method::GET,
            resource_url,
        )
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .unwrap();
    }
}

async fn policy_fetch<T: serde::de::DeserializeOwned>(
    client: &ClientTestContext,
    resource_url: &str,
) -> shared::Policy<T> {
    let policy_url = format!("{}/policy", resource_url);
    NexusRequest::object_get(client, &policy_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}
