// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Basic test for built-in roles

use dropshot::ResultsPage;
use http::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views::Role;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_roles_builtin(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    // Success cases
    let roles = NexusRequest::object_get(&testctx, "/v1/system/roles")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body::<ResultsPage<Role>>()
        .unwrap()
        .items;

    let expected = [
        ("fleet.admin", "Fleet Administrator"),
        ("fleet.collaborator", "Fleet Collaborator"),
        ("fleet.external-authenticator", "Fleet External Authenticator"),
        ("fleet.viewer", "Fleet Viewer"),
        ("project.admin", "Project Administrator"),
        ("project.collaborator", "Project Collaborator"),
        ("project.viewer", "Project Viewer"),
        ("silo.admin", "Silo Administrator"),
        ("silo.collaborator", "Silo Collaborator"),
        ("silo.viewer", "Silo Viewer"),
    ];
    for (actual, expected) in roles.iter().zip(expected.iter()) {
        let (expected_name, expected_description) = expected;
        assert_eq!(*expected_name, actual.name.to_string());
        assert_eq!(*expected_description, actual.description.to_string());
    }
    assert_eq!(roles.len(), expected.len());

    // This endpoint uses a custom pagination scheme that is easy to get wrong.
    // Let's test that all markers do work.
    let roles_paginated = NexusRequest::iter_collection_authn(
        &testctx,
        "/v1/system/roles",
        "",
        Some(1),
    )
    .await
    .expect("failed to iterate all roles");
    assert_eq!(roles, roles_paginated.all_items);
    // There's an empty page at the end of each dropshot scan.
    assert_eq!(roles.len() + 1, roles_paginated.npages);

    // Test GET /v1/system/roles/$role_name

    // Success cases
    for r in &roles {
        let one_role = NexusRequest::object_get(
            &testctx,
            &format!("/v1/system/roles/{}", r.name),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body::<Role>()
        .unwrap();
        assert_eq!(one_role, *r);
    }

    // Invalid name: missing "."
    NexusRequest::new(
        RequestBuilder::new(
            testctx,
            Method::GET,
            "/v1/system/roles/fleet_admin",
        )
        .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Invalid name: not found
    NexusRequest::new(
        RequestBuilder::new(
            testctx,
            Method::GET,
            "/v1/system/roles/fleet.admiral",
        )
        .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}
