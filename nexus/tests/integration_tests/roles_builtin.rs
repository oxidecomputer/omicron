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
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_nexus::external_api::views::Role;

#[nexus_test]
async fn test_roles_builtin(cptestctx: &ControlPlaneTestContext) {
    let testctx = &cptestctx.external_client;

    RequestBuilder::new(testctx, Method::GET, "/roles")
        .expect_status(Some(StatusCode::UNAUTHORIZED))
        .execute()
        .await
        .unwrap();

    NexusRequest::new(
        RequestBuilder::new(testctx, Method::GET, "/roles")
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .unwrap();

    let roles = NexusRequest::object_get(&testctx, "/roles")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body::<ResultsPage<Role>>()
        .unwrap()
        .items;

    let role_essentials = roles
        .iter()
        .map(|r| (r.name.as_str(), r.description.as_str()))
        .collect::<Vec<_>>();

    assert_eq!(
        role_essentials,
        vec![
            ("fleet.admin", "Fleet Administrator"),
            ("fleet.collaborator", "Fleet Collaborator"),
            ("organization.admin", "Organization Administrator"),
            ("organization.collaborator", "Organization Collaborator"),
            ("project.admin", "Project Administrator"),
            ("project.collaborator", "Project Collaborator"),
            ("project.viewer", "Project Viewer"),
        ]
    );

    // This endpoint uses a custom pagination scheme that is easy to get wrong.
    // Let's test that all markers do work.
    // XXX need fix this
    // let roles_paginated: Vec<Role> =
    //     dropshot::test_util::iter_collection(&testctx, "/roles", "", 1).await.0;
    // assert_eq!(roles, roles_paginated);
}
