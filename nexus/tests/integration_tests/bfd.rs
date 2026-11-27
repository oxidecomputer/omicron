// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests BFD support in the API

use super::endpoints::{
    DEMO_BFD_DISABLE, DEMO_BFD_DISABLE_URL, DEMO_BFD_ENABLE,
    DEMO_BFD_ENABLE_URL,
};
use http::{Method, StatusCode};
use nexus_db_queries::authn::USER_TEST_UNPRIVILEGED;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::bfd::BfdStatus;
use nexus_types::external_api::policy::FleetRole;
use nexus_types::identity::Asset;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const STATUS_URL: &str = "/v1/system/networking/bfd-status";

#[nexus_test]
async fn test_empty_bfd_status(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let status = NexusRequest::object_get(client, STATUS_URL)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<Vec<BfdStatus>>()
        .await;

    // `#[nexus_test]` doesn't set up BFD, so we should have no status. But we
    // should still be able to ask for that! (#[nexus_test] also only sets up
    // one fake scrimlet - that used to cause this endpoint to fail.)
    assert_eq!(status, Vec::new());
}

#[nexus_test]
async fn test_bfd_enable_disable_requires_fleet_admin(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let user = AuthnMode::UnprivilegedUser;

    // Fleet collaborator is the highest role that can't enable or disable BFD
    // sessions. It can still read their status.
    grant_iam(
        client,
        "/v1/system",
        FleetRole::Collaborator,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;
    NexusRequest::object_get(client, STATUS_URL)
        .authn_as(user.clone())
        .execute_and_parse_unwrap::<Vec<BfdStatus>>()
        .await;
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::FORBIDDEN,
        Method::POST,
        DEMO_BFD_ENABLE_URL,
        &*DEMO_BFD_ENABLE,
    )
    .authn_as(user.clone())
    .execute()
    .await
    .unwrap();
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::FORBIDDEN,
        Method::POST,
        DEMO_BFD_DISABLE_URL,
        &*DEMO_BFD_DISABLE,
    )
    .authn_as(user)
    .execute()
    .await
    .unwrap();
}
