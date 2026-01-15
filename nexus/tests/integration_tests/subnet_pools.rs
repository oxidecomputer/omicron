// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for Subnet Pools API stubs
//!
//! These tests verify that the stub endpoints return appropriate
//! "not implemented" errors. Once the full implementation is complete,
//! these tests should be replaced with proper CRUD tests.
//!
//! TODO(#9453): Replace stub tests with full implementation tests.

use http::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views::IpVersion;
use omicron_common::api::external::IdentityMetadataCreateParams;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const SUBNET_POOLS_URL: &str = "/v1/system/subnet-pools";

// Note: These tests verify that stub endpoints return 500 Internal Server Error.
// The detailed "endpoint is not implemented" message is intentionally not exposed
// to clients for security reasons (internal messages are logged server-side only).

#[nexus_test]
async fn test_subnet_pool_list_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::GET,
        SUBNET_POOLS_URL,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_subnet_pool_create_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let create_params = params::SubnetPoolCreate {
        identity: IdentityMetadataCreateParams {
            name: "test-pool".parse().unwrap(),
            description: String::from("A test subnet pool"),
        },
        ip_version: IpVersion::V4,
    };

    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::POST,
        SUBNET_POOLS_URL,
        &create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_subnet_pool_view_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let url = format!("{}/test-pool", SUBNET_POOLS_URL);

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::GET,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_subnet_pool_delete_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let url = format!("{}/test-pool", SUBNET_POOLS_URL);

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::DELETE,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_subnet_pool_subnet_list_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let url = format!("{}/test-pool/subnets", SUBNET_POOLS_URL);

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::GET,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_subnet_pool_silo_list_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let url = format!("{}/test-pool/silos", SUBNET_POOLS_URL);

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::GET,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_subnet_pool_utilization_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let url = format!("{}/test-pool/utilization", SUBNET_POOLS_URL);

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::GET,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}
