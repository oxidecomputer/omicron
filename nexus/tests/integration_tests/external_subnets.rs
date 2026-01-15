// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for External Subnets API stubs
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
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use omicron_common::api::external::IdentityMetadataCreateParams;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const PROJECT_NAME: &str = "test-project";

// Note: These tests verify that stub endpoints return 500 Internal Server Error.
// The detailed "endpoint is not implemented" message is intentionally not exposed
// to clients for security reasons (internal messages are logged server-side only).

fn external_subnets_url(project: &str) -> String {
    format!("/v1/external-subnets?project={}", project)
}

fn external_subnet_url(name: &str, project: &str) -> String {
    format!("/v1/external-subnets/{}?project={}", name, project)
}

fn external_subnet_attach_url(name: &str, project: &str) -> String {
    format!("/v1/external-subnets/{}/attach?project={}", name, project)
}

fn external_subnet_detach_url(name: &str, project: &str) -> String {
    format!("/v1/external-subnets/{}/detach?project={}", name, project)
}

#[nexus_test]
async fn test_external_subnet_list_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project first (external subnets are project-scoped)
    let _ = create_project(client, PROJECT_NAME).await;

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::GET,
        &external_subnets_url(PROJECT_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_external_subnet_create_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project first
    let _ = create_project(client, PROJECT_NAME).await;

    let create_params = params::ExternalSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: "test-subnet".parse().unwrap(),
            description: String::from("A test external subnet"),
        },
        pool: None,
        subnet: None,
        prefix_len: Some(24),
    };

    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::POST,
        &external_subnets_url(PROJECT_NAME),
        &create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_external_subnet_view_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project first
    let _ = create_project(client, PROJECT_NAME).await;

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::GET,
        &external_subnet_url("test-subnet", PROJECT_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_external_subnet_update_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project first
    let _ = create_project(client, PROJECT_NAME).await;

    let update_params = params::ExternalSubnetUpdate {
        identity: omicron_common::api::external::IdentityMetadataUpdateParams {
            name: None,
            description: Some(String::from("Updated description")),
        },
    };

    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::PUT,
        &external_subnet_url("test-subnet", PROJECT_NAME),
        &update_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_external_subnet_delete_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project first
    let _ = create_project(client, PROJECT_NAME).await;

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::DELETE,
        &external_subnet_url("test-subnet", PROJECT_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_external_subnet_attach_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project first
    let _ = create_project(client, PROJECT_NAME).await;

    let attach_params = params::ExternalSubnetAttach {
        kind: params::ExternalSubnetParentKind::Instance,
        parent: "test-instance".parse().unwrap(),
    };

    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::POST,
        &external_subnet_attach_url("test-subnet", PROJECT_NAME),
        &attach_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_external_subnet_detach_unimplemented(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project first
    let _ = create_project(client, PROJECT_NAME).await;

    NexusRequest::expect_failure(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::POST,
        &external_subnet_detach_url("test-subnet", PROJECT_NAME),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}
