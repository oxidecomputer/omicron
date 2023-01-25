// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::ResultsPage;
use http::{method::Method, StatusCode};
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::SemverVersion;
use omicron_nexus::external_api::views;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// This file could be combined with ./updates.rs, but there's a lot going on in
// there that has nothing to do with testing the API endpoints. We could come up
// with more descriptive names.

#[nexus_test]
async fn test_system_version(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let version =
        NexusRequest::object_get(&client, &"/v1/system/update/version")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::SystemVersion>()
            .await;

    assert_eq!(
        version,
        views::SystemVersion {
            version_range: views::VersionRange {
                low: SemverVersion::new(0, 0, 1),
                high: SemverVersion::new(0, 0, 2),
            },
            status: views::UpdateStatus::Steady,
        }
    );
}

// TODO: Figure out how to create system updates, update components, and
// updateable components for these tests in light of the fact that there are no
// create endpoints for those resources. We can call the Nexus functions
// directly here, but that requires nexus::app::update to be public so we can
// import CreateSystemUpdate from it. A test-only helper function that lives in
// a different file might let us avoid over-exporting.

#[nexus_test]
async fn test_list_updates(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let updates =
        NexusRequest::object_get(&client, &"/v1/system/update/updates")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<ResultsPage<views::SystemUpdate>>()
            .await;

    assert_eq!(updates.items.len(), 0);
    assert_eq!(updates.next_page, None);
}

#[nexus_test]
async fn test_list_components(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let component_updates =
        NexusRequest::object_get(&client, &"/v1/system/update/components")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<ResultsPage<views::ComponentUpdate>>()
            .await;

    assert_eq!(component_updates.items.len(), 0);
    assert_eq!(component_updates.next_page, None);
}

#[nexus_test]
async fn test_get_update(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        "/v1/system/update/updates/1.0.0",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_list_update_components(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        "/v1/system/update/updates/1.0.0/components",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

#[nexus_test]
async fn test_list_deployments(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let component_updates =
        NexusRequest::object_get(&client, &"/v1/system/update/deployments")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<ResultsPage<views::UpdateDeployment>>()
            .await;

    assert_eq!(component_updates.items.len(), 0);
    assert_eq!(component_updates.next_page, None);
}
