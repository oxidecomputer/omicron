// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests BGP status APIs.

use dropshot::test_util::ClientTestContext;
use http::Method;
use nexus_test_interface::NexusServer;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::networking::{
    BgpMessageHistories, SwitchError, SwitchResult, SwitchResults,
    UnnumberedInterfaces, UnnumberedManagerState,
};
use serde_json::{Value, json};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_bgp_unnumbered_status_by_switch(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let manager = NexusRequest::object_get(
        client,
        "/v1/system/networking/bgp-unnumbered-manager",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SwitchResults<UnnumberedManagerState>>()
    .await;
    match manager.switch0 {
        SwitchResult::Ok { value } => {
            assert!(value.interfaces.is_empty());
        }
        SwitchResult::Err { error } => {
            panic!("switch0 manager-state query failed: {error:?}")
        }
    }
    assert_mgd_unresolved(manager.switch1);

    let interfaces = NexusRequest::object_get(
        client,
        "/v1/system/networking/bgp-unnumbered-interfaces",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SwitchResults<UnnumberedInterfaces>>()
    .await;
    match interfaces.switch0 {
        SwitchResult::Ok { value: UnnumberedInterfaces(interfaces) } => {
            assert!(interfaces.is_empty());
        }
        SwitchResult::Err { error } => {
            panic!("switch0 interface query failed: {error:?}")
        }
    }
    assert_mgd_unresolved(interfaces.switch1);
}

#[nexus_test]
async fn test_bgp_aggregate_status_api_versions(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let available_empty = json!({
        "switch0": { "status": "ok", "value": [] },
        "switch1": {
            "status": "err",
            "error": {
                "type": "mgd_unresolved",
            },
        },
    });
    for url in [
        "/v1/system/networking/bgp-status",
        "/v1/system/networking/bgp-exported",
        "/v1/system/networking/bgp-imported?asn=64512",
    ] {
        let response: Value = NexusRequest::object_get(client, url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap()
            .await;
        assert_eq!(response, available_empty, "unexpected response from {url}");
    }
    let response = NexusRequest::object_get(
        client,
        "/v1/system/networking/bgp-message-history?asn=64512",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SwitchResults<BgpMessageHistories>>()
    .await;
    match response.switch0 {
        SwitchResult::Err {
            error:
                SwitchError::RequestRejected {
                    error_code,
                    message,
                    upstream_request_id,
                },
        } => {
            assert_eq!(error_code, None);
            assert_eq!(message, "Not Found");
            assert!(!upstream_request_id.is_empty());
        }
        SwitchResult::Ok { .. } => panic!("unknown ASN unexpectedly succeeded"),
        SwitchResult::Err { error } => {
            panic!("unknown ASN returned an unexpected error: {error:?}")
        }
    }
    assert_mgd_unresolved(response.switch1);

    let server_addr = cptestctx.server.get_http_server_external_address();
    let versioned_client =
        ClientTestContext::new(server_addr, cptestctx.logctx.log.clone());
    let old_version =
        nexus_external_api::VERSION_BGP_UNNUMBERED_PEERS.to_string();

    for url in [
        "/v1/system/networking/bgp-status",
        "/v1/system/networking/bgp-exported",
        "/v1/system/networking/bgp-imported?asn=64512",
    ] {
        let response =
            versioned_get(&versioned_client, url, &old_version).await;
        assert_eq!(response, json!([]), "unexpected response from {url}");
    }
    let response = versioned_get(
        &versioned_client,
        "/v1/system/networking/bgp-message-history?asn=64512",
        &old_version,
    )
    .await;
    assert_eq!(
        response,
        json!({
            "switch_histories": [],
        })
    );

    let initial_version = nexus_external_api::VERSION_INITIAL.to_string();
    let response = versioned_get(
        &versioned_client,
        "/v1/system/networking/bgp-routes-ipv4?asn=64512",
        &initial_version,
    )
    .await;
    assert_eq!(response, json!([]));
}

async fn versioned_get(
    client: &ClientTestContext,
    url: &str,
    version: &str,
) -> Value {
    NexusRequest::new(
        RequestBuilder::new(client, Method::GET, url)
            .header(omicron_common::api::VERSION_HEADER, version),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap()
    .await
}

fn assert_mgd_unresolved<T>(result: SwitchResult<T>) {
    match result {
        SwitchResult::Err { error: SwitchError::MgdUnresolved } => {}
        SwitchResult::Ok { .. } => {
            panic!("switch1 was unexpectedly available");
        }
        SwitchResult::Err { error } => {
            panic!("switch1 failed unexpectedly: {error:?}")
        }
    }
}
