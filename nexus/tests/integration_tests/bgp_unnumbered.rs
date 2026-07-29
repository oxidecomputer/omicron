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
    SwitchResult, SwitchResults, SwitchUnavailableReason, UnnumberedInterfaces,
    UnnumberedManagerState,
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
        SwitchResult::Available { value } => {
            assert!(value.interfaces.is_empty());
        }
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::MgdUnresolved,
        } => panic!("switch0 MGD was unexpectedly unresolved"),
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::QueryFailed,
        } => panic!("switch0 manager-state query unexpectedly failed"),
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
        SwitchResult::Available { value: UnnumberedInterfaces(interfaces) } => {
            assert!(interfaces.is_empty());
        }
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::MgdUnresolved,
        } => panic!("switch0 MGD was unexpectedly unresolved"),
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::QueryFailed,
        } => panic!("switch0 interface query unexpectedly failed"),
    }
    assert_mgd_unresolved(interfaces.switch1);
}

#[nexus_test]
async fn test_bgp_aggregate_status_api_versions(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let available_empty = json!({
        "switch0": { "status": "available", "value": [] },
        "switch1": {
            "status": "unavailable",
            "reason": "mgd_unresolved",
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
    let response: Value = NexusRequest::object_get(
        client,
        "/v1/system/networking/bgp-message-history?asn=64512",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap()
    .await;
    assert_eq!(
        response,
        json!({
            "switch0": {
                "status": "unavailable",
                "reason": "query_failed",
            },
            "switch1": {
                "status": "unavailable",
                "reason": "mgd_unresolved",
            },
        })
    );

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
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::MgdUnresolved,
        } => {}
        SwitchResult::Available { .. } => {
            panic!("switch1 was unexpectedly available");
        }
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::QueryFailed,
        } => panic!("switch1 query unexpectedly failed"),
    }
}
