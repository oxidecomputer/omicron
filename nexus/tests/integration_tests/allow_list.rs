// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for IP allow list endpoints.

use dropshot::test_util::ClientTestContext;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, views};
use omicron_common::api::external::AllowedSourceIps;
use std::net::IpAddr;
use std::net::Ipv4Addr;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const URL: &str = "/v1/system/networking/allow-list";

#[nexus_test]
async fn test_allow_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    // We should start with the default of any.
    let list: views::AllowList = NexusRequest::object_get(client, URL)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make GET request")
        .parsed_body()
        .unwrap();
    assert_eq!(
        list.allowed_ips,
        AllowedSourceIps::Any,
        "Should start with the default allow list of Any"
    );

    // All these requests use localhost, which makes things pretty easy.
    let our_addr = IpAddr::V4(Ipv4Addr::LOCALHOST);

    // Set the allowlist, and assert it's equal to what we set.
    async fn update_list_and_compare(
        client: &ClientTestContext,
        allowed_ips: AllowedSourceIps,
    ) {
        let new_list =
            params::AllowListUpdate { allowed_ips: allowed_ips.clone() };

        // PUT the list, which returns it, and ensure we get back what we set.
        let list: views::AllowList =
            NexusRequest::object_put(client, URL, Some(&new_list))
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .expect("failed to make PUT request")
                .parsed_body()
                .unwrap();
        assert_eq!(
            list.allowed_ips, allowed_ips,
            "Failed to update the allow list",
        );

        // GET it as well.
        let get_list: views::AllowList = NexusRequest::object_get(client, URL)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to make GET request")
            .parsed_body()
            .unwrap();
        assert_eq!(
            list.allowed_ips, get_list.allowed_ips,
            "List returned from PUT and GET should be the same",
        );
    }

    // Set the list with exactly one IP, make sure it's the same.
    let allowed_ips = AllowedSourceIps::try_from(vec![our_addr.into()])
        .expect("Expected a valid IP list");
    update_list_and_compare(client, allowed_ips).await;

    // Add our IP in the front and end, and still make sure that works.
    //
    // This is a regression for
    // https://github.com/oxidecomputer/omicron/issues/5727.
    let addrs =
        vec![our_addr.into(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)).into()];
    let allowed_ips = AllowedSourceIps::try_from(addrs.clone())
        .expect("Expected a valid IP list");
    update_list_and_compare(client, allowed_ips).await;

    let addrs = addrs.into_iter().rev().collect::<Vec<_>>();
    let allowed_ips =
        AllowedSourceIps::try_from(addrs).expect("Expected a valid IP list");
    update_list_and_compare(client, allowed_ips).await;

    // Set back to any
    update_list_and_compare(client, AllowedSourceIps::Any).await;

    // Check that we cannot make the request with a list that doesn't include
    // us.
    let addrs = vec![IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)).into()];
    let allowed_ips = AllowedSourceIps::try_from(addrs.clone())
        .expect("Expected a valid IP list");
    let new_list = params::AllowListUpdate { allowed_ips: allowed_ips.clone() };
    let err: dropshot::HttpErrorResponseBody =
        NexusRequest::expect_failure_with_body(
            client,
            http::StatusCode::BAD_REQUEST,
            http::Method::PUT,
            URL,
            &new_list,
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make PUT request")
        .parsed_body()
        .expect("failed to parse error response");
    assert!(err
        .message
        .contains("would prevent access from the current client"));

    // But we _should_ be able to make this self-defeating request through the
    // techport proxy server.
    let client = &cptestctx.techport_client;
    update_list_and_compare(client, allowed_ips).await;
}
