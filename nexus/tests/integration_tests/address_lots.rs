// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for operating on Address Lots

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    AddressLotBlockCreate, AddressLotCreate,
};
use omicron_common::api::external::{
    AddressLot, AddressLotBlock, AddressLotCreateResponse, AddressLotKind,
    IdentityMetadataCreateParams,
};
use std::net::IpAddr;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_address_lot_basic_crud(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    // Verify there is only one system lot
    let lots = NexusRequest::iter_collection_authn::<AddressLot>(
        client,
        "/v1/system/networking/address-lot",
        "",
        None,
    )
    .await
    .expect("Failed to list address lots")
    .all_items;
    assert_eq!(lots.len(), 1, "Expected one lot");

    // Create a lot
    let params = AddressLotCreate {
        identity: IdentityMetadataCreateParams {
            name: "parkinglot".parse().unwrap(),
            description: "an address parking lot".into(),
        },
        kind: AddressLotKind::Infra,
    };

    let response: AddressLotCreateResponse = NexusRequest::objects_post(
        client,
        "/v1/system/networking/address-lot",
        &params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    let address_lot = response.lot;

    let block_params = AddressLotBlockCreate {
        first_address: "203.0.113.10".parse().unwrap(),
        last_address: "203.0.113.20".parse().unwrap(),
    };

    NexusRequest::objects_post(
        client,
        "/v1/system/networking/address-lot/parkinglot/blocks",
        &block_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Verify there are lot blocks
    let blocks = NexusRequest::iter_collection_authn::<AddressLotBlock>(
        client,
        "/v1/system/networking/address-lot/parkinglot/blocks",
        "",
        None,
    )
    .await
    .expect("Failed to list address lot blocks")
    .all_items;

    assert_eq!(address_lot.identity.name, params.identity.name);
    assert_eq!(address_lot.identity.description, params.identity.description);
    assert_eq!(blocks.len(), 1, "Expected 1 address lot block");
    assert_eq!(
        blocks[0].first_address,
        "203.0.113.10".parse::<IpAddr>().unwrap()
    );
    assert_eq!(
        blocks[0].last_address,
        "203.0.113.20".parse::<IpAddr>().unwrap()
    );

    // Verify there are lots
    let lots = NexusRequest::iter_collection_authn::<AddressLot>(
        client,
        "/v1/system/networking/address-lot",
        "",
        None,
    )
    .await
    .expect("Failed to list address lots")
    .all_items;

    assert_eq!(lots.len(), 2, "Expected 2 lots");
    assert_eq!(lots[1], address_lot);
}

#[nexus_test]
async fn test_address_lot_invalid_range(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    let mut params = Vec::new();

    // Try to create a lot with different address families
    params.push((
        AddressLotCreate {
            identity: IdentityMetadataCreateParams {
                name: "family".parse().unwrap(),
                description: "an address parking lot".into(),
            },
            kind: AddressLotKind::Infra,
        },
        AddressLotBlockCreate {
            first_address: "203.0.113.10".parse().unwrap(),
            last_address: "fd00:1701::d".parse().unwrap(),
        },
    ));

    // Try to create an IPv4 lot where the first address comes after the second.
    params.push((
        AddressLotCreate {
            identity: IdentityMetadataCreateParams {
                name: "v4".parse().unwrap(),
                description: "an address parking lot".into(),
            },
            kind: AddressLotKind::Infra,
        },
        AddressLotBlockCreate {
            first_address: "203.0.113.20".parse().unwrap(),
            last_address: "203.0.113.10".parse().unwrap(),
        },
    ));

    // Try to create an IPv6 lot where the first address comes after the second.
    params.push((
        AddressLotCreate {
            identity: IdentityMetadataCreateParams {
                name: "v6".parse().unwrap(),
                description: "an address parking lot".into(),
            },
            kind: AddressLotKind::Infra,
        },
        AddressLotBlockCreate {
            first_address: "fd00:1701::d".parse().unwrap(),
            last_address: "fd00:1701::a".parse().unwrap(),
        },
    ));

    for (lot_params, block_params) in &params {
        NexusRequest::new(
            RequestBuilder::new(
                client,
                Method::POST,
                "/v1/system/networking/address-lot",
            )
            .body(Some(&lot_params))
            .expect_status(Some(StatusCode::CREATED)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

        NexusRequest::new(
            RequestBuilder::new(
                client,
                Method::POST,
                &format!(
                    "/v1/system/networking/address-lot/{}/blocks",
                    lot_params.identity.name
                ),
            )
            .body(Some(&block_params))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap_or_else(|_| panic!("unexpected success for: {:#?}", params))
        .parsed_body::<dropshot::HttpErrorResponseBody>()
        .unwrap();
    }
}
