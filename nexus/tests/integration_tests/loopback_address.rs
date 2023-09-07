// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for operating on Loopback Addresses

use dropshot::HttpErrorResponseBody;
use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    AddressLotBlockCreate, AddressLotCreate, LoopbackAddressCreate,
};
use nexus_types::external_api::views::Rack;
use omicron_common::api::external::{
    AddressLotKind, IdentityMetadataCreateParams, LoopbackAddress, NameOrId,
};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_loopback_address_basic_crud(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    // Create a lot
    let lot_params = AddressLotCreate {
        identity: IdentityMetadataCreateParams {
            name: "parkinglot".parse().unwrap(),
            description: "an address parking lot".into(),
        },
        kind: AddressLotKind::Infra,
        blocks: vec![AddressLotBlockCreate {
            first_address: "203.0.113.10".parse().unwrap(),
            last_address: "203.0.113.100".parse().unwrap(),
        }],
    };

    NexusRequest::objects_post(
        client,
        "/v1/system/networking/address-lot",
        &lot_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Verify there are no loopback addresses
    let addrs = NexusRequest::iter_collection_authn::<LoopbackAddress>(
        client,
        "/v1/system/networking/loopback-address",
        "",
        None,
    )
    .await
    .expect("Failed to list loopback addresses")
    .all_items;

    assert_eq!(addrs.len(), 0, "Expected no loopback addresses");

    let racks_url = "/v1/system/hardware/racks";
    let racks: Vec<Rack> =
        NexusRequest::iter_collection_authn(client, racks_url, "", None)
            .await
            .expect("failed to list racks")
            .all_items;

    let rack_id = racks[0].identity.id;

    // Create a loopback address
    let params = LoopbackAddressCreate {
        address_lot: NameOrId::Name("parkinglot".parse().unwrap()),
        rack_id: rack_id,
        switch_location: "switch0".parse().unwrap(),
        address: "203.0.113.99".parse().unwrap(),
        mask: 24,
        anycast: false,
    };
    let addr: LoopbackAddress = NexusRequest::objects_post(
        client,
        "/v1/system/networking/loopback-address",
        &params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(addr.address.ip(), params.address);
    assert_eq!(addr.address.prefix(), params.mask);
    assert_eq!(addr.rack_id, params.rack_id);
    assert_eq!(addr.switch_location, params.switch_location.to_string());

    // Verify conflict error on recreate
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            "/v1/system/networking/loopback-address",
        )
        .body(Some(&params))
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "address unavailable".to_string());

    // Verify there loopback addresses
    let addrs = NexusRequest::iter_collection_authn::<LoopbackAddress>(
        client,
        "/v1/system/networking/loopback-address",
        "",
        None,
    )
    .await
    .expect("Failed to list loopback addresses")
    .all_items;

    assert_eq!(addrs.len(), 1, "Expected 1 loopback address");
    assert_eq!(addrs[0].address.ip(), params.address);
    assert_eq!(addrs[0].address.prefix(), params.mask);

    // Verify error when deleting lot while in use
    let _error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::DELETE,
            "/v1/system/networking/address-lot/parkinglot",
        )
        .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Delete loopback address
    NexusRequest::object_delete(
        client,
        &format!(
            "{}/{}/{}/{}/{}",
            "/v1/system/networking/loopback-address",
            rack_id,
            "switch0",
            "203.0.113.99",
            24,
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Verify there are no addresses
    let addrs = NexusRequest::iter_collection_authn::<LoopbackAddress>(
        client,
        "/v1/system/networking/loopback-address",
        "",
        None,
    )
    .await
    .expect("Failed to list loopback addresses")
    .all_items;

    assert_eq!(addrs.len(), 0, "Expected no loopback addresses after delete");

    // Verify we can now delete the address lot.
    NexusRequest::object_delete(
        client,
        "/v1/system/networking/address-lot/parkinglot",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Create again after delete should work without conflict.
    NexusRequest::objects_post(
        client,
        "/v1/system/networking/address-lot",
        &lot_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    let _addr: LoopbackAddress = NexusRequest::objects_post(
        client,
        "/v1/system/networking/loopback-address",
        &params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
}

#[nexus_test]
async fn test_anycast_loopback_address_basic_crud(
    ctx: &ControlPlaneTestContext,
) {
    let client = &ctx.external_client;

    // Create a lot
    let lot_params = AddressLotCreate {
        identity: IdentityMetadataCreateParams {
            name: "parkinglot".parse().unwrap(),
            description: "an address parking lot".into(),
        },
        kind: AddressLotKind::Infra,
        blocks: vec![AddressLotBlockCreate {
            first_address: "203.0.113.10".parse().unwrap(),
            last_address: "203.0.113.100".parse().unwrap(),
        }],
    };

    NexusRequest::objects_post(
        client,
        "/v1/system/networking/address-lot",
        &lot_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Verify there are no loopback addresses
    let addrs = NexusRequest::iter_collection_authn::<LoopbackAddress>(
        client,
        "/v1/system/networking/loopback-address",
        "",
        None,
    )
    .await
    .expect("Failed to list loopback addresses")
    .all_items;

    assert_eq!(addrs.len(), 0, "Expected no loopback addresses");

    let racks_url = "/v1/system/hardware/racks";
    let racks: Vec<Rack> =
        NexusRequest::iter_collection_authn(client, racks_url, "", None)
            .await
            .expect("failed to list racks")
            .all_items;

    let rack_id = racks[0].identity.id;

    // Create an anycast loopback address
    let params = LoopbackAddressCreate {
        address_lot: NameOrId::Name("parkinglot".parse().unwrap()),
        rack_id,
        switch_location: "switch0".parse().unwrap(),
        address: "203.0.113.99".parse().unwrap(),
        mask: 24,
        anycast: true,
    };
    let addr: LoopbackAddress = NexusRequest::objects_post(
        client,
        "/v1/system/networking/loopback-address",
        &params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(addr.address.ip(), params.address);
    assert_eq!(addr.address.prefix(), params.mask);
    assert_eq!(addr.rack_id, params.rack_id);
    assert_eq!(addr.switch_location, params.switch_location.to_string());

    // Create a second anycast record for another switch
    let params = LoopbackAddressCreate {
        address_lot: NameOrId::Name("parkinglot".parse().unwrap()),
        rack_id,
        switch_location: "switch1".parse().unwrap(),
        address: "203.0.113.99".parse().unwrap(),
        mask: 24,
        anycast: true,
    };
    let addr: LoopbackAddress = NexusRequest::objects_post(
        client,
        "/v1/system/networking/loopback-address",
        &params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(addr.address.ip(), params.address);
    assert_eq!(addr.address.prefix(), params.mask);
    assert_eq!(addr.rack_id, params.rack_id);
    assert_eq!(addr.switch_location, params.switch_location.to_string());

    // Verify there are two anycast loopback addresses
    let addrs = NexusRequest::iter_collection_authn::<LoopbackAddress>(
        client,
        "/v1/system/networking/loopback-address",
        "",
        None,
    )
    .await
    .expect("Failed to list loopback addresses")
    .all_items;

    assert_eq!(addrs.len(), 2, "Expected 2 loopback addresses");
    assert_eq!(addrs[0].address.ip(), params.address);
    assert_eq!(addrs[0].address.prefix(), params.mask);

    // Delete anycast loopback addresses
    NexusRequest::object_delete(
        client,
        &format!(
            "{}/{}/{}/{}/{}",
            "/v1/system/networking/loopback-address",
            rack_id,
            "switch0",
            "203.0.113.99",
            24,
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Verify there is only one anycast loopback address
    let addrs = NexusRequest::iter_collection_authn::<LoopbackAddress>(
        client,
        "/v1/system/networking/loopback-address",
        "",
        None,
    )
    .await
    .expect("Failed to list loopback addresses")
    .all_items;

    assert_eq!(addrs.len(), 1, "Expected 1 loopback address");
    assert_eq!(addrs[0].address.ip(), params.address);
    assert_eq!(addrs[0].address.prefix(), params.mask);

    NexusRequest::object_delete(
        client,
        &format!(
            "{}/{}/{}/{}/{}",
            "/v1/system/networking/loopback-address",
            rack_id,
            "switch1",
            "203.0.113.99",
            24,
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Verify there are no addresses
    let addrs = NexusRequest::iter_collection_authn::<LoopbackAddress>(
        client,
        "/v1/system/networking/loopback-address",
        "",
        None,
    )
    .await
    .expect("Failed to list loopback addresses")
    .all_items;

    assert_eq!(addrs.len(), 0, "Expected no loopback addresses after delete");
}
