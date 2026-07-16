// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for router configurations

use http::StatusCode;
use http::method::Method;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::networking::{
    AddressLotBlockCreate, AddressLotCreate, AddressLotKind, BfdPeer,
    BgpAnnounceSetCreate, BgpAnnouncementCreate, BgpPeerKind,
    RouterConfiguration, RouterConfigurationBgpConfig,
    RouterConfigurationBgpConfigSet, RouterConfigurationBgpPeer,
    RouterConfigurationCreate, RouterConfigurationUpdate, StaticRoute,
};
use omicron_common::api::external::{
    IdentityMetadataCreateParams, IdentityMetadataUpdateParams, NameOrId,
};
use sled_agent_types::early_networking::BfdMode;
use sled_agent_types::early_networking::ImportExportPolicy;
use sled_agent_types::early_networking::MaxPathConfig;
use sled_agent_types::early_networking::SwitchSlot;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const CONFIGURATIONS_URL: &str = "/v1/system/networking/router-configurations";

/// Create the address lot and BGP announce set that BGP configurations
/// reference.
async fn create_announce_set(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    let lot_params = AddressLotCreate {
        identity: IdentityMetadataCreateParams {
            name: "parkinglot".parse().unwrap(),
            description: "an address parking lot".into(),
        },
        kind: AddressLotKind::Infra,
        blocks: vec![AddressLotBlockCreate {
            first_address: "1.2.3.0".parse().unwrap(),
            last_address: "1.2.3.255".parse().unwrap(),
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

    let announce_set = BgpAnnounceSetCreate {
        identity: IdentityMetadataCreateParams {
            name: "instances".parse().unwrap(),
            description: "autonomous system 47 announcements".into(),
        },
        announcement: vec![BgpAnnouncementCreate {
            address_lot_block: NameOrId::Name("parkinglot".parse().unwrap()),
            network: "1.2.3.0/24".parse().unwrap(),
        }],
    };
    NexusRequest::object_put(
        client,
        "/v1/system/networking/bgp-announce-set",
        Some(&announce_set),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn test_router_configuration_basic_crud(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;
    let configuration_url = format!("{CONFIGURATIONS_URL}/routy");

    // Initially there are no router configurations.
    let configurations = NexusRequest::iter_collection_authn::<
        RouterConfiguration,
    >(client, CONFIGURATIONS_URL, "", None)
    .await
    .unwrap()
    .all_items;
    assert!(configurations.is_empty());

    // Fetching a nonexistent router configuration fails.
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &configuration_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Create a router configuration.
    let params = RouterConfigurationCreate {
        identity: IdentityMetadataCreateParams {
            name: "routy".parse().unwrap(),
            description: "just a router configuration".into(),
        },
    };
    let created: RouterConfiguration =
        NexusRequest::objects_post(client, CONFIGURATIONS_URL, &params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(created.identity.name, "routy");
    assert_eq!(created.identity.description, "just a router configuration");
    assert_eq!(created.bgp_config, None);
    assert!(created.bgp_peers.is_empty());
    assert!(created.routes.is_empty());
    assert!(created.bfd_peers.is_empty());

    // Creating a router configuration with the same name fails.
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, CONFIGURATIONS_URL)
            .body(Some(&params))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // The new router configuration appears in the list and can be fetched.
    let configurations = NexusRequest::iter_collection_authn::<
        RouterConfiguration,
    >(client, CONFIGURATIONS_URL, "", None)
    .await
    .unwrap()
    .all_items;
    assert_eq!(configurations.len(), 1);
    assert_eq!(configurations[0].identity.id, created.identity.id);

    let fetched: RouterConfiguration =
        NexusRequest::object_get(client, &configuration_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(fetched.identity.id, created.identity.id);

    // Update the router configuration.
    let update = RouterConfigurationUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some("routy2".parse().unwrap()),
            description: Some("a renamed router configuration".into()),
        },
    };
    let updated: RouterConfiguration =
        NexusRequest::object_put(client, &configuration_url, Some(&update))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(updated.identity.id, created.identity.id);
    assert_eq!(updated.identity.name, "routy2");
    assert_eq!(updated.identity.description, "a renamed router configuration");

    // The router configuration can be fetched by its new name and deleted.
    let configuration_url = format!("{CONFIGURATIONS_URL}/routy2");
    NexusRequest::object_get(client, &configuration_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    NexusRequest::object_delete(client, &configuration_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &configuration_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn test_router_configuration_bgp_config(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;
    create_announce_set(ctx).await;

    // Create a router configuration.
    let params = RouterConfigurationCreate {
        identity: IdentityMetadataCreateParams {
            name: "routy".parse().unwrap(),
            description: "just a router configuration".into(),
        },
    };
    NexusRequest::objects_post(client, CONFIGURATIONS_URL, &params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // There's no BGP configuration yet.
    let bgp_config_url = format!("{CONFIGURATIONS_URL}/routy/bgp-config");
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &bgp_config_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Create the BGP configuration.
    let set = RouterConfigurationBgpConfigSet {
        asn: 47,
        max_paths: Default::default(),
        bgp_announce_set: NameOrId::Name("instances".parse().unwrap()),
    };
    let bgp_config: RouterConfigurationBgpConfig =
        NexusRequest::object_put(client, &bgp_config_url, Some(&set))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(bgp_config.asn, 47);
    assert_eq!(bgp_config.max_paths, MaxPathConfig::new(1).unwrap());

    // The BGP configuration can be fetched, and appears in the router
    // configuration view.
    let fetched: RouterConfigurationBgpConfig =
        NexusRequest::object_get(client, &bgp_config_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(fetched, bgp_config);

    let configuration: RouterConfiguration = NexusRequest::object_get(
        client,
        &format!("{CONFIGURATIONS_URL}/routy"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(configuration.bgp_config, Some(bgp_config));

    // Setting the BGP configuration again replaces it.
    let replace = RouterConfigurationBgpConfigSet {
        asn: 65001,
        max_paths: MaxPathConfig::new(4).unwrap(),
        bgp_announce_set: NameOrId::Name("instances".parse().unwrap()),
    };
    let replaced: RouterConfigurationBgpConfig =
        NexusRequest::object_put(client, &bgp_config_url, Some(&replace))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(replaced.asn, 65001);
    assert_eq!(replaced.max_paths, MaxPathConfig::new(4).unwrap());

    // Referencing a nonexistent announce set fails.
    let bad_set = RouterConfigurationBgpConfigSet {
        asn: 47,
        max_paths: Default::default(),
        bgp_announce_set: NameOrId::Name("no-such-set".parse().unwrap()),
    };
    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &bgp_config_url)
            .body(Some(&bad_set))
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Remove the BGP configuration.
    NexusRequest::object_delete(client, &bgp_config_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &bgp_config_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Removing it again is fine (idempotent).
    NexusRequest::object_delete(client, &bgp_config_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
}

fn demo_bgp_peer() -> RouterConfigurationBgpPeer {
    RouterConfigurationBgpPeer {
        name: "spine1".parse().unwrap(),
        peer: BgpPeerKind::Numbered {
            addr: "203.0.113.10".parse().unwrap(),
            port: "qsfp0".parse().unwrap(),
        },
        remote_asn: 65001,
        allowed_import: ImportExportPolicy::NoFiltering,
        allowed_export: ImportExportPolicy::NoFiltering,
        hold_time: 6,
        keepalive: 2,
        connect_retry: 3,
        delay_open: 0,
        idle_hold_time: 3,
        local_pref: None,
        communities: vec![100, 200],
        multi_exit_discriminator: None,
        enforce_first_as: true,
        md5_auth_key: None,
        min_ttl: Some(255),
        vlan_id: None,
        router_lifetime: Default::default(),
    }
}

fn demo_static_route() -> StaticRoute {
    StaticRoute {
        name: "default".parse().unwrap(),
        dst: "0.0.0.0/0".parse().unwrap(),
        gw: "203.0.113.1".parse().unwrap(),
        rib_priority: Some(1),
        vlan_id: None,
    }
}

fn demo_bfd_peer() -> BfdPeer {
    BfdPeer {
        name: "spine1-bfd".parse().unwrap(),
        remote: "203.0.113.10".parse().unwrap(),
        local: None,
        mode: BfdMode::MultiHop,
        detection_threshold: 3,
        required_rx: 1000000,
        switch: SwitchSlot::Switch0,
    }
}

#[nexus_test]
async fn test_router_configuration_sub_resources(
    ctx: &ControlPlaneTestContext,
) {
    let client = &ctx.external_client;

    // Create a router configuration.
    let params = RouterConfigurationCreate {
        identity: IdentityMetadataCreateParams {
            name: "routy".parse().unwrap(),
            description: "just a router configuration".into(),
        },
    };
    NexusRequest::objects_post(client, CONFIGURATIONS_URL, &params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    let bgp_peers_url = format!("{CONFIGURATIONS_URL}/routy/bgp-peers");
    let routes_url = format!("{CONFIGURATIONS_URL}/routy/routes");
    let bfd_peers_url = format!("{CONFIGURATIONS_URL}/routy/bfd-peers");

    // All entry collections start out empty.
    let bgp_peers: Vec<RouterConfigurationBgpPeer> =
        NexusRequest::object_get(client, &bgp_peers_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert!(bgp_peers.is_empty());

    // Create one entry of each kind.
    let bgp_peer = demo_bgp_peer();
    let created_peer: RouterConfigurationBgpPeer =
        NexusRequest::objects_post(client, &bgp_peers_url, &bgp_peer)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(created_peer, bgp_peer);

    let route = demo_static_route();
    let created_route: StaticRoute =
        NexusRequest::objects_post(client, &routes_url, &route)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(created_route, route);

    let bfd_peer = demo_bfd_peer();
    let created_bfd: BfdPeer =
        NexusRequest::objects_post(client, &bfd_peers_url, &bfd_peer)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(created_bfd, bfd_peer);

    // Creating an entry with a duplicate name fails.
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &bgp_peers_url)
            .body(Some(&bgp_peer))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // The entries appear in the parent router configuration view.
    let configuration: RouterConfiguration = NexusRequest::object_get(
        client,
        &format!("{CONFIGURATIONS_URL}/routy"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(configuration.bgp_peers, vec![bgp_peer.clone()]);
    assert_eq!(configuration.routes, vec![route.clone()]);
    assert_eq!(configuration.bfd_peers, vec![bfd_peer.clone()]);

    // Entries can be fetched individually.
    let bgp_peer_url = format!("{bgp_peers_url}/spine1");
    let fetched: RouterConfigurationBgpPeer =
        NexusRequest::object_get(client, &bgp_peer_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(fetched, bgp_peer);

    // Fetching a nonexistent entry fails.
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &format!("{bgp_peers_url}/no-such-peer"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Update an entry, including a rename.
    let mut updated_peer = bgp_peer.clone();
    updated_peer.name = "spine2".parse().unwrap();
    updated_peer.remote_asn = 65002;
    let updated: RouterConfigurationBgpPeer =
        NexusRequest::object_put(client, &bgp_peer_url, Some(&updated_peer))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(updated, updated_peer);

    // The entry is now reachable under its new name, not the old one.
    let bgp_peer_url = format!("{bgp_peers_url}/spine2");
    NexusRequest::object_get(client, &bgp_peer_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &format!("{bgp_peers_url}/spine1"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Updating a nonexistent entry fails.
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PUT,
            &format!("{routes_url}/no-such-route"),
        )
        .body(Some(&route))
        .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Remove the entries.
    NexusRequest::object_delete(client, &bgp_peer_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // Removing an entry twice fails.
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::DELETE,
        &bgp_peer_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Deleting the parent router configuration removes its entries: a new
    // router configuration created with the same name starts out empty.
    NexusRequest::object_delete(client, &format!("{CONFIGURATIONS_URL}/routy"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    NexusRequest::objects_post(client, CONFIGURATIONS_URL, &params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    let configuration: RouterConfiguration = NexusRequest::object_get(
        client,
        &format!("{CONFIGURATIONS_URL}/routy"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert!(configuration.bgp_peers.is_empty());
    assert!(configuration.routes.is_empty());
    assert!(configuration.bfd_peers.is_empty());
}
