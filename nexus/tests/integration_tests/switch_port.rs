// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for operating on Ports

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    Address, AddressConfig, AddressLotBlockCreate, AddressLotCreate,
    BgpAnnounceSetCreate, BgpAnnouncementCreate, BgpConfigCreate,
    BgpPeerConfig, LinkConfigCreate, LldpServiceConfigCreate, Route,
    RouteConfig, SwitchInterfaceConfigCreate, SwitchInterfaceKind,
    SwitchPortApplySettings, SwitchPortSettingsCreate,
};
use nexus_types::external_api::views::Rack;
use omicron_common::api::external::ImportExportPolicy;
use omicron_common::api::external::{
    self, AddressLotKind, BgpPeer, IdentityMetadataCreateParams, LinkFec,
    LinkSpeed, NameOrId, SwitchPort, SwitchPortSettingsView,
};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// TODO: unfortunately this test can no longer be run in the integration test
//       suite because it depends on communicating with MGS which is not part
//       of the infrastructure available in the integration test context.
#[ignore]
#[nexus_test]
async fn test_port_settings_basic_crud(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    // Create a lot
    let lot_params = AddressLotCreate {
        identity: IdentityMetadataCreateParams {
            name: "parkinglot".parse().unwrap(),
            description: "an address parking lot".into(),
        },
        kind: AddressLotKind::Infra,
        blocks: vec![
            AddressLotBlockCreate {
                first_address: "203.0.113.10".parse().unwrap(),
                last_address: "203.0.113.20".parse().unwrap(),
            },
            AddressLotBlockCreate {
                first_address: "1.2.3.0".parse().unwrap(),
                last_address: "1.2.3.255".parse().unwrap(),
            },
        ],
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

    // Create BGP announce set
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

    NexusRequest::objects_post(
        client,
        "/v1/system/networking/bgp-announce-set",
        &announce_set,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Create BGP config
    let bgp_config = BgpConfigCreate {
        identity: IdentityMetadataCreateParams {
            name: "as47".parse().unwrap(),
            description: "autonomous system 47".into(),
        },
        bgp_announce_set_id: NameOrId::Name("instances".parse().unwrap()),
        asn: 47,
        vrf: None,
        checker: None,
        shaper: None,
    };

    NexusRequest::objects_post(
        client,
        "/v1/system/networking/bgp",
        &bgp_config,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Create port settings
    let mut settings =
        SwitchPortSettingsCreate::new(IdentityMetadataCreateParams {
            name: "portofino".parse().unwrap(),
            description: "just a port".into(),
        });
    // links
    settings.links.insert(
        "phy0".into(),
        LinkConfigCreate {
            mtu: 4700,
            lldp: LldpServiceConfigCreate { enabled: false, lldp_config: None },
            fec: LinkFec::None,
            speed: LinkSpeed::Speed100G,
            autoneg: false,
        },
    );
    // interfaces
    settings.interfaces.insert(
        "phy0".into(),
        SwitchInterfaceConfigCreate {
            v6_enabled: true,
            kind: SwitchInterfaceKind::Primary,
        },
    );
    // routes
    settings.routes.insert(
        "phy0".into(),
        RouteConfig {
            routes: vec![Route {
                dst: "1.2.3.0/24".parse().unwrap(),
                gw: "1.2.3.4".parse().unwrap(),
                vid: None,
                local_pref: None,
            }],
        },
    );
    // addresses
    settings.addresses.insert(
        "phy0".into(),
        AddressConfig {
            addresses: vec![Address {
                address: "203.0.113.10/24".parse().unwrap(),
                vlan_id: None,
                address_lot: NameOrId::Name("parkinglot".parse().unwrap()),
            }],
        },
    );

    let created: SwitchPortSettingsView = NexusRequest::objects_post(
        client,
        "/v1/system/networking/switch-port-settings",
        &settings,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(created.links.len(), 1);
    assert_eq!(created.routes.len(), 1);
    assert_eq!(created.addresses.len(), 1);

    let link0 = &created.links[0];
    assert_eq!(&link0.link_name, "phy0");
    assert_eq!(link0.mtu, 4700);

    let lldp0 = &created.link_lldp[0];
    assert_eq!(lldp0.enabled, false);
    assert_eq!(lldp0.lldp_config_id, None);

    let ifx0 = &created.interfaces[0];
    assert_eq!(&ifx0.interface_name, "phy0");
    assert_eq!(ifx0.v6_enabled, true);
    assert_eq!(ifx0.kind, external::SwitchInterfaceKind::Primary);

    let route0 = &created.routes[0];
    assert_eq!(route0.dst, "1.2.3.0/24".parse().unwrap());
    assert_eq!(route0.gw, "1.2.3.4".parse().unwrap());

    let addr0 = &created.addresses[0];
    assert_eq!(addr0.address, "203.0.113.10/24".parse().unwrap());

    // Get the port settings back
    let roundtrip: SwitchPortSettingsView = NexusRequest::object_get(
        client,
        "/v1/system/networking/switch-port-settings/portofino",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(roundtrip.links.len(), 1);
    assert_eq!(roundtrip.routes.len(), 1);
    assert_eq!(roundtrip.addresses.len(), 1);

    let link0 = &roundtrip.links[0];
    assert_eq!(&link0.link_name, "phy0");
    assert_eq!(link0.mtu, 4700);

    let lldp0 = &roundtrip.link_lldp[0];
    assert_eq!(lldp0.enabled, false);
    assert_eq!(lldp0.lldp_config_id, None);

    let ifx0 = &roundtrip.interfaces[0];
    assert_eq!(&ifx0.interface_name, "phy0");
    assert_eq!(ifx0.v6_enabled, true);
    assert_eq!(ifx0.kind, external::SwitchInterfaceKind::Primary);

    let route0 = &roundtrip.routes[0];
    assert_eq!(route0.dst, "1.2.3.0/24".parse().unwrap());
    assert_eq!(route0.gw, "1.2.3.4".parse().unwrap());

    let addr0 = &roundtrip.addresses[0];
    assert_eq!(addr0.address, "203.0.113.10/24".parse().unwrap());

    // Delete port settings
    NexusRequest::object_delete(
        client,
        &format!(
            "{}?port_settings={}",
            "/v1/system/networking/switch-port-settings", "portofino",
        ),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Create same port settings again. Should not see conflict.
    let _created: SwitchPortSettingsView = NexusRequest::objects_post(
        client,
        "/v1/system/networking/switch-port-settings",
        &settings,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Update port settings. Should not see conflict.
    settings.bgp_peers.insert(
        "phy0".into(),
        BgpPeerConfig {
            peers: vec![BgpPeer {
                bgp_config: NameOrId::Name("as47".parse().unwrap()),
                interface_name: "phy0".to_string(),
                addr: "1.2.3.4".parse().unwrap(),
                hold_time: 6,
                idle_hold_time: 6,
                delay_open: 0,
                connect_retry: 3,
                keepalive: 2,
                remote_asn: None,
                min_ttl: None,
                md5_auth_key: None,
                multi_exit_discriminator: None,
                communities: Vec::new(),
                local_pref: None,
                enforce_first_as: false,
                allowed_export: ImportExportPolicy::NoFiltering,
                allowed_import: ImportExportPolicy::NoFiltering,
                vlan_id: None,
            }],
        },
    );
    let _created: SwitchPortSettingsView = NexusRequest::objects_post(
        client,
        "/v1/system/networking/switch-port-settings",
        &settings,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // There should be one switch port to begin with, see
    // Server::start_and_populate in nexus/src/lib.rs

    let ports = NexusRequest::iter_collection_authn::<SwitchPort>(
        client,
        "/v1/system/hardware/switch-port",
        "",
        None,
    )
    .await
    .expect("Failed to list switch ports")
    .all_items;

    assert_eq!(ports.len(), 1, "Expected one ports");

    // apply port settings

    let apply_settings = SwitchPortApplySettings {
        port_settings: NameOrId::Name("portofino".parse().unwrap()),
    };

    let racks_url = "/v1/system/hardware/racks";
    let racks: Vec<Rack> =
        NexusRequest::iter_collection_authn(client, racks_url, "", None)
            .await
            .expect("failed to list racks")
            .all_items;

    let rack_id = racks[0].identity.id;

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            &format!("/v1/system/hardware/switch-port/qsfp0/settings?rack_id={rack_id}&switch_location=switch0"),
        )
        .body(Some(&apply_settings))
        .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // clear port settings

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::DELETE,
            &format!("/v1/system/hardware/switch-port/qsfp0/settings?rack_id={rack_id}&switch_location=switch0"),
        )
        .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}
