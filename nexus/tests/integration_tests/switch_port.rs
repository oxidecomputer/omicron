// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for operating on Ports

use std::str::FromStr;

use http::StatusCode;
use http::method::Method;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    Address, AddressConfig, AddressLotBlockCreate, AddressLotCreate,
    BgpAnnounceSetCreate, BgpAnnouncementCreate, BgpConfigCreate,
    BgpPeerConfig, LinkConfigCreate, LldpLinkConfigCreate, Route, RouteConfig,
    SwitchInterfaceConfigCreate, SwitchInterfaceKind, SwitchPortApplySettings,
    SwitchPortSettingsCreate,
};
use nexus_types::external_api::views::Rack;
use omicron_common::api::external::{
    self, AddressLotKind, BgpPeer, IdentityMetadataCreateParams, LinkFec,
    LinkSpeed, NameOrId, SwitchLocation, SwitchPort, SwitchPortSettings,
};
use omicron_common::api::external::{ImportExportPolicy, Name};
use oxnet::IpNet;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

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

    NexusRequest::object_put(
        client,
        "/v1/system/networking/bgp-announce-set",
        Some(&announce_set),
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

    let link0_name =
        Name::from_str("phy0").expect("phy0 should be a valid name");

    let lldp0_params = LldpLinkConfigCreate {
        enabled: true,
        link_name: Some("Link Name".into()),
        link_description: Some("link description".into()),
        chassis_id: Some("Chassis ID".into()),
        system_name: Some("System Name".into()),
        system_description: Some("System description".into()),
        management_ip: Some(
            "203.0.113.10"
                .parse()
                .expect("management_ip should be a valid address"),
        ),
    };

    let link1_name =
        Name::from_str("phy1").expect("phy1 should be a valid name");

    let lldp1_params = LldpLinkConfigCreate {
        enabled: true,
        link_name: Some("Link Name 2".into()),
        link_description: Some("link description".into()),
        chassis_id: Some("Chassis ID".into()),
        system_name: Some("System Name".into()),
        system_description: Some("System description".into()),
        management_ip: Some(
            "203.0.113.10"
                .parse()
                .expect("management_ip should be a valid address"),
        ),
    };

    // links
    settings.links.push(LinkConfigCreate {
        link_name: link0_name.clone(),
        mtu: 4700,
        lldp: lldp0_params.clone(),
        fec: Some(LinkFec::None),
        speed: LinkSpeed::Speed100G,
        autoneg: false,
        tx_eq: None,
    });

    settings.links.push(LinkConfigCreate {
        link_name: link1_name.clone(),
        mtu: 4700,
        lldp: lldp1_params.clone(),
        fec: Some(LinkFec::None),
        speed: LinkSpeed::Speed100G,
        autoneg: false,
        tx_eq: None,
    });

    // interfaces
    settings.interfaces.push(SwitchInterfaceConfigCreate {
        link_name: link0_name.clone(),
        v6_enabled: true,
        kind: SwitchInterfaceKind::Primary,
    });
    // routes
    settings.routes.push(RouteConfig {
        link_name: link0_name.clone(),
        routes: vec![Route {
            dst: "1.2.3.0/24".parse().unwrap(),
            gw: "1.2.3.4".parse().unwrap(),
            vid: None,
            rib_priority: None,
        }],
    });
    // addresses
    settings.addresses.push(AddressConfig {
        link_name: link0_name.clone(),
        addresses: vec![Address {
            address: "203.0.113.10/24".parse().unwrap(),
            vlan_id: None,
            address_lot: NameOrId::Name("parkinglot".parse().unwrap()),
        }],
    });

    let created: SwitchPortSettings = NexusRequest::objects_post(
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

    assert_eq!(created.links.len(), 2);
    assert_eq!(created.routes.len(), 1);
    assert_eq!(created.addresses.len(), 1);

    let link0 = &created.links[0];
    assert_eq!(&link0.link_name.to_string(), "phy0");
    assert_eq!(link0.mtu, 4700);

    let lldp0 = link0.lldp_link_config.clone().unwrap();
    assert_eq!(lldp0, lldp0_params);

    let link1 = &created.links[1];
    assert_eq!(&link1.link_name.to_string(), "phy1");
    assert_eq!(link1.mtu, 4700);

    let lldp1 = link1.lldp_link_config.clone().unwrap();
    assert_eq!(lldp1, lldp1_params);

    let ifx0 = &created.interfaces[0];
    assert_eq!(&ifx0.interface_name.to_string(), "phy0");
    assert_eq!(ifx0.v6_enabled, true);
    assert_eq!(ifx0.kind, external::SwitchInterfaceKind::Primary);

    let route0 = &created.routes[0];
    assert_eq!(route0.dst, IpNet::from_str("1.2.3.0/24").unwrap());
    assert_eq!(&route0.gw.to_string(), "1.2.3.4");

    let addr0 = &created.addresses[0];
    assert_eq!(addr0.address, IpNet::from_str("203.0.113.10/24").unwrap());

    // Get the port settings back
    let roundtrip: SwitchPortSettings = NexusRequest::object_get(
        client,
        "/v1/system/networking/switch-port-settings/portofino",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(roundtrip.links.len(), 2);
    assert_eq!(roundtrip.routes.len(), 1);
    assert_eq!(roundtrip.addresses.len(), 1);

    let link0 = &roundtrip.links[0];
    assert_eq!(&link0.link_name.to_string(), "phy0");
    assert_eq!(link0.mtu, 4700);

    let lldp0 = link0.lldp_link_config.clone().unwrap();
    assert_eq!(lldp0, lldp0_params);

    let link1 = &roundtrip.links[1];
    assert_eq!(&link1.link_name.to_string(), "phy1");
    assert_eq!(link1.mtu, 4700);

    let lldp1 = link1.lldp_link_config.clone().unwrap();
    assert_eq!(lldp1, lldp1_params);

    let ifx0 = &roundtrip.interfaces[0];
    assert_eq!(&ifx0.interface_name.to_string(), "phy0");
    assert_eq!(ifx0.v6_enabled, true);
    assert_eq!(ifx0.kind, external::SwitchInterfaceKind::Primary);

    let route0 = &roundtrip.routes[0];
    assert_eq!(route0.dst, IpNet::from_str("1.2.3.0/24").unwrap());
    assert_eq!(&route0.gw.to_string(), "1.2.3.4");

    let addr0 = &roundtrip.addresses[0];
    assert_eq!(addr0.address, IpNet::from_str("203.0.113.10/24").unwrap());

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
    let _created: SwitchPortSettings = NexusRequest::objects_post(
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
    settings.bgp_peers.push(BgpPeerConfig {
        link_name: link0_name.clone(),
        peers: vec![BgpPeer {
            bgp_config: NameOrId::Name("as47".parse().unwrap()),
            interface_name: "phy0".parse().unwrap(),
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
    });
    let _created: SwitchPortSettings = NexusRequest::objects_post(
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

    assert_eq!(ports.len(), 2, "Expected two ports");

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

#[nexus_test(extra_sled_agents = 1)]
async fn test_port_settings_basic_v6_crud(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    // Create a lot
    let lot_name =
        Name::from_str("subspace").expect("subspace should be a valid name");
    let lot_params = AddressLotCreate {
        identity: IdentityMetadataCreateParams {
            name: lot_name.clone(),
            description: "where the comms happen".into(),
        },
        kind: AddressLotKind::Infra,
        blocks: vec![AddressLotBlockCreate {
            first_address: "1701::a".parse().unwrap(),
            last_address: "1701::e".parse().unwrap(),
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

    // Create port settings
    let settings_name =
        Name::from_str("nacelle").expect("nacell should be a valid name");
    let mut settings =
        SwitchPortSettingsCreate::new(IdentityMetadataCreateParams {
            name: settings_name.clone(),
            description: "just a port".into(),
        });

    let link_name =
        Name::from_str("phy0").expect("phy0 should be a valid name");

    settings.links.push(LinkConfigCreate {
        link_name: link_name.clone(),
        mtu: 1500,
        lldp: LldpLinkConfigCreate {
            enabled: false,
            link_name: None,
            link_description: None,
            chassis_id: None,
            system_name: None,
            system_description: None,
            management_ip: None,
        },
        fec: None,
        speed: LinkSpeed::Speed100G,
        autoneg: false,
        tx_eq: None,
    });

    settings.interfaces.push(SwitchInterfaceConfigCreate {
        link_name: link_name.clone(),
        v6_enabled: true,
        kind: SwitchInterfaceKind::Primary,
    });

    settings.addresses.push(AddressConfig {
        link_name: link_name.clone(),
        addresses: vec![Address {
            address: "1701::d/64".parse().unwrap(),
            vlan_id: None,
            address_lot: NameOrId::Name(lot_name.clone()),
        }],
    });

    settings.routes.push(RouteConfig {
        link_name: link_name.clone(),
        routes: vec![Route {
            dst: "2000::/64".parse().unwrap(),
            gw: "2000::1".parse().unwrap(),
            vid: None,
            rib_priority: None,
        }],
    });

    let created: SwitchPortSettings = NexusRequest::objects_post(
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

    let addr = &created.addresses[0];
    assert_eq!(addr.address, IpNet::from_str("1701::d/64").unwrap());

    let route = &created.routes[0];
    assert_eq!(route.dst, IpNet::from_str("2000::/64").unwrap());
    assert_eq!(&route.gw.to_string(), "2000::1");

    let mgd = &ctx.mgd[&SwitchLocation::Switch0];
    let mgd_client = mg_admin_client::Client::new(
        &format!("http://localhost:{}", mgd.port),
        ctx.logctx.log.clone(),
    );

    // apply port settings
    let apply_settings = SwitchPortApplySettings {
        port_settings: NameOrId::Name(settings_name.clone()),
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

    for _ in 0..20 {
        // wait for mgd to start
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        // ensure that our route has landed in mgd
        match mgd_client.static_list_v6_routes().await {
            Ok(routes) => {
                let n = routes.len();
                if n == 1 {
                    return;
                } else {
                    println!("expected 1 route got {n}")
                }
            }
            Err(e) => {
                println!("failed to contact mgd: {e}");
                #[cfg(target_os = "linux")]
                dump_connection_diagnostics(mgd.port);
            }
        }
    }
    panic!("expected number of routes not found");
}

#[cfg(target_os = "linux")]
pub fn dump_connection_diagnostics(port: u16) {
    let script = format!(
        r#"
echo "=== ULIMIT STATUS ==="
ulimit -a

echo -e "\n=== SYSTEM-WIDE FILE DESCRIPTOR LIMITS ==="
cat /proc/sys/fs/file-nr
cat /proc/sys/fs/file-max

echo -e "\n=== PROCESS FD COUNTS (top 20) ==="
for pid in /proc/[0-9]*; do
  fd_count=$(ls -1 ${{pid}}/fd 2>/dev/null | wc -l)
  name=$(cat ${{pid}}/comm 2>/dev/null)
  echo "$fd_count $name $(basename $pid)"
done | sort -rn | head -20

echo -e "\n=== CURRENT PROCESS FD COUNT ==="
ls -1 /proc/$$/fd | wc -l

echo -e "\n=== SOCKET STATISTICS ==="
ss -s

echo -e "\n=== TCP CONNECTIONS TO LOCALHOST (summary) ==="
ss -tn state all | grep -E '127\.0\.0\.1|::1|\[::1\]' | awk '{{print $1}}' | sort | uniq -c

echo -e "\n=== CONNECTIONS TO PORT {port} ==="
ss -tnp | grep {port}

echo -e "\n=== TIME_WAIT SOCKETS ==="
ss -tn state time-wait | wc -l

echo -e "\n=== EPHEMERAL PORT RANGE ==="
cat /proc/sys/net/ipv4/ip_local_port_range

echo -e "\n=== NETSTAT COUNTS BY STATE ==="
ss -tan | awk 'NR>1 {{print $1}}' | sort | uniq -c | sort -rn

echo -e "\n=== DAEMON PROCESS STATUS ==="
lsof -i :{port} 2>/dev/null || ss -tlnp | grep {port}

echo -e "\n=== ALL LISTENING SOCKETS (TCP + UDP) ==="
ss -tulnp

echo -e "\n=== LOCALHOST LISTENERS BY PORT ==="
ss -tlnp | grep -E '127\.0\.0\.1|::1|\*:\*' | awk '{{
  split($4, addr, ":");
  port = addr[length(addr)];
  proc = $6;
  gsub(/users:\(\("/, "", proc);
  gsub(/",pid=.*/, "", proc);
  print port, proc
}}' | sort -n

echo -e "\n=== MEMORY PRESSURE ==="
free -m
"#,
        port = port
    );

    match std::process::Command::new("bash").arg("-c").arg(&script).output() {
        Ok(output) => {
            eprintln!("=== CONNECTION DIAGNOSTICS (port {port}) ===");
            eprintln!("{}", String::from_utf8_lossy(&output.stdout));
            if !output.stderr.is_empty() {
                eprintln!(
                    "stderr: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
        Err(e) => {
            eprintln!("Failed to run diagnostics: {e}");
        }
    }
}
