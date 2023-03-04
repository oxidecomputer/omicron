use anyhow::Result;
use end_to_end_tests::helpers::ctx::{build_client, gateway_ip, Context};
use end_to_end_tests::helpers::{generate_name, get_system_ip_pool};
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use oxide_client::types::{
    AddressLotBlockCreate, AddressLotCreate, AddressLotKind, ByteCount,
    DiskCreate, DiskSource, IpRange, Ipv4Range, LoopbackAddressCreate, Name,
    Route, RouteConfig, SwitchPortApplySettings, SwitchPortConfig,
    SwitchPortGeometry, SwitchPortSettingsCreate,
};
use oxide_client::{
    ClientDisksExt, ClientExternalNetworkingExt, ClientProjectsExt,
    ClientSystemExt,
};
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let client = build_client()?;

    // ===== ENSURE NEXUS IS UP ===== //
    eprintln!("waiting for nexus to come up...");
    wait_for_condition(
        || async {
            client
                .project_list()
                .send()
                .await
                .map_err(|_| CondCheckError::<oxide_client::Error>::NotYet)
        },
        &Duration::from_secs(1),
        &Duration::from_secs(300),
    )
    .await?;

    let (first, last) = get_system_ip_pool()?;

    // ===== EXTERNAL NETWORKING ===== //
    eprintln!("configuring external networking");

    eprintln!("creating an address lot...");
    let address_lot_name: Name = "parkinglot".parse().unwrap();
    client
        .networking_address_lot_create()
        .body(AddressLotCreate {
            name: address_lot_name.clone(),
            description: "an address parking lot".into(),
            kind: AddressLotKind::Infra,
            blocks: vec![
                AddressLotBlockCreate {
                    first_address: first.into(),
                    last_address: last.into(),
                },
                AddressLotBlockCreate {
                    first_address: "fd00:99::1".parse().unwrap(),
                    last_address: "fd00:99::ffff".parse().unwrap(),
                },
            ],
        })
        .send()
        .await?;

    let rack_id = client.rack_list().send().await?.items[0].id;

    let port_settings_name: Name = "portofino".parse().unwrap();

    // Create port settings
    let mut settings = SwitchPortSettingsCreate {
        name: port_settings_name.clone(),
        description: "just a port".into(),
        port_config: SwitchPortConfig {
            geometry: SwitchPortGeometry::Qsfp28x1,
        },
        addresses: HashMap::new(),
        bgp_peers: HashMap::new(),
        groups: Vec::new(),
        interfaces: HashMap::new(),
        links: HashMap::new(),
        routes: HashMap::new(),
    };
    settings.routes.insert(
        "phy0".into(),
        RouteConfig {
            routes: vec![Route {
                dst: "0.0.0.0/0".parse().unwrap(),
                gw: gateway_ip().parse().unwrap(),
            }],
        },
    );

    eprintln!("creating port settings...");
    client
        .networking_switch_port_settings_create()
        .body(settings)
        .send()
        .await?;

    // Apply port settings
    eprintln!("applying port settings...");
    client
        .networking_switch_port_apply_settings()
        .port("qsfp0")
        .rack_id(rack_id)
        .switch_location("switch0")
        .body(SwitchPortApplySettings {
            port_settings: port_settings_name.into(),
        })
        .send()
        .await?;

    // Loopback address
    eprintln!("creating boundary services loopback...");
    client
        .networking_loopback_address_create()
        .body(LoopbackAddressCreate {
            address_lot: address_lot_name.into(),
            rack_id: rack_id,
            switch_location: "switch0".parse().unwrap(),
            address: "fd00:99::1".parse().unwrap(),
            mask: 64,
        })
        .send()
        .await?;

    // ===== CREATE IP POOL ===== //
    eprintln!("creating IP pool...");
    client
        .ip_pool_range_add()
        .pool("default")
        .body(IpRange::V4(Ipv4Range { first, last }))
        .send()
        .await?;

    // ===== ENSURE DATASETS ARE READY ===== //
    eprintln!("ensuring datasets are ready...");
    let ctx = Context::from_client(client).await?;
    let disk_name = generate_name("disk")?;
    wait_for_condition(
        || async {
            ctx.client
                .disk_create()
                .project(ctx.project_name.clone())
                .body(DiskCreate {
                    name: disk_name.clone(),
                    description: String::new(),
                    disk_source: DiskSource::Blank {
                        block_size: 512.try_into().unwrap(),
                    },
                    size: ByteCount(1024 * 1024 * 1024),
                })
                .send()
                .await
                .map_err(|_| CondCheckError::<oxide_client::Error>::NotYet)
        },
        &Duration::from_secs(1),
        &Duration::from_secs(120),
    )
    .await?;
    ctx.client
        .disk_delete()
        .project(ctx.project_name.clone())
        .disk(disk_name)
        .send()
        .await?;
    ctx.cleanup().await?;

    eprintln!("let's roll.");
    Ok(())
}
