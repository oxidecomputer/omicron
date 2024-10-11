// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Deployment of Clickhouse keeper and server nodes via clickhouse-admin running in
//! deployed clickhouse zones.

use anyhow::anyhow;
use camino::Utf8PathBuf;
use clickhouse_admin_keeper_client::Client as ClickhouseKeeperClient;
use clickhouse_admin_server_client::Client as ClickhouseServerClient;
use clickhouse_admin_types::ClickhouseHost;
use clickhouse_admin_types::KeeperConfigurableSettings;
use clickhouse_admin_types::KeeperSettings;
use clickhouse_admin_types::RaftServerSettings;
use clickhouse_admin_types::ServerConfigurableSettings;
use clickhouse_admin_types::ServerSettings;
use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use nexus_db_queries::context::OpContext;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::deployment::ClickhouseClusterConfig;
use omicron_common::address::CLICKHOUSE_ADMIN_PORT;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use slog::error;
use slog::info;
use slog::warn;
use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::str::FromStr;

const CLICKHOUSE_SERVER_CONFIG_DIR: &str =
    "/opt/oxide/clickhouse_server/config.d";
const CLICKHOUSE_KEEPER_CONFIG_DIR: &str = "/opt/oxide/clickhouse_keeper";
const CLICKHOUSE_DATA_DIR: &str = "/data";

pub(crate) async fn deploy_nodes(
    opctx: &OpContext,
    zones: &BTreeMap<SledUuid, BlueprintZonesConfig>,
    clickhouse_cluster_config: &ClickhouseClusterConfig,
) -> Result<(), Vec<anyhow::Error>> {
    let keeper_configs = match keeper_configs(zones, clickhouse_cluster_config)
    {
        Ok(keeper_configs) => keeper_configs,
        Err(e) => {
            // We can't proceed if we fail to generate configs.
            // Let's be noisy about it.
            error!(
                opctx.log,
                "failed to generate clickhouse keeper configs: {e}"
            );
            return Err(vec![e]);
        }
    };

    let keeper_hosts: Vec<_> = keeper_configs
        .iter()
        .map(|s| ClickhouseHost::Ipv6(s.settings.listen_addr))
        .collect();

    let server_configs =
        match server_configs(zones, clickhouse_cluster_config, keeper_hosts) {
            Ok(server_configs) => server_configs,
            Err(e) => {
                // We can't proceed if we fail to generate configs.
                // Let's be noisy about it.
                error!(
                    opctx.log,
                    "Failed to generate clickhouse server configs: {e}"
                );
                return Err(vec![e]);
            }
        };

    let mut errors = vec![];
    let log = opctx.log.clone();

    // Inform each clickhouse-admin server in a keeper zone or server zone about
    // its node's configuration
    let mut futs = FuturesUnordered::new();
    for config in keeper_configs {
        let admin_addr = SocketAddr::V6(SocketAddrV6::new(
            config.settings.listen_addr,
            CLICKHOUSE_ADMIN_PORT,
            0,
            0,
        ));
        let admin_url = format!("http://{admin_addr}");
        let log = log.new(slog::o!("admin_url" => admin_url.clone()));
        futs.push(Either::Left(async move {
            let client = ClickhouseKeeperClient::new(&admin_url, log.clone());
            client.generate_config(&config).await.map(|_| ()).map_err(|e| {
                anyhow!(
                    concat!(
                        "failed to send config for clickhouse keeper ",
                        "with id {} to clickhouse-admin-keeper; admin_url = {}",
                        "error = {}"
                    ),
                    config.settings.id,
                    admin_url,
                    e
                )
            })
        }));
    }
    for config in server_configs {
        let admin_addr = SocketAddr::V6(SocketAddrV6::new(
            config.settings.listen_addr,
            CLICKHOUSE_ADMIN_PORT,
            0,
            0,
        ));
        let admin_url = format!("http://{admin_addr}");
        let log = opctx.log.new(slog::o!("admin_url" => admin_url.clone()));
        futs.push(Either::Right(async move {
            let client = ClickhouseServerClient::new(&admin_url, log.clone());
            client.generate_config(&config).await.map(|_| ()).map_err(|e| {
                anyhow!(
                    concat!(
                        "failed to send config for clickhouse server ",
                        "with id {} to clickhouse-admin-server; admin_url = {}",
                        "error = {}"
                    ),
                    config.settings.id,
                    admin_url,
                    e
                )
            })
        }));
    }

    while let Some(res) = futs.next().await {
        if let Err(e) = res {
            warn!(log, "{e}");
            errors.push(e);
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    info!(
        opctx.log,
        "Successfully deployed all clickhouse server and keeper configs"
    );

    Ok(())
}

fn server_configs(
    zones: &BTreeMap<SledUuid, BlueprintZonesConfig>,
    clickhouse_cluster_config: &ClickhouseClusterConfig,
    keepers: Vec<ClickhouseHost>,
) -> Result<Vec<ServerConfigurableSettings>, anyhow::Error> {
    let server_ips: BTreeMap<OmicronZoneUuid, Ipv6Addr> = zones
        .values()
        .flat_map(|zones_config| {
            zones_config
                .zones
                .iter()
                .filter(|zone_config| {
                    clickhouse_cluster_config
                        .servers
                        .contains_key(&zone_config.id)
                })
                .map(|zone_config| {
                    (zone_config.id, zone_config.underlay_address)
                })
        })
        .collect();

    let mut remote_servers =
        Vec::with_capacity(clickhouse_cluster_config.servers.len());

    for (zone_id, server_id) in &clickhouse_cluster_config.servers {
        remote_servers.push(ClickhouseHost::Ipv6(
            *server_ips.get(zone_id).ok_or_else(|| {
                anyhow!(
                    "Failed to retrieve zone {} for server id {}",
                    zone_id,
                    server_id
                )
            })?,
        ));
    }

    let mut server_configs =
        Vec::with_capacity(clickhouse_cluster_config.servers.len());

    for (zone_id, server_id) in &clickhouse_cluster_config.servers {
        server_configs.push(ServerConfigurableSettings {
            generation: clickhouse_cluster_config.generation,
            settings: ServerSettings {
                config_dir: Utf8PathBuf::from_str(CLICKHOUSE_SERVER_CONFIG_DIR)
                    .unwrap(),
                id: *server_id,
                datastore_path: Utf8PathBuf::from_str(CLICKHOUSE_DATA_DIR)
                    .unwrap(),
                // SAFETY: We already successfully performed the same lookup to compute
                // `remote_servers` above.
                listen_addr: *server_ips.get(zone_id).unwrap(),
                keepers: keepers.clone(),
                remote_servers: remote_servers.clone(),
            },
        });
    }

    Ok(server_configs)
}

fn keeper_configs(
    zones: &BTreeMap<SledUuid, BlueprintZonesConfig>,
    clickhouse_cluster_config: &ClickhouseClusterConfig,
) -> Result<Vec<KeeperConfigurableSettings>, anyhow::Error> {
    let keeper_ips: BTreeMap<OmicronZoneUuid, Ipv6Addr> = zones
        .values()
        .flat_map(|zones_config| {
            zones_config
                .zones
                .iter()
                .filter(|zone_config| {
                    clickhouse_cluster_config
                        .keepers
                        .contains_key(&zone_config.id)
                })
                .map(|zone_config| {
                    (zone_config.id, zone_config.underlay_address)
                })
        })
        .collect();

    let mut raft_servers =
        Vec::with_capacity(clickhouse_cluster_config.keepers.len());

    for (zone_id, keeper_id) in &clickhouse_cluster_config.keepers {
        raft_servers.push(RaftServerSettings {
            id: *keeper_id,
            host: ClickhouseHost::Ipv6(*keeper_ips.get(zone_id).ok_or_else(
                || {
                    anyhow!(
                        "Failed to retrieve zone {} for keeper id {}",
                        zone_id,
                        keeper_id
                    )
                },
            )?),
        });
    }

    let mut keeper_configs =
        Vec::with_capacity(clickhouse_cluster_config.keepers.len());

    for (zone_id, keeper_id) in &clickhouse_cluster_config.keepers {
        keeper_configs.push(KeeperConfigurableSettings {
            generation: clickhouse_cluster_config.generation,
            settings: KeeperSettings {
                config_dir: Utf8PathBuf::from_str(CLICKHOUSE_KEEPER_CONFIG_DIR)
                    .unwrap(),
                id: *keeper_id,
                raft_servers: raft_servers.clone(),
                datastore_path: Utf8PathBuf::from_str(CLICKHOUSE_DATA_DIR)
                    .unwrap(),
                // SAFETY: We already successfully performed the same lookup to compute
                // `raft_servers` above.
                listen_addr: *keeper_ips.get(zone_id).unwrap(),
            },
        });
    }

    Ok(keeper_configs)
}

#[cfg(test)]
mod test {
    use super::*;
    use clickhouse_admin_types::ClickhouseHost;
    use clickhouse_admin_types::KeeperId;
    use clickhouse_admin_types::ServerId;
    use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::inventory::ZpoolName;
    use omicron_common::api::external::Generation;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::collections::BTreeSet;

    fn test_data(
    ) -> (BTreeMap<SledUuid, BlueprintZonesConfig>, ClickhouseClusterConfig)
    {
        let num_keepers = 3u64;
        let num_servers = 2u64;

        let mut zones = BTreeMap::new();
        let mut config = ClickhouseClusterConfig::new("test".to_string());

        for keeper_id in 1..=num_keepers {
            let sled_id = SledUuid::new_v4();
            let zone_id = OmicronZoneUuid::new_v4();
            let zone_config = BlueprintZonesConfig {
                generation: Generation::new(),
                zones: vec![BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: zone_id,
                    underlay_address: Ipv6Addr::new(
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        keeper_id as u16,
                    ),
                    filesystem_pool: None,
                    zone_type: BlueprintZoneType::ClickhouseKeeper(
                        blueprint_zone_type::ClickhouseKeeper {
                            address: SocketAddrV6::new(
                                Ipv6Addr::LOCALHOST,
                                0,
                                0,
                                0,
                            ),
                            dataset: OmicronZoneDataset {
                                pool_name: ZpoolName::new_external(
                                    ZpoolUuid::new_v4(),
                                ),
                            },
                        },
                    ),
                }],
            };
            zones.insert(sled_id, zone_config);
            config.keepers.insert(zone_id, keeper_id.into());
        }

        for server_id in 1..=num_servers {
            let sled_id = SledUuid::new_v4();
            let zone_id = OmicronZoneUuid::new_v4();
            let zone_config = BlueprintZonesConfig {
                generation: Generation::new(),
                zones: vec![BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: zone_id,
                    underlay_address: Ipv6Addr::new(
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        server_id as u16 + 10,
                    ),
                    filesystem_pool: None,
                    zone_type: BlueprintZoneType::ClickhouseServer(
                        blueprint_zone_type::ClickhouseServer {
                            address: SocketAddrV6::new(
                                Ipv6Addr::LOCALHOST,
                                0,
                                0,
                                0,
                            ),
                            dataset: OmicronZoneDataset {
                                pool_name: ZpoolName::new_external(
                                    ZpoolUuid::new_v4(),
                                ),
                            },
                        },
                    ),
                }],
            };
            zones.insert(sled_id, zone_config);
            config.servers.insert(zone_id, server_id.into());
        }

        (zones, config)
    }

    #[test]
    fn test_generate_config_settings() {
        let (zones, clickhouse_cluster_config) = test_data();

        // Generate our keeper settings to send to keepers
        let keeper_settings =
            keeper_configs(&zones, &clickhouse_cluster_config)
                .expect("generated keeper settings");

        // Are the keeper settings what we expect
        assert_eq!(keeper_settings.len(), 3);
        let expected_keeper_ids: BTreeSet<_> =
            [1u64, 2, 3].into_iter().map(KeeperId::from).collect();
        let mut keeper_ids = BTreeSet::new();
        let mut keeper_ips_last_octet_as_keeper_id = BTreeSet::new();
        for k in &keeper_settings {
            assert_eq!(k.settings.raft_servers.len(), 3);
            for rs in &k.settings.raft_servers {
                keeper_ids.insert(rs.id);
                let ClickhouseHost::Ipv6(ip) = rs.host else {
                    panic!("bad host");
                };
                keeper_ips_last_octet_as_keeper_id
                    .insert(KeeperId(u64::from(*ip.octets().last().unwrap())));
            }
        }
        assert_eq!(keeper_ids, expected_keeper_ids);
        assert_eq!(keeper_ids, keeper_ips_last_octet_as_keeper_id);

        let keeper_hosts: Vec<_> = keeper_settings
            .iter()
            .map(|s| ClickhouseHost::Ipv6(s.settings.listen_addr))
            .collect();

        // Generate our server settings to send to clickhouse servers
        let server_settings =
            server_configs(&zones, &clickhouse_cluster_config, keeper_hosts)
                .expect("generated server settings");

        // Are our server settings what we expect
        assert_eq!(server_settings.len(), 2);
        let expected_server_ids: BTreeSet<_> =
            [1u64, 2].into_iter().map(ServerId::from).collect();
        let mut server_ids = BTreeSet::new();
        let mut server_ips_last_octet = BTreeSet::new();
        let expected_server_ips_last_octet: BTreeSet<u8> =
            [11u8, 12].into_iter().collect();
        for s in server_settings {
            assert_eq!(s.settings.keepers.len(), 3);
            assert_eq!(s.settings.remote_servers.len(), 2);
            server_ids.insert(s.settings.id);

            server_ips_last_octet
                .insert(*s.settings.listen_addr.octets().last().unwrap());

            // Are all our keeper ips correct?
            let mut keeper_ips_last_octet_as_keeper_id = BTreeSet::new();
            for host in &s.settings.keepers {
                let ClickhouseHost::Ipv6(ip) = host else {
                    panic!("bad host");
                };
                keeper_ips_last_octet_as_keeper_id
                    .insert(KeeperId(u64::from(*ip.octets().last().unwrap())));
            }
            assert_eq!(keeper_ips_last_octet_as_keeper_id, expected_keeper_ids);

            // Are all our remote server ips correct?
            let mut remote_server_last_octets = BTreeSet::new();
            for host in &s.settings.remote_servers {
                let ClickhouseHost::Ipv6(ip) = host else {
                    panic!("bad host");
                };
                remote_server_last_octets.insert(*ip.octets().last().unwrap());
            }
            assert_eq!(
                remote_server_last_octets,
                expected_server_ips_last_octet
            );
        }
        // Are all our server ids correct
        assert_eq!(server_ids, expected_server_ids);

        // Are all our server listen ips correct?
        assert_eq!(server_ips_last_octet, expected_server_ips_last_octet);
    }
}
