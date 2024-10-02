// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Deployment of Clickhouse keeper and server nodes via clickhouse-admin running in
//! deployed clickhouse zones.

use anyhow::anyhow;
use anyhow::Context;
use camino::Utf8PathBuf;
use clickhouse_admin_api::KeeperConfigurableSettings;
use clickhouse_admin_api::ServerConfigurableSettings;
use clickhouse_admin_client::Client;
use clickhouse_admin_types::config::ClickhouseHost;
use clickhouse_admin_types::config::RaftServerSettings;
use clickhouse_admin_types::KeeperId;
use clickhouse_admin_types::KeeperSettings;
use clickhouse_admin_types::ServerSettings;
use nexus_db_queries::context::OpContext;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::deployment::ClickhouseClusterConfig;
use omicron_common::address::CLICKHOUSE_ADMIN_PORT;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use std::str::FromStr;

const CLICKHOUSE_SERVER_CONFIG_DIR: &str =
    "/opt/oxide/clickhouse_server/config.d";
const CLICKHOUSE_KEEPER_CONFIG_DIR: &str =
    "/opt/oxide/clickhouse_keeper/config.d";
const CLICKHOUSE_DATA_DIR: &str = "/data";

pub(crate) async fn deploy_nodes(
    opctx: &OpContext,
    zones: &BTreeMap<SledUuid, BlueprintZonesConfig>,
    clickhouse_cluster_config: &ClickhouseClusterConfig,
) -> Result<(), Vec<anyhow::Error>> {
    let keeper_configs = match keeper_configs(zones, clickhouse_cluster_config)
    {
        Ok(keeper_configs) => keeper_configs,
        Err(e) => return Err(vec![e]),
    };

    let keeper_hosts: Vec<_> = keeper_configs
        .iter()
        .map(|s| ClickhouseHost::Ipv6(s.settings.listen_addr))
        .collect();

    let server_configs =
        match server_configs(zones, clickhouse_cluster_config, keeper_hosts) {
            Ok(server_configs) => server_configs,
            Err(e) => return Err(vec![e]),
        };

    // Inform each clickhouse keeper about its configuration
    for keeper in keeper_configs {}

    // Inform each clickhouse server about its configuration
    for server in server_configs {}

    todo!()
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
