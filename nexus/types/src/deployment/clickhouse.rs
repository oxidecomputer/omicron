// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used in blueprints related to clickhouse configuration

use crate::deployment::{
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZonesConfig,
};
use camino::Utf8PathBuf;
use clickward::config::{
    KeeperConfig, KeeperConfigsForReplica, KeeperCoordinationSettings,
    LogConfig, LogLevel, Macros, RaftServerConfig, RaftServers, RemoteServers,
    ReplicaConfig, ServerConfig,
};
use clickward::{KeeperId, ServerId};
use omicron_common::address::CLICKHOUSE_INTERSERVER_HTTP_PORT;
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use omicron_common::address::{
    CLICKHOUSE_HTTP_PORT, CLICKHOUSE_KEEPER_PORT, CLICKHOUSE_KEEPER_RAFT_PORT,
};
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::BTreeMap;
use uuid::Uuid;

const BASE_DIR: &str = "/opt/oxide/clickhouse";

/// Global configuration for all clickhouse servers (replicas) and keepers
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct ClickhouseClusterConfig {
    // The last update to the clickhouse cluster configuration
    // This is used by clickhouse server and keeper zones to discard
    // configurations they are up to date with.
    generation: u64,
    max_used_server_id: u64,
    max_used_keeper_id: u64,
    cluster_name: String,
    secret: String,
    servers: BTreeMap<OmicronZoneUuid, ReplicaConfig>,
    keepers: BTreeMap<OmicronZoneUuid, KeeperConfig>,
}

impl ClickhouseClusterConfig {
    ///  Create an intitial deployment for the first blueprint
    pub fn new(
        cluster_name: String,
        all_blueprint_zones: &BTreeMap<SledUuid, BlueprintZonesConfig>,
    ) -> ClickhouseClusterConfig {
        let base_dir = Utf8PathBuf::from(BASE_DIR);
        let logs_dir = base_dir.join("logs");

        // Get all `InService` clickhouse zones
        let (server_zones, keeper_zones) =
            all_in_service_clickhouse_zones(all_blueprint_zones);

        // All servers must point to all keepers (via their client ports), so
        // first we build up our set of `ServerConfig`s for keepers.
        let keeper_configs_for_replica: Vec<_> = keeper_zones
            .iter()
            .map(|z| ServerConfig {
                host: z.underlay_address.to_string(),
                port: CLICKHOUSE_KEEPER_PORT,
            })
            .collect();

        // All servers must also point to each other, so we also build up this
        // set
        let server_configs_for_replica: Vec<_> = server_zones
            .iter()
            .map(|z| ServerConfig {
                host: z.underlay_address.to_string(),
                port: CLICKHOUSE_TCP_PORT,
            })
            .collect();

        // All keepers must point to each other via their ids and raft ports
        let raft_servers: Vec<_> = keeper_zones
            .iter()
            .enumerate()
            .map(|(i, z)| RaftServerConfig {
                id: KeeperId(i as u64 + 1),
                hostname: z.underlay_address.to_string(),
                port: CLICKHOUSE_KEEPER_RAFT_PORT,
            })
            .collect();

        // A unique ID that ties all servers together
        let secret = Uuid::new_v4().to_string();

        // Create configurations for all servers
        //
        // All servers are new so start numbering the ids from `1`.
        let mut servers = BTreeMap::new();
        let mut max_used_server_id = 0;
        for sz in server_zones {
            max_used_server_id += 1;
            let replica_config = ReplicaConfig {
                logger: LogConfig {
                    level: LogLevel::Trace,
                    log: logs_dir.join("clickhouse.log"),
                    errorlog: logs_dir.join("clickhouse.err.log"),
                    size: "100M".to_string(),
                    count: 1,
                },
                macros: Macros {
                    // No sharding
                    shard: 1,
                    replica: ServerId(max_used_server_id),
                    cluster: cluster_name.clone(),
                },
                listen_host: sz.underlay_address.to_string(),
                http_port: CLICKHOUSE_HTTP_PORT,
                tcp_port: CLICKHOUSE_TCP_PORT,
                interserver_http_port: CLICKHOUSE_INTERSERVER_HTTP_PORT,
                remote_servers: RemoteServers {
                    cluster: cluster_name.clone(),
                    secret: secret.clone(),
                    replicas: server_configs_for_replica.clone(),
                },
                keepers: KeeperConfigsForReplica {
                    nodes: keeper_configs_for_replica.clone(),
                },
                data_path: base_dir.join("data"),
            };
            servers.insert(sz.id, replica_config);
        }

        // Create configurations for all keepers
        //
        // All keepers are new so start numbering the ids from 1
        let mut keepers = BTreeMap::new();
        let max_used_keeper_id = raft_servers.len() as u64;
        for kz in keeper_zones {
            let keeper_config = KeeperConfig {
                logger: LogConfig {
                    level: LogLevel::Trace,
                    log: logs_dir.join("clickhouse-keeper.log"),
                    errorlog: logs_dir.join("clickhouse-keeper.err.log"),
                    size: "100M".to_string(),
                    count: 1,
                },
                listen_host: kz.underlay_address.to_string(),
                tcp_port: CLICKHOUSE_KEEPER_PORT,
                server_id: KeeperId(max_used_server_id),
                log_storage_path: base_dir.join("coordination").join("log"),
                snapshot_storage_path: base_dir
                    .join("coordination")
                    .join("snapshots"),
                coordination_settings: KeeperCoordinationSettings {
                    operation_timeout_ms: 10000,
                    session_timeout_ms: 30000,
                    raft_logs_level: LogLevel::Trace,
                },
                raft_config: RaftServers { servers: raft_servers.clone() },
            };
            keepers.insert(kz.id, keeper_config);
        }

        ClickhouseClusterConfig {
            generation: 1,
            max_used_server_id,
            max_used_keeper_id,
            cluster_name,
            secret,
            servers,
            keepers,
        }
    }

    /// Create a deployment dependent on the configuration from the parent
    /// blueprint
    pub fn new_based_on<'a>(
        log: &Logger,
        parent_config: &'a ClickhouseClusterConfig,
        all_blueprint_zones: &BTreeMap<SledUuid, BlueprintZonesConfig>,
    ) -> ClickhouseClusterConfig {
        todo!()
    }
}

type ServerZone = BlueprintZoneConfig;
type KeeperZone = BlueprintZoneConfig;

/// Find and return all replicated clickhouse zones that should be running
fn all_in_service_clickhouse_zones(
    all_blueprint_zones: &BTreeMap<SledUuid, BlueprintZonesConfig>,
) -> (Vec<ServerZone>, Vec<KeeperZone>) {
    let mut server_zones = vec![];
    let mut keeper_zones = vec![];
    for (_sled_id, config) in all_blueprint_zones {
        for zone in &config.zones {
            if zone.disposition == BlueprintZoneDisposition::InService {
                if zone.zone_type.is_clickhouse_server() {
                    server_zones.push(zone.clone());
                } else if zone.zone_type.is_clickhouse_keeper() {
                    keeper_zones.push(zone.clone())
                }
            }
        }
    }

    (server_zones, keeper_zones)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::blueprint_zone_type;
    use crate::deployment::BlueprintZoneType;
    use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
    use omicron_common::api::external::Generation;
    use std::collections::BTreeSet;
    use std::net::{Ipv6Addr, SocketAddrV6};

    /// Create a few in service `ClickhouseKeeper` and `ClickhouseServer` zones.
    pub fn gen_in_service_clickhouse_zones(
    ) -> BTreeMap<SledUuid, BlueprintZonesConfig> {
        (0..3u64)
            .map(|i| {
                let mut zones = vec![];

                let sled_id = SledUuid::new_v4();
                let keeper_zone = BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: OmicronZoneUuid::new_v4(),
                    underlay_address: Ipv6Addr::new(
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        i as u16 + 1,
                    ),
                    filesystem_pool: None,
                    zone_type: BlueprintZoneType::ClickhouseKeeper(
                        blueprint_zone_type::ClickhouseKeeper {
                            address: SocketAddrV6::new(
                                Ipv6Addr::new(
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    i as u16 + 1,
                                ),
                                CLICKHOUSE_KEEPER_PORT,
                                0,
                                0,
                            ),
                            dataset: OmicronZoneDataset {
                                pool_name: format!("oxp_{}", Uuid::new_v4())
                                    .parse()
                                    .expect("bad name"),
                            },
                        },
                    ),
                };
                // Each sled getgs a keeper zone
                zones.push(keeper_zone);

                // Only 2 sleds get clickhouse server zones
                if i <= 1 {
                    let server_zone = BlueprintZoneConfig {
                        disposition: BlueprintZoneDisposition::InService,
                        id: OmicronZoneUuid::new_v4(),
                        underlay_address: Ipv6Addr::new(
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            i as u16 + 10,
                        ),
                        filesystem_pool: None,
                        zone_type: BlueprintZoneType::ClickhouseServer(
                            blueprint_zone_type::ClickhouseServer {
                                address: SocketAddrV6::new(
                                    Ipv6Addr::new(
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        i as u16 + 10,
                                    ),
                                    CLICKHOUSE_HTTP_PORT,
                                    0,
                                    0,
                                ),
                                dataset: OmicronZoneDataset {
                                    pool_name: format!(
                                        "oxp_{}",
                                        Uuid::new_v4()
                                    )
                                    .parse()
                                    .expect("bad name"),
                                },
                            },
                        ),
                    };
                    zones.push(server_zone);
                }

                (
                    sled_id,
                    BlueprintZonesConfig {
                        generation: Generation::new(),
                        zones,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn test_new_clickhouse_cluster_config() {
        let cluster_name = "test-cluster".to_string();
        let all_blueprint_zones = gen_in_service_clickhouse_zones();
        let config = ClickhouseClusterConfig::new(
            cluster_name.clone(),
            &all_blueprint_zones,
        );

        assert_eq!(config.generation, 1);
        assert_eq!(config.max_used_server_id, 2);
        assert_eq!(config.max_used_keeper_id, 3);
        assert_eq!(config.cluster_name, cluster_name);

        println!("{:#?}", config);

        // Ensure we have 3 valid keeper configs
        for zones_config in all_blueprint_zones.values() {
            let keeper_bp_zone_config = zones_config.zones.first().unwrap();
            let keeper_config =
                config.keepers.get(&keeper_bp_zone_config.id).unwrap();
            assert!(keeper_bp_zone_config.zone_type.is_clickhouse_keeper());
            assert_eq!(
                keeper_bp_zone_config.underlay_address.to_string(),
                keeper_config.listen_host
            );
            assert_eq!(keeper_config.raft_config.servers.len(), 3);
        }

        // Ensure that we have 2 valid clickhouse server configs
        for zones_config in all_blueprint_zones.values() {
            let Some(server_bp_zone_config) = zones_config.zones.get(1) else {
                // We only have 2 clickhouse server configs
                continue;
            };
            let server_config =
                config.servers.get(&server_bp_zone_config.id).unwrap();
            assert!(server_bp_zone_config.zone_type.is_clickhouse_server());
            assert_eq!(
                server_bp_zone_config.underlay_address.to_string(),
                server_config.listen_host
            );
            assert_eq!(server_config.remote_servers.replicas.len(), 2);
            assert_eq!(server_config.keepers.nodes.len(), 3);
        }

        // TODO: Verify some properties about the configs such as distinct ids, etc..
    }
}
