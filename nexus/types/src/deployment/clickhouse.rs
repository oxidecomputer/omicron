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
use omicron_common::address::{
    CLICKHOUSE_HTTP_PORT, CLICKHOUSE_INTERSERVER_HTTP_PORT,
    CLICKHOUSE_KEEPER_PORT, CLICKHOUSE_KEEPER_RAFT_PORT, CLICKHOUSE_TCP_PORT,
};
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::BTreeMap;
use uuid::Uuid;

const BASE_DIR: &str = "/opt/oxide/clickhouse";

/// Clickhouse keeper clusters only allow adding one node at a time
///
/// The planner must progress through these states when adding a new keeper
/// node.
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub enum AddKeeperState {
    /// The executor should attempt to start the new keeper via `clickhouse-admin`
    /// in the target zone.
    ///
    /// The generated keeper config for this node must include all the other
    /// nodes, but those nodes should net yet learn of this config. That is done
    /// in the `Reconfiguring` state.
    StartingKeeper { omicron_zone_id: OmicronZoneUuid, config: KeeperConfig },
    /// The executor should attempt a reconfiguration by updating the
    /// configurations at all  the other keepers. It must stay in this state
    /// until the reconfiguration either succeeds or fails, which it learns
    /// by polling `clickhouse-admin` in one or more zones.
    ///
    /// If the keeper addition succeeds then a transition to a
    /// [`StableKeeperConfig`] should be made. At this point, the configuration
    /// for the clickhouse servers should also be updated to point to the new
    /// keeper.
    Reconfiguring {
        new_node_omicron_zone_id: OmicronZoneUuid,
        keepers: BTreeMap<OmicronZoneUuid, KeeperConfig>,
    },
    /// In some cases, reconfiguration of the keeper can fail. In this case,
    /// the existing zone must be expunged. Once the zone is expunged, then the
    /// planner should go ahead and try to add a new keeper zone again.
    Failed { stable_config: BTreeMap<OmicronZoneUuid, KeeperConfig> },
}

/// The current configuration state of the keeper cluster
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub enum KeeperClusterState {
    /// A configuration of a keeper cluster with no ongoing reconfigurations (node
    /// additions or removals)
    Stable { keepers: BTreeMap<OmicronZoneUuid, KeeperConfig> },
    /// We're currently adding a node to the keeper cluster
    AddingNode {
        prior_stable_config: BTreeMap<OmicronZoneUuid, KeeperConfig>,
        add_node_state: AddKeeperState,
    },
    // TODO: `RemovingNode`
}

/// The current configuration of the keeper cluster
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct KeeperClusterConfig {
    max_used_keeper_id: u64,
    state: KeeperClusterState,
}

/// The current configuration of all clickhouse server replicas
///
/// In contrast to keepers, servers do not require a multi-step reconfiguration
/// to add, and multiple servers can be added or removed simultaneously. Removal
/// is slightly more complex in that we need to ensure that the servers are
/// shutdown and shutdown is noticed before we [drop](https://clickhouse.com/
/// docs/en/sql-reference/statements/system#drop-replica) the replica from the
/// cluster.
///
/// TODO: Model server removal
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct ClickhouseServerClusterConfig {
    max_used_server_id: u64,
    servers: BTreeMap<OmicronZoneUuid, ReplicaConfig>,
}

/// Global configuration for all clickhouse servers (replicas) and keepers
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct ClickhouseClusterConfig {
    // The last update to the clickhouse cluster configuration
    // This is used by clickhouse server and keeper zones to discard
    // configurations they are up to date with.
    generation: Generation,
    cluster_name: String,
    secret: String,
    servers: ClickhouseServerClusterConfig,
    keepers: KeeperClusterConfig,
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
        let mut max_used_keeper_id = 0;
        for kz in keeper_zones {
            max_used_keeper_id += 1;
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
                server_id: KeeperId(max_used_keeper_id),
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

        let keepers = KeeperClusterConfig {
            max_used_keeper_id,
            state: KeeperClusterState::Stable { keepers },
        };

        let servers =
            ClickhouseServerClusterConfig { max_used_server_id, servers };

        ClickhouseClusterConfig {
            generation: Generation::new(),
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
    use omicron_common::address::CRUCIBLE_PORT;
    use omicron_common::api::external::Generation;
    use std::collections::BTreeSet;
    use std::net::{Ipv6Addr, SocketAddrV6};

    /// Generate a bunch of blueprint zones for an initial blueprint
    /// This acts like RSS and therefore all zones should be `InService`.
    pub fn gen_initial_blueprint_zones(
    ) -> BTreeMap<SledUuid, BlueprintZonesConfig> {
        (0..3u64)
            .map(|i| {
                let mut zones = vec![];

                // The keeper zone is always the first one, for testing simplicity
                let keeper_underlay_address =
                    Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, i as u16 + 1);
                let sled_id = SledUuid::new_v4();
                let keeper_zone = BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: OmicronZoneUuid::new_v4(),
                    underlay_address: keeper_underlay_address,
                    filesystem_pool: None,
                    zone_type: BlueprintZoneType::ClickhouseKeeper(
                        blueprint_zone_type::ClickhouseKeeper {
                            address: SocketAddrV6::new(
                                keeper_underlay_address,
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
                // Each sled gets a keeper zone
                zones.push(keeper_zone);

                // The clickhouse server zone is always the second one for
                // testing simplicity.
                //
                // Only 2 sleds get clickhouse server zones.
                if i <= 1 {
                    let server_underlay_address =
                        Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, i as u16 + 10);
                    let server_zone = BlueprintZoneConfig {
                        disposition: BlueprintZoneDisposition::InService,
                        id: OmicronZoneUuid::new_v4(),
                        underlay_address: server_underlay_address,
                        filesystem_pool: None,
                        zone_type: BlueprintZoneType::ClickhouseServer(
                            blueprint_zone_type::ClickhouseServer {
                                address: SocketAddrV6::new(
                                    server_underlay_address,
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

                // Throw in a Crucible for good measure
                //
                // Crucible zones should be ignored by the clickhouse config
                // generation code.
                let crucible_underlay_address =
                    Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, i as u16 + 20);
                let crucible_zone = BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: OmicronZoneUuid::new_v4(),
                    underlay_address: crucible_underlay_address,
                    filesystem_pool: None,
                    zone_type: BlueprintZoneType::Crucible(
                        blueprint_zone_type::Crucible {
                            address: SocketAddrV6::new(
                                crucible_underlay_address,
                                CRUCIBLE_PORT,
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
                zones.push(crucible_zone);

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
        let all_blueprint_zones = gen_initial_blueprint_zones();
        let config = ClickhouseClusterConfig::new(
            cluster_name.clone(),
            &all_blueprint_zones,
        );

        assert_eq!(config.generation, Generation::new());
        assert_eq!(config.servers.max_used_server_id, 2);
        assert_eq!(config.keepers.max_used_keeper_id, 3);
        assert_eq!(config.cluster_name, cluster_name);

        println!("{:#?}", config);

        // We know we are in a stable configuration, since this is the initial config
        let KeeperClusterConfig {
            max_used_keeper_id,
            state: KeeperClusterState::Stable { keepers },
        } = config.keepers
        else {
            panic!("Not in stable keeper state");
        };

        let ClickhouseServerClusterConfig { max_used_server_id, servers } =
            config.servers;

        // Ensure we have 3 valid keeper configs
        for zones_config in all_blueprint_zones.values() {
            // Keeper zones are always first
            let keeper_bp_zone_config = zones_config.zones.first().unwrap();
            let keeper_config = keepers.get(&keeper_bp_zone_config.id).unwrap();
            assert!(keeper_bp_zone_config.zone_type.is_clickhouse_keeper());
            assert_eq!(
                keeper_bp_zone_config.underlay_address.to_string(),
                keeper_config.listen_host
            );
            assert_eq!(keeper_config.raft_config.servers.len(), 3);
        }

        // Ensure that we have 2 valid clickhouse server configs
        let mut found_server_zones = 0;
        for zones_config in all_blueprint_zones.values() {
            let Some(server_bp_zone_config) = zones_config
                .zones
                .iter()
                .find(|z| z.zone_type.is_clickhouse_server())
            else {
                // We only have clickhouse server zones on 2 out of 3 sleds.
                continue;
            };
            found_server_zones += 1;

            let server_config = servers.get(&server_bp_zone_config.id).unwrap();
            assert!(server_bp_zone_config.zone_type.is_clickhouse_server());
            assert_eq!(
                server_bp_zone_config.underlay_address.to_string(),
                server_config.listen_host
            );
            assert_eq!(server_config.remote_servers.replicas.len(), 2);
            assert_eq!(server_config.keepers.nodes.len(), 3);
        }

        // Ensure we generated configurations for both clickhouse server zones
        assert_eq!(found_server_zones, 2);

        // All keepers and servers should have unique IDs
        let expected_keeper_ids: BTreeSet<_> =
            [1, 2, 3].into_iter().map(KeeperId).collect();
        let expected_server_ids: BTreeSet<_> =
            [1, 2].into_iter().map(ServerId).collect();

        let keeper_ids: BTreeSet<_> =
            keepers.values().map(|c| c.server_id).collect();
        let server_ids: BTreeSet<_> =
            servers.values().map(|c| c.macros.replica).collect();

        assert_eq!(expected_keeper_ids, keeper_ids);
        assert_eq!(expected_server_ids, server_ids);

        // All servers should have the same `remote_servers` and `keepers`
        // configurations
        let mut checker = None;
        for replica_config in servers.values() {
            match checker {
                None => checker = Some(replica_config),
                Some(checker) => {
                    assert_eq!(
                        checker.remote_servers,
                        replica_config.remote_servers
                    );
                    assert_eq!(checker.keepers, replica_config.keepers);
                }
            }
        }

        // All Keepers should have the same `raft_config`
        let mut checker = None;
        for keeper_config in keepers.values() {
            match checker {
                None => {
                    checker = Some(keeper_config);
                }
                Some(checker) => {
                    assert_eq!(checker.raft_config, keeper_config.raft_config);
                }
            }
        }
    }
}
