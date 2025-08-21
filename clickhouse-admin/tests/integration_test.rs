// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use clickhouse_admin_test_utils::{
    allocate_available_ports,
    clickhouse_cluster_test_deployment_with_dynamic_ports,
    default_clickhouse_log_ctx_and_path,
};
use clickhouse_admin_types::{
    ClickhouseHost, ClickhouseKeeperClusterMembership, KeeperId,
    KeeperServerInfo, KeeperServerType, RaftConfig,
};
use omicron_clickhouse_admin::ClickhouseCli;
use slog::{Drain, info, o};
use slog_term::{FullFormat, PlainDecorator, TestStdoutWriter};
use std::collections::BTreeSet;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::str::FromStr;

fn log() -> slog::Logger {
    let decorator = PlainDecorator::new(TestStdoutWriter);
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

// In Clickward, keeper server ports are assigned by adding i to each
// base port. Keeper IDs are also assigned with consecutive numbers
// starting with 1.
fn get_keeper_server_port(base_ports: &clickward::BasePorts, keeper_id: KeeperId) -> u16 {
    let raw_id = keeper_id.0;
    // We can safely unwrap raw_id as the Keeper IDs we use for testing are
    // all in the single digits
    base_ports.keeper + u16::try_from(raw_id).unwrap()
}

fn get_keeper_raft_port(base_ports: &clickward::BasePorts, keeper_id: KeeperId) -> u16 {
    let raw_id = keeper_id.0;
    base_ports.raft + u16::try_from(raw_id).unwrap()
}

#[tokio::test]
async fn test_lgif_parsing() -> anyhow::Result<()> {
    // Use dynamic port allocation for test isolation
    let base_ports = allocate_available_ports(Some(25000), Some(10))?;
    
    let clickhouse_cli = ClickhouseCli::new(
        Utf8PathBuf::from_str("clickhouse")?,
        SocketAddrV6::new(
            Ipv6Addr::LOCALHOST,
            get_keeper_server_port(&base_ports, KeeperId(1)),
            0,
            0,
        ),
        &log(),
    );

    let lgif = clickhouse_cli.lgif().await.unwrap();

    // The first log index from a newly created cluster should always be 1
    assert_eq!(lgif.first_log_idx, 1);

    Ok(())
}

#[tokio::test]
async fn test_raft_config_parsing() -> anyhow::Result<()> {
    // Use dynamic port allocation for test isolation
    let base_ports = allocate_available_ports(Some(26000), Some(10))?;
    
    let clickhouse_cli = ClickhouseCli::new(
        Utf8PathBuf::from_str("clickhouse").unwrap(),
        SocketAddrV6::new(
            Ipv6Addr::LOCALHOST,
            get_keeper_server_port(&base_ports, KeeperId(1)),
            0,
            0,
        ),
        &log(),
    );

    let raft_config = clickhouse_cli.raft_config().await.unwrap();

    let mut keeper_servers = BTreeSet::new();
    let num_keepers = 3;

    for i in 1..=num_keepers {
        let raft_port = get_keeper_raft_port(&base_ports, KeeperId(i));
        keeper_servers.insert(KeeperServerInfo {
            server_id: clickhouse_admin_types::KeeperId(i),
            host: ClickhouseHost::Ipv6("::1".parse().unwrap()),
            raft_port,
            server_type: KeeperServerType::Participant,
            priority: 1,
        });
    }

    let expected_raft_config = RaftConfig { keeper_servers };

    assert_eq!(raft_config, expected_raft_config);

    Ok(())
}

#[tokio::test]
async fn test_keeper_conf_parsing() -> anyhow::Result<()> {
    // Use dynamic port allocation for test isolation
    let base_ports = allocate_available_ports(Some(27000), Some(10))?;
    
    let clickhouse_cli = ClickhouseCli::new(
        Utf8PathBuf::from_str("clickhouse").unwrap(),
        SocketAddrV6::new(
            Ipv6Addr::LOCALHOST,
            get_keeper_server_port(&base_ports, KeeperId(1)),
            0,
            0,
        ),
        &log(),
    );

    let conf = clickhouse_cli.keeper_conf().await.unwrap();

    assert_eq!(conf.server_id, KeeperId(1));

    Ok(())
}

#[tokio::test]
async fn test_keeper_cluster_membership() -> anyhow::Result<()> {
    // Use dynamic port allocation for test isolation
    let base_ports = allocate_available_ports(Some(28000), Some(10))?;
    
    let clickhouse_cli = ClickhouseCli::new(
        Utf8PathBuf::from_str("clickhouse").unwrap(),
        SocketAddrV6::new(
            Ipv6Addr::LOCALHOST,
            get_keeper_server_port(&base_ports, KeeperId(1)),
            0,
            0,
        ),
        &log(),
    );

    let keeper_cluster_membership =
        clickhouse_cli.keeper_cluster_membership().await.unwrap();

    let mut raft_config = BTreeSet::new();

    let num_keepers = 3;
    for i in 1..=num_keepers {
        raft_config.insert(KeeperId(i));
    }

    let expected_keeper_cluster_membership =
        ClickhouseKeeperClusterMembership {
            queried_keeper: KeeperId(1),
            // This number is always different so we won't be testing it
            leader_committed_log_index: 0,
            raft_config,
        };

    assert_eq!(
        keeper_cluster_membership.queried_keeper,
        expected_keeper_cluster_membership.queried_keeper
    );
    assert_eq!(
        keeper_cluster_membership.raft_config,
        expected_keeper_cluster_membership.raft_config
    );

    Ok(())
}

#[tokio::test]
async fn test_teardown() -> anyhow::Result<()> {
    let (logctx, path) = default_clickhouse_log_ctx_and_path();

    info!(&logctx.log, "Tearing down ClickHouse cluster"; "path" => ?path);

    let deployment = clickhouse_cluster_test_deployment_with_dynamic_ports(path.clone())?;
    deployment.teardown()?;
    std::fs::remove_dir_all(path)?;
    logctx.cleanup_successful();

    Ok(())
}
