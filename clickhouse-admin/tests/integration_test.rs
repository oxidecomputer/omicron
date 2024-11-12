// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use camino::Utf8PathBuf;
use clickhouse_admin_types::{
    ClickhouseHost, ClickhouseKeeperClusterMembership, KeeperId,
    KeeperServerInfo, KeeperServerType, RaftConfig,
};
use clickward::{BasePorts, Deployment, DeploymentConfig};
use dropshot::test_util::{log_prefix_for_test, LogContext};
use dropshot::{ConfigLogging, ConfigLoggingLevel};
use omicron_clickhouse_admin::ClickhouseCli;
use omicron_test_utils::dev::test_setup_log;
use oximeter_test_utils::wait_for_keepers;
use slog::info;
use std::collections::BTreeSet;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::str::FromStr;

#[tokio::test]
async fn test_1() {
    println!("Running test 1...");
}

#[tokio::test]
async fn test_2() {
    println!("Running test 2...");
    //     assert_eq!(1, 2);
}

#[tokio::test]
async fn test_lgif_parsing() -> anyhow::Result<()> {
    // TODO: Do we want these to have their own log dirs or not?
    let logctx = LogContext::new(
        "clickhouse_cluster",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );

    let clickhouse_cli = ClickhouseCli::new(
        Utf8PathBuf::from_str("clickhouse")?,
        SocketAddrV6::new(Ipv6Addr::LOCALHOST, 29001, 0, 0),
    )
    .with_log(logctx.log);

    let lgif = clickhouse_cli.lgif().await?;

    // The first log index from a newly created cluster should always be 1
    assert_eq!(lgif.first_log_idx, 1);

    Ok(())
}

#[tokio::test]
async fn test_raft_config_parsing() -> anyhow::Result<()> {
    let logctx = LogContext::new(
        "clickhouse_cluster",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );

    let clickhouse_cli = ClickhouseCli::new(
        Utf8PathBuf::from_str("clickhouse").unwrap(),
        SocketAddrV6::new(Ipv6Addr::LOCALHOST, 29001, 0, 0),
    )
    .with_log(logctx.log);

    let raft_config = clickhouse_cli.raft_config().await.unwrap();

    let mut keeper_servers = BTreeSet::new();
    let num_keepers = 3;

    for i in 1..=num_keepers {
        let raft_port = u16::try_from(29100 + i).unwrap();
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
    let logctx = LogContext::new(
        "clickhouse_cluster",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );

    let clickhouse_cli = ClickhouseCli::new(
        Utf8PathBuf::from_str("clickhouse").unwrap(),
        SocketAddrV6::new(Ipv6Addr::LOCALHOST, 29001, 0, 0),
    )
    .with_log(logctx.log);

    let conf = clickhouse_cli.keeper_conf().await.unwrap();

    assert_eq!(conf.server_id, clickhouse_admin_types::KeeperId(1));
 
    Ok(())
}

#[tokio::test]
async fn test_keeper_cluster_membership() -> anyhow::Result<()> {
    let logctx = LogContext::new(
        "clickhouse_cluster",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );

    let clickhouse_cli = ClickhouseCli::new(
        Utf8PathBuf::from_str("clickhouse").unwrap(),
        SocketAddrV6::new(Ipv6Addr::LOCALHOST, 29001, 0, 0),
    )
    .with_log(logctx.log);

    let keeper_cluster_membership =
        clickhouse_cli.keeper_cluster_membership().await.unwrap();

    let mut raft_config = BTreeSet::new();

    let num_keepers = 3;
    for i in 1..=num_keepers {
        raft_config.insert(clickhouse_admin_types::KeeperId(i));
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
    let logctx = LogContext::new(
        "clickhouse_cluster",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );

    let (parent_dir, _prefix) = log_prefix_for_test("clickhouse_cluster");
    // TODO: Switch to "{prefix}_clickward_test" ?
    let path = parent_dir.join(format!("clickward_test"));

    info!(&logctx.log, "Tearing down ClickHouse cluster"; "path" => ?path);

    // TODO: Find another way to retrieve deployment

    // We spin up several replicated clusters and must use a
    // separate set of ports in case the tests run concurrently.
    let base_ports = BasePorts {
        keeper: 29000,
        raft: 29100,
        clickhouse_tcp: 29200,
        clickhouse_http: 29300,
        clickhouse_interserver_http: 29400,
    };

    let config = DeploymentConfig {
        path: path.clone(),
        base_ports,
        cluster_name: "oximeter_cluster".to_string(),
    };

    let deployment = Deployment::new(config);
    deployment.teardown()?;
    std::fs::remove_dir_all(path)?;
    logctx.cleanup_successful();

    Ok(())
}