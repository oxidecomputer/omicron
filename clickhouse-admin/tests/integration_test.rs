// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use camino::Utf8PathBuf;
use clickward::{BasePorts, Deployment, DeploymentConfig, KeeperId};
use dropshot::test_util::log_prefix_for_test;
use omicron_clickhouse_admin::ClickhouseCli;
use omicron_test_utils::dev::test_setup_log;
use oximeter_test_utils::wait_for_keepers;
use slog::info;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::str::FromStr;

#[tokio::test]
async fn test_lgif_parsing() -> anyhow::Result<()> {
    let logctx = test_setup_log("test_lgif_parsing");
    let log = logctx.log.clone();

    let (parent_dir, prefix) = log_prefix_for_test(logctx.test_name());
    let path = parent_dir.join(format!("{prefix}-oximeter-clickward-test"));
    std::fs::create_dir(&path)?;

    // We use the default ports in `test_schemas_disjoint` and must use a
    // separate set here in case the two tests run concurrently.
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

    let mut deployment = Deployment::new(config);

    // We only need a single keeper to test the lgif command
    let num_keepers = 1;
    let num_replicas = 1;
    deployment
        .generate_config(num_keepers, num_replicas)
        .context("failed to generate config")?;
    deployment.deploy().context("failed to deploy")?;

    wait_for_keepers(&log, &deployment, vec![KeeperId(1)]).await?;

    let clickhouse_cli = ClickhouseCli::new(
        Utf8PathBuf::from_str("clickhouse").unwrap(),
        SocketAddrV6::new(Ipv6Addr::LOCALHOST, 29001, 0, 0),
    )
    .with_log(log.clone());

    let lgif = clickhouse_cli.lgif().await.unwrap();

    // The first log index from a newly created cluster should always be 1
    assert_eq!(lgif.first_log_idx, 1);

    info!(&log, "Cleaning up test");
    deployment.teardown()?;
    std::fs::remove_dir_all(path)?;
    logctx.cleanup_successful();
    Ok(())
}
