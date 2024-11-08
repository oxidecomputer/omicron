// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use clickward::{BasePorts, Deployment, DeploymentConfig};
use dropshot::test_util::{log_prefix_for_test, LogContext};
use dropshot::{ConfigLogging, ConfigLoggingLevel};
use oximeter_test_utils::wait_for_keepers;

#[tokio::main]
async fn main() -> Result<()> {
    let logctx = LogContext::new(
        "clickhouse_cluster",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );

    let (parent_dir, _prefix) = log_prefix_for_test(logctx.test_name());
    // TODO: Switch to "{prefix}_clickward_test"?
    let path = parent_dir.join(format!("clickward_test"));
    std::fs::create_dir(&path)?;

    slog::info!(logctx.log, "Setting up a ClickHouse cluster");

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

    let mut deployment = Deployment::new(config);

    // TODO: Use 2 replicas and 3 keepers, for now I'm just testing
    let num_keepers = 1;
    let num_replicas = 1;
    deployment
        .generate_config(num_keepers, num_replicas)
        .context("failed to generate config")?;
    deployment.deploy().context("failed to deploy")?;

    wait_for_keepers(&logctx.log, &deployment, vec![clickward::KeeperId(1)])
        .await?;

    // TODO: Also wait for replicas

    Ok(())
}
