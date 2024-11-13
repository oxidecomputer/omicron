// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use clickhouse_admin_test_utils::DEFAULT_CLICKHOUSE_ADMIN_BASE_PORTS;
use clickward::{Deployment, DeploymentConfig, KeeperId};
use dropshot::test_util::{log_prefix_for_test, LogContext};
use dropshot::{ConfigLogging, ConfigLoggingLevel};
use oximeter_db::Client;
use oximeter_test_utils::{wait_for_keepers, wait_for_ping};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let request_timeout = Duration::from_secs(15);
    let logctx = LogContext::new(
        "clickhouse_cluster",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );

    let (parent_dir, _prefix) = log_prefix_for_test(logctx.test_name());
    // TODO: Switch to "{prefix}_clickward_test"?
    let path = parent_dir.join("clickward_test");
    std::fs::create_dir(&path)?;

    slog::info!(logctx.log, "Setting up a ClickHouse cluster");

    // We spin up several replicated clusters and must use a
    // separate set of ports in case the tests run concurrently.
    let base_ports = DEFAULT_CLICKHOUSE_ADMIN_BASE_PORTS;

    let config = DeploymentConfig {
        path: path.clone(),
        base_ports,
        cluster_name: "oximeter_cluster".to_string(),
    };

    let mut deployment = Deployment::new(config);

    let num_keepers = 3;
    let num_replicas = 2;
    deployment
        .generate_config(num_keepers, num_replicas)
        .context("failed to generate config")?;
    deployment.deploy().context("failed to deploy")?;

    let client1 = Client::new_with_request_timeout(
        deployment.http_addr(1.into()),
        deployment.native_addr(1.into()),
        &logctx.log,
        request_timeout,
    );
    let client2 = Client::new_with_request_timeout(
        deployment.http_addr(2.into()),
        deployment.native_addr(2.into()),
        &logctx.log,
        request_timeout,
    );

    wait_for_ping(&logctx.log, &client1).await?;
    wait_for_ping(&logctx.log, &client2).await?;
    wait_for_keepers(
        &logctx.log,
        &deployment,
        (1..=num_keepers).map(KeeperId).collect(),
    )
    .await?;

    Ok(())
}
