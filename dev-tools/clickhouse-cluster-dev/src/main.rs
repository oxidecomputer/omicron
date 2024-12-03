// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sets up a 3 keeper 2 replica ClickHouse cluster for clickhouse-admin
//! integration tests.
//!
//! NB: This should only be used for testing that doesn't write data to
//! ClickHouse. Otherwise, it may result in flaky tests.

use anyhow::{Context, Result};
use clickhouse_admin_test_utils::{
    default_clickhouse_cluster_test_deployment,
    default_clickhouse_log_ctx_and_path,
};
use clickward::KeeperId;
use oximeter_db::Client;
use oximeter_test_utils::{wait_for_keepers, wait_for_ping};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let request_timeout = Duration::from_secs(15);
    let (logctx, path) = default_clickhouse_log_ctx_and_path();
    
    if path.exists() {
        slog::info!(logctx.log, "Removing previous temporary test directory");
        std::fs::remove_dir_all(&path)?;
    }

    std::fs::create_dir(&path)?;

    slog::info!(logctx.log, "Setting up a ClickHouse cluster");

    let mut deployment =
        default_clickhouse_cluster_test_deployment(path.clone());

    let num_keepers = 3;
    let num_replicas = 2;
    deployment
        .generate_config(num_keepers, num_replicas)
        .context("failed to generate config")?;
    deployment.deploy().context("failed to deploy")?;

    let client1 = Client::new_with_request_timeout(
        deployment.native_addr(1.into()),
        &logctx.log,
        request_timeout,
    );
    let client2 = Client::new_with_request_timeout(
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
