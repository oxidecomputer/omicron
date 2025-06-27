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
use scopeguard::ScopeGuard;
use std::time::Duration;

fn main() -> Result<()> {
    oxide_tokio_rt::run(main_impl())
}

async fn main_impl() -> Result<()> {
    let request_timeout = Duration::from_secs(15);
    let (logctx, path) = default_clickhouse_log_ctx_and_path();

    if path.exists() {
        let deployment =
            default_clickhouse_cluster_test_deployment(path.clone());
        slog::info!(logctx.log, "Stopping test clickhouse nodes");
        deployment.teardown()?;
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

    // Put `deployment` into a scope guard that tears it down if we fail or
    // panic below. We'll defuse it on success.
    let deployment = scopeguard::guard(deployment, |deployment| {
        slog::info!(
            logctx.log,
            "Stopping test clickhouse nodes due to failure",
        );
        if let Err(err) = deployment.teardown() {
            slog::error!(
                logctx.log,
                "Failed to tear down clickhouse nodes";
                "err" => #%err,
            );
        }
    });

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

    wait_for_ping(&logctx.log, &client1).await.with_context(|| {
        format!(
            "client 1 timeout with deployment configuration: {:#?}",
            deployment
        )
    })?;
    wait_for_ping(&logctx.log, &client2).await.with_context(|| {
        format!(
            "client 2 timeout with deployment configuration: {:#?}",
            deployment
        )
    })?;
    wait_for_keepers(
        &logctx.log,
        &deployment,
        (1..=num_keepers).map(KeeperId).collect(),
    )
    .await
    .with_context(|| {
        format!(
            "keeper servers failure with deployment configuration: {:#?}",
            deployment
        )
    })?;

    // Deployment complete: defuse the scopeguard to avoid tearing it down.
    _ = ScopeGuard::into_inner(deployment);

    Ok(())
}
