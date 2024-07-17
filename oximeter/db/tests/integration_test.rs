// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use camino_tempfile::Builder;
use dropshot::test_util::log_prefix_for_test;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::test_setup_log;
use oximeter_db::Client;
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::test]
async fn test_cluster() -> anyhow::Result<()> {
    let logctx = test_setup_log("test_cluster");
    let log = &logctx.log;

    let (parent_dir, prefix) = log_prefix_for_test(logctx.test_name());
    let dir = Builder::new()
        .prefix(&format!("{prefix}-clickward-deployment"))
        .tempdir_in(parent_dir)
        .context("failed to create tempdir for clickward deployment")?;

    let deployment =
        clickward::Deployment::new_with_default_port_config(dir.path().into());

    let num_keepers = 3;
    let num_replicas = 2;
    deployment
        .generate_config(num_keepers, num_replicas)
        .context("failed to generate config")?;
    deployment.deploy().context("failed to deploy")?;

    let port = deployment.http_port(1);
    let addr: SocketAddr = format!("[::1]:{port}").parse().unwrap();
    let client = Client::new(addr, log);
    wait_for_ping(&client).await?;

    deployment.teardown()?;

    Ok(())
}

/// Try to ping the server until it is responds.
async fn wait_for_ping(client: &Client) -> anyhow::Result<()> {
    poll::wait_for_condition(
        || async {
            client
                .ping()
                .await
                .map_err(|_| poll::CondCheckError::<oximeter_db::Error>::NotYet)
        },
        &Duration::from_millis(1),
        &Duration::from_secs(10),
    )
    .await
    .context("failed to ping clickhouse server")?;
    Ok(())
}
