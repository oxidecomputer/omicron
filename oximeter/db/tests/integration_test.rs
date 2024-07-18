// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use clickward::{Deployment, KeeperClient, KeeperError};
use dropshot::test_util::log_prefix_for_test;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::test_setup_log;
use oximeter_db::{Client, DbWrite};
use std::time::Duration;

#[tokio::test]
async fn test_cluster() -> anyhow::Result<()> {
    tokio::time::sleep(Duration::from_secs(10)).await;
    let logctx = test_setup_log("test_cluster");
    let log = &logctx.log;

    let (parent_dir, prefix) = log_prefix_for_test(logctx.test_name());
    let path = parent_dir.join(format!("{prefix}-oximeter-clickward-test"));
    std::fs::create_dir(&path)?;

    let mut deployment = Deployment::new_with_default_port_config(
        path.clone(),
        "oximeter_cluster".to_string(),
    );

    let num_keepers = 3;
    let num_replicas = 2;
    deployment
        .generate_config(num_keepers, num_replicas)
        .context("failed to generate config")?;
    deployment.deploy().context("failed to deploy")?;

    let client1 = Client::new(deployment.http_addr(1)?, log);
    let client2 = Client::new(deployment.http_addr(2)?, log);
    wait_for_ping(&client1).await?;
    wait_for_ping(&client2).await?;
    wait_for_keepers(&deployment, (1..=3).collect()).await?;

    client1.init_replicated_db().await.context("Failed to initialize db")?;

    // Ensure our database tables show up on both servers
    let output1 = client1.list_replicated_tables().await?;
    let output2 = client2.list_replicated_tables().await?;
    assert_eq!(output1, output2);

    deployment.teardown()?;
    std::fs::remove_dir_all(path)?;
    logctx.cleanup_successful();

    Ok(())
}

/// Wait for all keeper servers to be capable of handling commands
async fn wait_for_keepers(
    deployment: &Deployment,
    ids: Vec<u64>,
) -> anyhow::Result<()> {
    let mut keepers = vec![];
    for id in ids {
        keepers.push(KeeperClient::new(deployment.keeper_addr(id)?));
    }

    poll::wait_for_condition(
        || async {
            let mut done = true;
            for keeper in &keepers {
                match keeper.config().await {
                    Ok(config) => {
                        // The node isn't really up yet
                        if config.len() != keepers.len() {
                            done = false;
                            break;
                        }
                    }
                    Err(_) => {
                        done = false;
                    }
                }
            }
            if !done {
                Err(poll::CondCheckError::<KeeperError>::NotYet)
            } else {
                Ok(())
            }
        },
        &Duration::from_millis(1),
        &Duration::from_secs(10),
    )
    .await
    .context("failed to contact all keepers")?;

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
