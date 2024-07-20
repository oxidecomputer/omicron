// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use clickward::{Deployment, KeeperClient, KeeperError};
use dropshot::test_util::log_prefix_for_test;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::test_setup_log;
use oximeter::test_util;
use oximeter_db::{Client, DbWrite, OxqlResult, Sample};
use std::default::Default;
use std::time::Duration;

pub struct TestInput {
    n_projects: usize,
    n_instances: usize,
    n_cpus: usize,
    n_samples: usize,
}

impl Default for TestInput {
    fn default() -> Self {
        TestInput { n_projects: 2, n_instances: 2, n_cpus: 2, n_samples: 1000 }
    }
}

impl TestInput {
    fn n_timeseries(&self) -> usize {
        self.n_projects * self.n_instances * self.n_cpus
    }

    fn n_points(&self) -> usize {
        self.n_projects * self.n_instances * self.n_cpus * self.n_samples
    }
}

#[tokio::test]
async fn test_cluster() -> anyhow::Result<()> {
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

    let input = TestInput::default();

    // Let's write some samples to our first replica and wait for them to show
    // up on replica 2.
    let samples = test_util::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    assert_eq!(samples.len(), input.n_points());
    client1.insert_samples(&samples).await.expect("failed to insert samples");

    // Get all the samples from the replica where the data was inserted
    let oxql_res1 = client1
        .oxql_query("get virtual_machine:cpu_busy")
        .await
        .expect("failed to get all samples");

    // Ensure the samples are correct on this replica
    assert_input_and_output(&input, &samples, &oxql_res1);

    wait_for_expected_output(&client2, &samples)
        .await
        .expect("failed to get samples from client2");

    deployment.teardown()?;
    std::fs::remove_dir_all(path)?;
    logctx.cleanup_successful();

    Ok(())
}

fn total_points(oxql_res: &OxqlResult) -> usize {
    if oxql_res.tables.len() != 1 {
        return 0;
    }
    let table = &oxql_res.tables.first().unwrap();
    let mut total_points = 0;
    for timeseries in table.timeseries() {
        total_points += timeseries.points.iter_points().count();
    }
    total_points
}

/// Ensure the samples inserted come back as points when queried
fn assert_input_and_output(
    input: &TestInput,
    samples: &[Sample],
    oxql_res: &OxqlResult,
) {
    assert_eq!(oxql_res.tables.len(), 1);
    let table = oxql_res.tables.first().unwrap();
    assert_eq!(table.n_timeseries(), input.n_timeseries());
    assert_eq!(total_points(&oxql_res), samples.len());
}

/// Wait for all `samples` inserted to exist at the replica that `client` is
/// speaking to.
async fn wait_for_expected_output(
    client: &Client,
    samples: &[Sample],
) -> anyhow::Result<()> {
    poll::wait_for_condition(
        || async {
            let oxql_res = client
                .oxql_query("get virtual_machine:cpu_busy")
                .await
                .map_err(|_| {
                    poll::CondCheckError::<oximeter_db::Error>::NotYet
                })?;
            if total_points(&oxql_res) != samples.len() {
                Err(poll::CondCheckError::<oximeter_db::Error>::NotYet)
            } else {
                Ok(())
            }
        },
        &Duration::from_millis(10),
        &Duration::from_secs(10),
    )
    .await
    .context("failed to ping clickhouse server")?;
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
