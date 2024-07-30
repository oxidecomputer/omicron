// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use clickward::{Deployment, KeeperClient, KeeperError};
use dropshot::test_util::log_prefix_for_test;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::test_setup_log;
use oximeter::test_util;
use oximeter_db::{Client, DbWrite, OxqlResult, Sample, TestDbWrite};
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
        TestInput { n_projects: 2, n_instances: 2, n_cpus: 2, n_samples: 50 }
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
    usdt::register_probes().unwrap();
    let request_timeout = Duration::from_secs(15);
    let start = tokio::time::Instant::now();
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

    let client1 = Client::new_with_request_timeout(
        deployment.http_addr(1)?,
        log,
        request_timeout,
    );
    let client2 = Client::new_with_request_timeout(
        deployment.http_addr(2)?,
        log,
        request_timeout,
    );
    wait_for_ping(&client1).await?;
    wait_for_ping(&client2).await?;
    wait_for_keepers(&deployment, (1..=num_keepers).collect()).await?;
    println!("deploy setup time = {:?}", start.elapsed());

    let start = tokio::time::Instant::now();
    client1
        .init_test_minimal_replicated_db()
        .await
        .context("failed to initialize db")?;
    println!("init replicated db time = {:?}", start.elapsed());

    // Ensure our database tables show up on both servers
    let start = tokio::time::Instant::now();
    let output1 = client1.list_replicated_tables().await?;
    let output2 = client2.list_replicated_tables().await?;
    println!("list tables time = {:?}", start.elapsed());
    assert_eq!(output1, output2);

    let input = TestInput::default();

    // Let's write some samples to our first replica and wait for them to show
    // up on replica 2.
    let start = tokio::time::Instant::now();
    let samples = test_util::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    println!("generate samples time = {:?}", start.elapsed());
    assert_eq!(samples.len(), input.n_points());
    let start = tokio::time::Instant::now();
    client1.insert_samples(&samples).await.expect("failed to insert samples");
    println!("insert samples time = {:?}", start.elapsed());

    // Get all the samples from the replica where the data was inserted
    let start = tokio::time::Instant::now();
    let oxql_res1 = client1
        .oxql_query("get virtual_machine:cpu_busy")
        .await
        .expect("failed to get all samples");
    println!("query samples from client1 time = {:?}", start.elapsed());

    // Ensure the samples are correct on this replica
    assert_input_and_output(&input, &samples, &oxql_res1);

    let start = tokio::time::Instant::now();
    wait_for_num_points(&client2, samples.len())
        .await
        .expect("failed to get samples from client2");
    println!("query samples from client2 time = {:?}", start.elapsed());

    // Add a 3rd clickhouse server and wait for it to come up
    deployment.add_server().expect("failed to launch a 3rd clickhouse server");
    let client3 = Client::new_with_request_timeout(
        deployment.http_addr(3)?,
        log,
        request_timeout,
    );
    wait_for_ping(&client3).await?;

    // We need to initiate copying from existing replicated tables by creating
    // the DB and those tables on the new node.
    client3
        .init_test_minimal_replicated_db()
        .await
        .expect("failed to initialized db");

    // Wait for all the data to be copied to node 3
    let start = tokio::time::Instant::now();
    wait_for_num_points(&client3, samples.len())
        .await
        .expect("failed to get samples from client3");
    println!("query samples from client3 time = {:?}", start.elapsed());

    // Let's stop replica 1 and write some data to replica 3
    // When we bring replica 1 back up we should see data get replicated to it.
    // Data should also get replicated to replica 2 immediately.
    deployment.stop_server(1).unwrap();

    // Generate some new samples and insert them at replica3
    let samples = test_util::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    client3.insert_samples(&samples).await.expect("failed to insert samples");

    // Ensure that both replica 3 and replica 2 have the samples
    wait_for_num_points(&client3, samples.len() * 2)
        .await
        .expect("failed to get samples from client3");
    wait_for_num_points(&client2, samples.len() * 2)
        .await
        .expect("failed to get samples from client2");

    // Restart node 1 and wait for the data to replicate
    deployment.start_server(1).expect("failed to restart clickhouse server 1");
    wait_for_ping(&client1).await.expect("failed to ping server 1");
    wait_for_num_points(&client1, samples.len() * 2)
        .await
        .expect("failed to get samples from client1");

    // Remove a keeper node
    deployment.remove_keeper(2).expect("failed to remove keeper");

    // Querying from any node should still work after a keeper is removed
    wait_for_num_points(&client1, samples.len() * 2)
        .await
        .expect("failed to get samples from client1");

    // We still have a quorum (2 of 3 keepers), so we should be able to insert
    let samples = test_util::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    client3.insert_samples(&samples).await.expect("failed to insert samples");
    wait_for_num_points(&client2, samples.len() * 3)
        .await
        .expect("failed to get samples from client1");

    // Stop another keeper
    deployment.stop_keeper(1).expect("failed to stop keeper");

    // We should still be able to query
    wait_for_num_points(&client3, samples.len() * 3)
        .await
        .expect("failed to get samples from client1");

    println!("Attempting to insert samples without keeper quorum");
    let samples = test_util::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    // We have lost quorum and should not be able to insert
    client1
        .insert_samples(&samples)
        .await
        .expect_err("insert succeeded without keeper quorum");

    // Bringing the keeper back up should allow us to insert again
    deployment.start_keeper(1).expect("failed to restart keeper");
    wait_for_keepers(&deployment, vec![1, 3])
        .await
        .expect("failed to sync keepers");
    let samples = test_util::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    client1.insert_samples(&samples).await.expect("failed to insert samples");
    wait_for_num_points(&client2, samples.len() * 4)
        .await
        .expect("failed to get samples from client1");

    // Add a new keeper to restore fault tolerance and try again
    deployment.add_keeper().expect("Failed to add keeeper");
    wait_for_keepers(&deployment, vec![1, 3, 4])
        .await
        .expect("failed to sync keepers");
    let samples = test_util::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    client1.insert_samples(&samples).await.expect("failed to insert samples");
    wait_for_num_points(&client2, samples.len() * 5)
        .await
        .expect("failed to get samples from client1");

    println!("Cleaning up test");
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

/// Wait for the number of `samples` inserted to equal the number of `points`
/// returned at the replica that `client` is speaking to.
async fn wait_for_num_points(
    client: &Client,
    n_samples: usize,
) -> anyhow::Result<()> {
    poll::wait_for_condition(
        || async {
            let oxql_res = client
                .oxql_query("get virtual_machine:cpu_busy")
                .await
                .map_err(|_| {
                    poll::CondCheckError::<oximeter_db::Error>::NotYet
                })?;
            if total_points(&oxql_res) != n_samples {
                println!(
                    "received {}, expected {}",
                    total_points(&oxql_res),
                    n_samples
                );
                Err(poll::CondCheckError::<oximeter_db::Error>::NotYet)
            } else {
                Ok(())
            }
        },
        &Duration::from_millis(10),
        &Duration::from_secs(30),
    )
    .await
    .context("failed to get all samples from clickhouse server")?;
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
        &Duration::from_secs(30),
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
