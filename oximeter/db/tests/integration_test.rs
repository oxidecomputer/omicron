// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use clickward::{BasePorts, Deployment, DeploymentConfig, KeeperId};
use dropshot::test_util::log_prefix_for_test;
use omicron_test_utils::dev::file_checksum;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::test_setup_log;
use oximeter_db::oxql::query::QueryAuthzScope;
use oximeter_db::{Client, DbWrite, OxqlResult, Sample, TestDbWrite};
use oximeter_test_utils::wait_for_keepers;
use slog::{Logger, info};
use std::collections::BTreeSet;
use std::default::Default;
use std::path::PathBuf;
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

/// Ensure the oximeter database schemas for both single node and replicated
/// cluster are not modified.
///
/// Schema changes for the `oximeter` database are not allowed because there is
/// no mechanism to apply them to existing systems. If we need to support schema
/// changes at some point, we'll have to do the work to update them during
/// automated update. See https://github.com/oxidecomputer/omicron/issues/8862
/// for details.
#[tokio::test]
async fn test_schemas_are_not_modified() -> anyhow::Result<()> {
    let cur_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let single_node_schema_checksum =
        file_checksum(cur_dir.as_path().join("schema/single-node/db-init.sql"))
            .unwrap();
    let replicated_schema_1_checksum = file_checksum(
        cur_dir.as_path().join("schema/replicated/db-init-1.sql"),
    )
    .unwrap();
    let replicated_schema_2_checksum = file_checksum(
        cur_dir.as_path().join("schema/replicated/db-init-2.sql"),
    )
    .unwrap();

    assert_eq!(
        "1422131e72b3410b7f4520bf72a213f0c3288ce8cd738a0010b1bf44edea480a",
        single_node_schema_checksum
    );
    assert_eq!(
        "038ca7fc66006d6c06243a6fa122cf2f0bfee6edf55227e13166a9b854169abb",
        replicated_schema_1_checksum
    );
    assert_eq!(
        "d78c49e5662a2bd211eba63cc71a33bffacca7c6a80d9090532329650c6baae0",
        replicated_schema_2_checksum
    );
    Ok(())
}

/// Ensure `db-init-1.sql` and `db-init-2.sql` contain disjoint sets of tables.
#[tokio::test]
async fn test_schemas_disjoint() -> anyhow::Result<()> {
    let request_timeout = Duration::from_secs(15);
    let logctx = test_setup_log("test_schemas_disjoint");
    let log = &logctx.log;

    let (parent_dir, prefix) = log_prefix_for_test(logctx.test_name());
    let path = parent_dir.join(format!("{prefix}-oximeter-clickward-test"));
    std::fs::create_dir(&path)?;

    let mut deployment = Deployment::new_with_default_port_config(
        path.clone(),
        "oximeter_cluster".to_string(),
    );

    // We are not testing replication here. We are just testing that the tables
    // loaded by each sql file are not loaded by the other sql file.
    let num_keepers = 1;
    let num_replicas = 1;
    deployment
        .generate_config(num_keepers, num_replicas)
        .context("failed to generate config")?;
    deployment.deploy().context("failed to deploy")?;

    let client1 = Client::new_with_request_timeout(
        deployment.native_addr(1.into()),
        log,
        request_timeout,
    );

    wait_for_ping(log, &client1).await?;
    wait_for_keepers(log, &deployment, vec![KeeperId(1)]).await?;

    // Load all the tables inserted by `db-init-1.sql`
    client1.init_replicated_db_from_file(1).await?;
    let tables1 = client1
        .list_replicated_tables()
        .await
        .expect("failed to read replicated tables");

    // Drop the database so that we can insert the tables from `db-init-2.sql`
    client1.wipe_replicated_db().await.expect("failed to wipe db");

    // Load all the tables inserted by `db-init-2.sql`
    client1.init_replicated_db_from_file(2).await?;
    let tables2: BTreeSet<_> = client1
        .list_replicated_tables()
        .await
        .expect("failed to read replicated tables")
        .into_iter()
        .collect();

    // Ensure that tables1 and tables2 are disjoint
    for table in &tables1 {
        assert!(
            !tables2.contains(table),
            "table `{}` exists in both `db-init-1.sql` and `db-init-2.sql`",
            table
        );
    }

    info!(log, "Cleaning up test");
    deployment.teardown()?;
    std::fs::remove_dir_all(path)?;
    logctx.cleanup_successful();

    Ok(())
}

/// Test that replicated/clustered clickhouse setups work as we expect.
///
/// Note that we are querying from [distributed](
/// https://clickhouse.com/docs/en/engines/table-engines/special/distributed)
/// tables here, but we are not sharding. This test assumes that all inserts
/// and queries therefore access data from the contacted server, since we have
/// full replicas at all servers. The "first healthy node" should always be
/// the one we contact. Regardless, even if the contacted node proxies to a
/// different server this test ensures that we are able to query the latest
/// data. While we could always guarantee we are inserting to or querying from
/// the contacted server by using the `_local` tables instead of the distributed
/// tables used by oxql, that would not be what we do in production, and so
/// doesn't make much sense in an integration test.
#[tokio::test]
async fn test_cluster() -> anyhow::Result<()> {
    usdt::register_probes().expect("Failed to register USDT probes");
    let request_timeout = Duration::from_secs(15);
    let start = tokio::time::Instant::now();
    let logctx = test_setup_log("test_cluster");
    let log = &logctx.log;

    let (parent_dir, prefix) = log_prefix_for_test(logctx.test_name());
    let path = parent_dir.join(format!("{prefix}-oximeter-clickward-test"));
    std::fs::create_dir(&path)?;

    // We use the default ports in `test_schemas_disjoint` and must use a
    // separate set here in case the two tests run concurrently.
    let base_ports = BasePorts {
        keeper: 19000,
        raft: 19100,
        clickhouse_tcp: 19200,
        clickhouse_http: 19300,
        clickhouse_interserver_http: 19400,
    };

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
        deployment.native_addr(1.into()),
        log,
        request_timeout,
    );
    let client2 = Client::new_with_request_timeout(
        deployment.native_addr(2.into()),
        log,
        request_timeout,
    );
    wait_for_ping(log, &client1).await?;
    wait_for_ping(log, &client2).await?;
    wait_for_keepers(
        log,
        &deployment,
        (1..=num_keepers).map(KeeperId).collect(),
    )
    .await?;
    info!(log, "deploy setup time = {:?}", start.elapsed());

    let start = tokio::time::Instant::now();
    client1
        .init_test_minimal_replicated_db()
        .await
        .context("failed to initialize db")?;
    info!(log, "init replicated db time = {:?}", start.elapsed());

    // Ensure our database tables show up on both servers
    let start = tokio::time::Instant::now();
    let output1 = client1.list_replicated_tables().await?;
    let output2 = client2.list_replicated_tables().await?;
    info!(log, "list tables time = {:?}", start.elapsed());
    assert_eq!(output1, output2);

    let input = TestInput::default();

    // Let's write some samples to our first replica and wait for them to show
    // up on replica 2.
    let start = tokio::time::Instant::now();
    let samples = oximeter_test_utils::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    info!(log, "generate samples time = {:?}", start.elapsed());
    assert_eq!(samples.len(), input.n_points());
    let start = tokio::time::Instant::now();
    client1.insert_samples(&samples).await.expect("failed to insert samples");
    info!(log, "insert samples time = {:?}", start.elapsed());

    // Get all the samples from the replica where the data was inserted
    let start = tokio::time::Instant::now();
    let oxql_res1 = client1
        .oxql_query(
            "get virtual_machine:cpu_busy | filter timestamp > @2000-01-01",
            QueryAuthzScope::Fleet,
        )
        .await
        .expect("failed to get all samples");
    info!(log, "query samples from client1 time = {:?}", start.elapsed());

    // Ensure the samples are correct on this replica
    assert_input_and_output(&input, &samples, &oxql_res1);

    let start = tokio::time::Instant::now();
    wait_for_num_points(&log, &client2, samples.len())
        .await
        .expect("failed to get samples from client2");
    info!(log, "query samples from client2 time = {:?}", start.elapsed());

    // Add a 3rd clickhouse server and wait for it to come up
    deployment.add_server().expect("failed to launch a 3rd clickhouse server");
    let client3 = Client::new_with_request_timeout(
        deployment.native_addr(3.into()),
        log,
        request_timeout,
    );
    wait_for_ping(log, &client3).await?;
    info!(log, "successfully pinged client server 3");

    // We need to initiate copying from existing replicated tables by creating
    // the DB and those tables on the new node.
    client3
        .init_test_minimal_replicated_db()
        .await
        .expect("failed to initialized db");
    info!(log, "successfully pinged client server 3");

    // Wait for all the data to be copied to node 3
    let start = tokio::time::Instant::now();
    wait_for_num_points(&log, &client3, samples.len())
        .await
        .expect("failed to get samples from client3");
    info!(log, "query samples from client3 time = {:?}", start.elapsed());

    // Let's stop replica 1 and write some data to replica 3
    // When we bring replica 1 back up we should see data get replicated to it.
    // Data should also get replicated to replica 2 immediately.
    deployment.stop_server(1.into()).unwrap();

    info!(log, "successfully stopped server 1");

    // Generate some new samples and insert them at replica3
    let samples = oximeter_test_utils::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    client3
        .insert_samples(&samples)
        .await
        .expect("failed to insert samples at server3");

    // Ensure that both replica 3 and replica 2 have the samples
    wait_for_num_points(&log, &client3, samples.len() * 2)
        .await
        .expect("failed to get samples from client3");
    wait_for_num_points(&log, &client2, samples.len() * 2)
        .await
        .expect("failed to get samples from client2");

    // Restart node 1 and wait for the data to replicate
    deployment
        .start_server(1.into())
        .expect("failed to restart clickhouse server 1");
    wait_for_ping(log, &client1).await.expect("failed to ping server 1");
    wait_for_num_points(&log, &client1, samples.len() * 2)
        .await
        .expect("failed to get samples from client1");

    // Remove a keeper node
    deployment.remove_keeper(KeeperId(2)).expect("failed to remove keeper 2");

    // Querying from any node should still work after a keeper is removed
    wait_for_num_points(&log, &client1, samples.len() * 2)
        .await
        .expect("failed to get samples from client1");

    // We still have a quorum (2 of 3 keepers), so we should be able to insert
    // Removing a node may require a new leader election which would require
    // some polling, so we go ahead and do that.
    let samples = oximeter_test_utils::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    wait_for_insert(log, &client3, &samples)
        .await
        .expect("failed to insert samples at server3");
    wait_for_num_points(&log, &client2, samples.len() * 3)
        .await
        .expect("failed to get samples from client2");

    // Stop another keeper
    deployment.stop_keeper(1.into()).expect("failed to stop keeper 1");

    // We should still be able to query
    wait_for_num_points(&log, &client3, samples.len() * 3)
        .await
        .expect("failed to get samples from client1");

    info!(log, "Attempting to insert samples without keeper quorum");
    let samples = oximeter_test_utils::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    // We are expecting a timeout here. Typical inserts take on the order of a
    // few hundred milliseconds. To shorten the length of our test, we create a
    // new client with a shorter timeout.
    let client1_short_timeout = Client::new_with_request_timeout(
        deployment.native_addr(1.into()),
        log,
        Duration::from_secs(2),
    );
    // We have lost quorum and should not be able to insert
    client1_short_timeout
        .insert_samples(&samples)
        .await
        .expect_err("insert succeeded without keeper quorum");

    // Bringing the keeper back up should allow us to insert again
    deployment.start_keeper(1.into()).expect("failed to restart keeper");
    wait_for_keepers(
        log,
        &deployment,
        [1, 3].map(KeeperId).into_iter().collect(),
    )
    .await
    .expect("failed to sync keepers");
    let samples = oximeter_test_utils::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    wait_for_insert(log, &client1, &samples)
        .await
        .expect("failed to insert samples at server1");
    wait_for_num_points(&log, &client2, samples.len() * 4)
        .await
        .expect("failed to get samples from client1");

    // Add a new keeper to restore fault tolerance and try again
    deployment.add_keeper().expect("Failed to add keeeper");
    wait_for_keepers(
        log,
        &deployment,
        [1, 3, 4].map(KeeperId).into_iter().collect(),
    )
    .await
    .expect("failed to sync keepers");
    let samples = oximeter_test_utils::generate_test_samples(
        input.n_projects,
        input.n_instances,
        input.n_cpus,
        input.n_samples,
    );
    wait_for_insert(log, &client1, &samples)
        .await
        .expect("failed to insert samples at server1");
    wait_for_num_points(&log, &client2, samples.len() * 5)
        .await
        .expect("failed to get samples from client1");

    info!(log, "Cleaning up test");
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
    log: &Logger,
    client: &Client,
    n_samples: usize,
) -> anyhow::Result<()> {
    poll::wait_for_condition(
        || async {
            let oxql_res = client
                .oxql_query(
                    "get virtual_machine:cpu_busy | filter timestamp > @2000-01-01",
                    QueryAuthzScope::Fleet)
                .await
                .map_err(|_| {
                    poll::CondCheckError::<oximeter_db::Error>::NotYet
                })?;
            if total_points(&oxql_res) != n_samples {
                info!(
                    log,
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

// TODO: Use the function in the other package
/// Try to ping the server until it responds.
async fn wait_for_ping(log: &Logger, client: &Client) -> anyhow::Result<()> {
    poll::wait_for_condition(
        || async {
            client
                .ping()
                .await
                .map_err(|_| poll::CondCheckError::<oximeter_db::Error>::NotYet)
        },
        &Duration::from_millis(100),
        &Duration::from_secs(30),
    )
    .await
    .context("failed to ping ClickHouse server")?;
    info!(log, "ClickHouse server ready");
    Ok(())
}

/// Wait for insert to succeed after messing with keeper configuration
async fn wait_for_insert(
    log: &Logger,
    client: &Client,
    samples: &[Sample],
) -> anyhow::Result<()> {
    poll::wait_for_condition(
        || async {
            client
                .insert_samples(&samples)
                .await
                .map_err(|_| poll::CondCheckError::<oximeter_db::Error>::NotYet)
        },
        &Duration::from_millis(1000),
        &Duration::from_secs(60),
    )
    .await
    .context("failed to insert samples into ClickHouse server")?;
    info!(log, "inserted samples into clickhouse server");
    Ok(())
}
