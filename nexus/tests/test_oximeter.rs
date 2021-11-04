//! Integration tests for oximeter collectors and producers.

pub mod common;

use dropshot::test_util::LogContext;
use omicron_test_utils::dev;
use std::net;
use uuid::Uuid;

#[tokio::test]
async fn test_oximeter_database_records() {
    let context = common::test_setup("test_oximeter_database_records").await;
    let db = &context.database;

    // Get a handle to the DB, for various tests
    let conn = db.connect().await.unwrap();

    // Verify that the Oximeter instance lives in the DB.
    let result = conn
        .query("SELECT * FROM omicron.public.Oximeter;", &[])
        .await
        .unwrap();
    assert_eq!(
        result.len(),
        1,
        "Expected a single Oximeter instance in the database"
    );
    let actual_id = result[0].get::<&str, Uuid>("id");
    assert_eq!(
        actual_id,
        common::OXIMETER_UUID.parse().unwrap(),
        "Oximeter ID does not match the ID returned from the database"
    );

    // Verify that the producer lives in the DB.
    let result = conn
        .query("SELECT * FROM omicron.public.MetricProducer;", &[])
        .await
        .unwrap();
    assert_eq!(
        result.len(),
        1,
        "Expected a single metric producer instance in the database"
    );
    let actual_id = result[0].get::<&str, Uuid>("id");
    assert_eq!(
        actual_id,
        common::PRODUCER_UUID.parse().unwrap(),
        "Producer ID does not match the ID returned from the database"
    );
    let actual_oximeter_id = result[0].get::<&str, Uuid>("oximeter_id");
    assert_eq!(
        actual_oximeter_id,
        common::OXIMETER_UUID.parse().unwrap(),
        "Producer's oximeter ID returned from the database does not match the expected ID"
    );

    context.teardown().await;
}

#[tokio::test]
async fn test_oximeter_reregistration() {
    let test_name = "test_oximeter_reregistration";
    let mut config = common::load_test_config();
    let logctx = LogContext::new(test_name, &config.log);
    let log = &logctx.log;
    let rack_id = Uuid::parse_str(crate::common::RACK_UUID).unwrap();

    // Start databases
    let database = dev::test_setup_database(log).await;
    let clickhouse = dev::clickhouse::ClickHouseInstance::new(0).await.unwrap();
    config.database.url = database.pg_config().clone();
    let nexus =
        omicron_nexus::Server::start(&config, &rack_id, log).await.unwrap();

    // Set up an Oximeter collector server
    let collector_id = Uuid::parse_str(crate::common::OXIMETER_UUID).unwrap();
    let oximeter = common::start_oximeter(
        nexus.http_server_internal.local_addr(),
        clickhouse.port(),
        collector_id,
    )
    .await
    .unwrap();

    // Get a handle to the DB, for various tests
    let conn = database.connect().await.unwrap();

    // ClickHouse client for verifying collection.
    let ch_address =
        net::SocketAddrV6::new("::1".parse().unwrap(), clickhouse.port(), 0, 0);
    let client =
        oximeter_db::Client::new(ch_address.into(), log.clone()).await.unwrap();

    // Create a metric producer in a scope
    let producer_id = Uuid::parse_str(crate::common::PRODUCER_UUID).unwrap();
    let timeseries_name = "integration_target:integration_metric";
    let timeseries = {
        // Set up a test metric producer server
        let _producer = common::start_producer_server(
            nexus.http_server_internal.local_addr(),
            producer_id,
        )
        .await
        .unwrap();

        // Wait until the collector has started to pull and insert data
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // There should be some data in ClickHouse at this point.
        let data = client
            .filter_timeseries_with(timeseries_name, &[], None, None)
            .await
            .expect("Failed to retrieve a timeseries");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].timeseries_name, timeseries_name);
        data.into_iter().next().unwrap()
    };

    // At this point, the producer has been dropped. Oximeter will still be collecting, so we wait
    // a bit, and verify that there is no new data in ClickHouse.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let data = client
        .filter_timeseries_with(timeseries_name, &[], None, None)
        .await
        .expect("Failed to retrieve a timeseries");
    assert_eq!(data.len(), 1);
    assert_eq!(&data[0], &timeseries);

    // Restart the producer, and verify that we have _more_ data than before
    // Set up a test metric producer server
    let producer = common::start_producer_server(
        nexus.http_server_internal.local_addr(),
        producer_id,
    )
    .await
    .unwrap();

    // Wait until the collector has started to pull and insert data
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let new_timeseries = {
        let data = client
            .filter_timeseries_with(timeseries_name, &[], None, None)
            .await
            .expect("Failed to retrieve a timeseries");
        assert_eq!(data.len(), 1);
        let new_timeseries = data.into_iter().next().unwrap();

        assert_eq!(timeseries.target, new_timeseries.target);
        assert_eq!(timeseries.metric, new_timeseries.metric);
        assert!(
            new_timeseries.measurements.len() > timeseries.measurements.len()
        );
        new_timeseries
    };

    // Also verify that the producer's port is still correct, because we've updated it.
    let result = conn
        .query("SELECT * FROM omicron.public.MetricProducer;", &[])
        .await
        .unwrap();
    assert_eq!(result.len(), 1, "Expected a single producer in the database");
    let actual_port = result[0].get::<&str, i32>("port") as u16;
    assert_eq!(actual_port, producer.address().port());

    // Drop oximeter and verify that we've still got the same data, because there's no collector
    // running.
    let timeseries = new_timeseries;
    drop(oximeter);
    let new_timeseries = {
        let data = client
            .filter_timeseries_with(timeseries_name, &[], None, None)
            .await
            .expect("Failed to retrieve a timeseries");
        assert_eq!(data.len(), 1);
        let new_timeseries = data.into_iter().next().unwrap();
        new_timeseries
    };
    assert_eq!(timeseries, new_timeseries);

    // Restart oximeter again, and verify that we have even more new data.
    let _oximeter = common::start_oximeter(
        nexus.http_server_internal.local_addr(),
        clickhouse.port(),
        collector_id,
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let new_timeseries = {
        let data = client
            .filter_timeseries_with(timeseries_name, &[], None, None)
            .await
            .expect("Failed to retrieve a timeseries");
        assert_eq!(data.len(), 1);
        let new_timeseries = data.into_iter().next().unwrap();
        new_timeseries
    };
    assert!(new_timeseries.measurements.len() > timeseries.measurements.len());
    assert_eq!(
        new_timeseries.measurements[..timeseries.measurements.len()],
        timeseries.measurements
    );
}
