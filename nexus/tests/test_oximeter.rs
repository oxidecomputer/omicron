//! Integration tests for oximeter collectors and producers.

pub mod common;

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
