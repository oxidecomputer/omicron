// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for Oximeter collectors and producers.

use crate::db::raw_query_builder::{QueryBuilder, TypedSqlQuery};
use diesel::sql_types;
use uuid::Uuid;

/// For a given Oximeter instance (which is presumably no longer running),
/// reassign any collectors assigned to it to a different Oximeter. Each
/// assignment is randomly chosen from among the non-deleted Oximeter instances
/// recorded in the `oximeter` table.
pub fn reassign_producers_query(oximeter_id: Uuid) -> TypedSqlQuery<()> {
    let builder = QueryBuilder::new();

    // Find all non-deleted Oximeter instances.
    let builder = builder.sql(
        "\
        WITH available_oximeters AS ( \
          SELECT ARRAY( \
            SELECT id FROM oximeter WHERE time_deleted IS NULL
          ) AS ids \
        ), ",
    );

    // Create a mapping of producer ID <-> new, random, non-deleted Oximeter ID
    // for every producer assigned to `oximeter_id`. If the `ids` array from the
    // previous expression is empty, every `new_id` column in this expression
    // will be NULL. We'll catch that in the update below.
    let builder = builder
        .sql(
            "\
            new_assignments AS ( \
              SELECT
                metric_producer.id AS producer_id,
                ids[1 + floor(random() * array_length(ids, 1)::float)::int]
                  AS new_id
              FROM metric_producer
              LEFT JOIN available_oximeters ON true
              WHERE oximeter_id = ",
        )
        .param()
        .sql(")")
        .bind::<sql_types::Uuid, _>(oximeter_id);

    // Actually perform the update. If the `new_id` column from the previous
    // step is `NULL` (because there aren't any non-deleted Oximeter instances),
    // this will fail the `NOT NULL` constraint on the oximeter_id column.
    let builder = builder
        .sql(
            "\
            UPDATE metric_producer SET oximeter_id = ( \
              SELECT new_id FROM new_assignments \
              WHERE new_assignments.producer_id = metric_producer.id \
            ) WHERE oximeter_id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(oximeter_id);

    builder.query()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    // This test is a bit of a "change detector", but it's here to help with
    // debugging too. If you change this query, it can be useful to see exactly
    // how the output SQL has been altered.
    #[tokio::test]
    async fn expectorate_query() {
        let oximeter_id = Uuid::nil();

        let query = reassign_producers_query(oximeter_id);

        expectorate_query_contents(
            &query,
            "tests/output/oximeter_reassign_producers.sql",
        )
        .await;
    }

    // Explain the SQL query to ensure that it creates a valid SQL string.
    #[tokio::test]
    async fn explainable() {
        let logctx = dev::test_setup_log("explainable");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = crate::db::Pool::new_single_host(&logctx.log, &cfg);
        let conn = pool.claim().await.unwrap();

        let oximeter_id = Uuid::nil();

        let query = reassign_producers_query(oximeter_id);
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
