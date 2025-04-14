// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of query to update crucible_dataset size_used after
//! hard-deleting regions

use crate::db::datastore::RunnableQueryNoReturn;
use crate::db::raw_query_builder::QueryBuilder;
use diesel::sql_types;
use uuid::Uuid;

/// Update the affected Crucible dataset rows after hard-deleting regions
pub fn dataset_update_query(
    dataset_ids: Vec<Uuid>,
) -> impl RunnableQueryNoReturn {
    let mut builder = QueryBuilder::new();

    builder.sql(
        "WITH
  size_used_with_reservation AS (
    SELECT
      crucible_dataset.id AS crucible_dataset_id,
      SUM(
        CASE
          WHEN block_size IS NULL THEN 0
          ELSE
            CASE
              WHEN reservation_percent = '25' THEN
                (block_size * blocks_per_extent * extent_count) / 4 +
                (block_size * blocks_per_extent * extent_count)
            END
        END
      ) AS reserved_size
    FROM crucible_dataset
    LEFT JOIN region ON crucible_dataset.id = region.dataset_id
    WHERE
      crucible_dataset.time_deleted IS NULL AND
      crucible_dataset.id = ANY (",
    );

    builder.param().bind::<sql_types::Array<sql_types::Uuid>, _>(dataset_ids);

    builder.sql(
        ")
    GROUP BY crucible_dataset.id
  )
  UPDATE crucible_dataset
  SET size_used = size_used_with_reservation.reserved_size
  FROM size_used_with_reservation
  WHERE crucible_dataset.id = size_used_with_reservation.crucible_dataset_id",
    );

    builder.query::<()>()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    // This test is a bit of a "change detector", but it's here to help with
    // debugging too. If you change this query, it can be useful to see exactly
    // how the output SQL has been altered.
    #[tokio::test]
    async fn expectorate_query() {
        let query =
            dataset_update_query(vec![Uuid::nil(), Uuid::nil(), Uuid::nil()]);

        expectorate_query_contents(
            &query,
            "tests/output/dataset_update_query.sql",
        )
        .await;
    }

    // Explain the possible forms of the SQL query to ensure that it
    // creates a valid SQL string.
    #[tokio::test]
    async fn explainable() {
        let logctx = dev::test_setup_log("explainable");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query =
            dataset_update_query(vec![Uuid::nil(), Uuid::nil(), Uuid::nil()]);

        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
