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
      crucible_dataset.id IN (",
    );

    for (idx, dataset_id) in dataset_ids.into_iter().enumerate() {
        if idx != 0 {
            builder.sql(",");
        }
        builder.param().bind::<sql_types::Uuid, _>(dataset_id);
    }

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
