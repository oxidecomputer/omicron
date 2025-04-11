WITH
  size_used_with_reservation
    AS (
      SELECT
        crucible_dataset.id AS crucible_dataset_id,
        sum(
          CASE
          WHEN block_size IS NULL THEN 0
          ELSE CASE
          WHEN reservation_percent = '25'
          THEN block_size * blocks_per_extent * extent_count / 4
          + block_size * blocks_per_extent * extent_count
          END
          END
        )
          AS reserved_size
      FROM
        crucible_dataset LEFT JOIN region ON crucible_dataset.id = region.dataset_id
      WHERE
        crucible_dataset.time_deleted IS NULL AND crucible_dataset.id = ANY ($1)
      GROUP BY
        crucible_dataset.id
    )
UPDATE
  crucible_dataset
SET
  size_used = size_used_with_reservation.reserved_size
FROM
  size_used_with_reservation
WHERE
  crucible_dataset.id = size_used_with_reservation.crucible_dataset_id
