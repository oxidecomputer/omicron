WITH
  old_regions
    AS (
      SELECT
        region.id,
        region.time_created,
        region.time_modified,
        region.dataset_id,
        region.volume_id,
        region.block_size,
        region.blocks_per_extent,
        region.extent_count,
        region.port,
        region.read_only,
        region.deleting,
        region.reservation_percent
      FROM
        region
      WHERE
        region.volume_id = $1
    ),
  old_zpool_usage
    AS (
      SELECT
        crucible_dataset.pool_id, sum(crucible_dataset.size_used) AS size_used
      FROM
        crucible_dataset
      WHERE
        (crucible_dataset.size_used IS NOT NULL) AND (crucible_dataset.time_deleted IS NULL)
      GROUP BY
        crucible_dataset.pool_id
    ),
  existing_zpools
    AS (
      (
        SELECT
          crucible_dataset.pool_id
        FROM
          crucible_dataset INNER JOIN old_regions ON old_regions.dataset_id = crucible_dataset.id
      )
      UNION
        (
          SELECT
            crucible_dataset.pool_id
          FROM
            crucible_dataset
            INNER JOIN region_snapshot ON region_snapshot.dataset_id = crucible_dataset.id
          WHERE
            region_snapshot.snapshot_id = $2
        )
    ),
  candidate_zpools
    AS (
      SELECT
        old_zpool_usage.pool_id
      FROM
        old_zpool_usage
        INNER JOIN (zpool INNER JOIN sled ON zpool.sled_id = sled.id) ON
            zpool.id = old_zpool_usage.pool_id
        INNER JOIN physical_disk ON zpool.physical_disk_id = physical_disk.id
        INNER JOIN crucible_dataset ON crucible_dataset.pool_id = zpool.id
      WHERE
        (old_zpool_usage.size_used + $3 + zpool.control_plane_storage_buffer)
        <= (
            SELECT
              total_size
            FROM
              omicron.public.inv_zpool
            WHERE
              inv_zpool.id = old_zpool_usage.pool_id
            ORDER BY
              inv_zpool.time_collected DESC
            LIMIT
              1
          )
        AND sled.sled_policy = 'in_service'
        AND sled.sled_state = 'active'
        AND physical_disk.disk_policy = 'in_service'
        AND physical_disk.disk_state = 'active'
        AND NOT (zpool.id = ANY (SELECT existing_zpools.pool_id FROM existing_zpools))
        AND (crucible_dataset.time_deleted IS NULL)
        AND crucible_dataset.no_provision = false
    ),
  candidate_datasets
    AS (
      SELECT
        DISTINCT ON (crucible_dataset.pool_id) crucible_dataset.id, crucible_dataset.pool_id
      FROM
        crucible_dataset
        INNER JOIN candidate_zpools ON crucible_dataset.pool_id = candidate_zpools.pool_id
      WHERE
        (crucible_dataset.time_deleted IS NULL) AND crucible_dataset.no_provision = false
      ORDER BY
        crucible_dataset.pool_id, md5(CAST(crucible_dataset.id AS BYTES) || $4)
    ),
  shuffled_candidate_datasets
    AS (
      SELECT
        candidate_datasets.id, candidate_datasets.pool_id
      FROM
        candidate_datasets
      ORDER BY
        md5(CAST(candidate_datasets.id AS BYTES) || $5)
      LIMIT
        $6
    ),
  candidate_regions
    AS (
      SELECT
        gen_random_uuid() AS id,
        now() AS time_created,
        now() AS time_modified,
        shuffled_candidate_datasets.id AS dataset_id,
        $7 AS volume_id,
        $8 AS block_size,
        $9 AS blocks_per_extent,
        $10 AS extent_count,
        NULL AS port,
        $11 AS read_only,
        false AS deleting,
        $12 AS reservation_percent
      FROM
        shuffled_candidate_datasets
      LIMIT
        $13 - (SELECT count(*) FROM old_regions)
    ),
  proposed_dataset_changes
    AS (
      SELECT
        candidate_regions.dataset_id AS id,
        crucible_dataset.pool_id AS pool_id,
        $14 AS size_used_delta
      FROM
        candidate_regions
        INNER JOIN crucible_dataset ON crucible_dataset.id = candidate_regions.dataset_id
    ),
  do_insert
    AS (
      SELECT
        (
          (
            (SELECT count(*) FROM old_regions LIMIT 1) < $15
            AND CAST(
                IF(
                  (
                    (
                      (
                        (SELECT count(*) FROM candidate_zpools LIMIT 1)
                        + (SELECT count(*) FROM existing_zpools LIMIT 1)
                      )
                    )
                    >= $16
                  ),
                  'TRUE',
                  'Not enough space'
                )
                  AS BOOL
              )
          )
          AND CAST(
              IF(
                (
                  (
                    (
                      (SELECT count(*) FROM candidate_regions LIMIT 1)
                      + (SELECT count(*) FROM old_regions LIMIT 1)
                    )
                  )
                  >= $17
                ),
                'TRUE',
                'Not enough datasets'
              )
                AS BOOL
            )
        )
        AND CAST(
            IF(
              (
                (
                  (
                    SELECT
                      count(DISTINCT pool_id)
                    FROM
                      (
                        (
                          SELECT
                            crucible_dataset.pool_id
                          FROM
                            candidate_regions
                            INNER JOIN crucible_dataset ON
                                candidate_regions.dataset_id = crucible_dataset.id
                        )
                        UNION
                          (
                            SELECT
                              crucible_dataset.pool_id
                            FROM
                              old_regions
                              INNER JOIN crucible_dataset ON
                                  old_regions.dataset_id = crucible_dataset.id
                          )
                      )
                    LIMIT
                      1
                  )
                )
                >= $18
              ),
              'TRUE',
              'Not enough unique zpools selected'
            )
              AS BOOL
          )
          AS insert
    ),
  inserted_regions
    AS (
      INSERT
      INTO
        region
          (
            id,
            time_created,
            time_modified,
            dataset_id,
            volume_id,
            block_size,
            blocks_per_extent,
            extent_count,
            port,
            read_only,
            deleting,
            reservation_percent
          )
      SELECT
        candidate_regions.id,
        candidate_regions.time_created,
        candidate_regions.time_modified,
        candidate_regions.dataset_id,
        candidate_regions.volume_id,
        candidate_regions.block_size,
        candidate_regions.blocks_per_extent,
        candidate_regions.extent_count,
        candidate_regions.port,
        candidate_regions.read_only,
        candidate_regions.deleting,
        candidate_regions.reservation_percent
      FROM
        candidate_regions
      WHERE
        (SELECT do_insert.insert FROM do_insert LIMIT 1)
      RETURNING
        region.id,
        region.time_created,
        region.time_modified,
        region.dataset_id,
        region.volume_id,
        region.block_size,
        region.blocks_per_extent,
        region.extent_count,
        region.port,
        region.read_only,
        region.deleting,
        region.reservation_percent
    ),
  updated_datasets
    AS (
      UPDATE
        crucible_dataset
      SET
        size_used
          = crucible_dataset.size_used
          + (
              SELECT
                proposed_dataset_changes.size_used_delta
              FROM
                proposed_dataset_changes
              WHERE
                proposed_dataset_changes.id = crucible_dataset.id
              LIMIT
                1
            )
      WHERE
        crucible_dataset.id = ANY (SELECT proposed_dataset_changes.id FROM proposed_dataset_changes)
        AND (SELECT do_insert.insert FROM do_insert LIMIT 1)
      RETURNING
        crucible_dataset.id,
        crucible_dataset.time_created,
        crucible_dataset.time_modified,
        crucible_dataset.time_deleted,
        crucible_dataset.rcgen,
        crucible_dataset.pool_id,
        crucible_dataset.ip,
        crucible_dataset.port,
        crucible_dataset.size_used,
        crucible_dataset.no_provision
    )
(
  SELECT
    crucible_dataset.id,
    crucible_dataset.time_created,
    crucible_dataset.time_modified,
    crucible_dataset.time_deleted,
    crucible_dataset.rcgen,
    crucible_dataset.pool_id,
    crucible_dataset.ip,
    crucible_dataset.port,
    crucible_dataset.size_used,
    crucible_dataset.no_provision,
    old_regions.id,
    old_regions.time_created,
    old_regions.time_modified,
    old_regions.dataset_id,
    old_regions.volume_id,
    old_regions.block_size,
    old_regions.blocks_per_extent,
    old_regions.extent_count,
    old_regions.port,
    old_regions.read_only,
    old_regions.deleting,
    old_regions.reservation_percent
  FROM
    old_regions INNER JOIN crucible_dataset ON old_regions.dataset_id = crucible_dataset.id
)
UNION
  (
    SELECT
      updated_datasets.id,
      updated_datasets.time_created,
      updated_datasets.time_modified,
      updated_datasets.time_deleted,
      updated_datasets.rcgen,
      updated_datasets.pool_id,
      updated_datasets.ip,
      updated_datasets.port,
      updated_datasets.size_used,
      updated_datasets.no_provision,
      inserted_regions.id,
      inserted_regions.time_created,
      inserted_regions.time_modified,
      inserted_regions.dataset_id,
      inserted_regions.volume_id,
      inserted_regions.block_size,
      inserted_regions.blocks_per_extent,
      inserted_regions.extent_count,
      inserted_regions.port,
      inserted_regions.read_only,
      inserted_regions.deleting,
      inserted_regions.reservation_percent
    FROM
      inserted_regions
      INNER JOIN updated_datasets ON inserted_regions.dataset_id = updated_datasets.id
  )
