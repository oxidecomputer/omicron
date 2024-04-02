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
        region.extent_count
      FROM
        region
      WHERE
        region.volume_id = $1
    ),
  old_zpool_usage
    AS (
      SELECT
        dataset.pool_id, sum(dataset.size_used) AS size_used
      FROM
        dataset
      WHERE
        (dataset.size_used IS NOT NULL) AND (dataset.time_deleted IS NULL)
      GROUP BY
        dataset.pool_id
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
      WHERE
        (old_zpool_usage.size_used + $2)
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
    ),
  candidate_datasets
    AS (
      SELECT
        DISTINCT ON (dataset.pool_id) dataset.id, dataset.pool_id
      FROM
        dataset INNER JOIN candidate_zpools ON dataset.pool_id = candidate_zpools.pool_id
      WHERE
        ((dataset.time_deleted IS NULL) AND (dataset.size_used IS NOT NULL))
        AND dataset.kind = 'crucible'
      ORDER BY
        dataset.pool_id, md5(CAST(dataset.id AS BYTES) || $3)
    ),
  shuffled_candidate_datasets
    AS (
      SELECT
        candidate_datasets.id, candidate_datasets.pool_id
      FROM
        candidate_datasets
      ORDER BY
        md5(CAST(candidate_datasets.id AS BYTES) || $4)
      LIMIT
        $5
    ),
  candidate_regions
    AS (
      SELECT
        gen_random_uuid() AS id,
        now() AS time_created,
        now() AS time_modified,
        shuffled_candidate_datasets.id AS dataset_id,
        $6 AS volume_id,
        $7 AS block_size,
        $8 AS blocks_per_extent,
        $9 AS extent_count
      FROM
        shuffled_candidate_datasets
    ),
  proposed_dataset_changes
    AS (
      SELECT
        candidate_regions.dataset_id AS id,
        dataset.pool_id AS pool_id,
        candidate_regions.block_size
        * candidate_regions.blocks_per_extent
        * candidate_regions.extent_count
          AS size_used_delta
      FROM
        candidate_regions INNER JOIN dataset ON dataset.id = candidate_regions.dataset_id
    ),
  do_insert
    AS (
      SELECT
        (
          (
            (SELECT count(*) FROM old_regions LIMIT 1) < $10
            AND CAST(
                IF(
                  ((SELECT count(*) FROM candidate_zpools LIMIT 1) >= $11),
                  'TRUE',
                  'Not enough space'
                )
                  AS BOOL
              )
          )
          AND CAST(
              IF(
                ((SELECT count(*) FROM candidate_regions LIMIT 1) >= $12),
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
                  SELECT
                    count(DISTINCT dataset.pool_id)
                  FROM
                    candidate_regions
                    INNER JOIN dataset ON candidate_regions.dataset_id = dataset.id
                  LIMIT
                    1
                )
                >= $13
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
            extent_count
          )
      SELECT
        candidate_regions.id,
        candidate_regions.time_created,
        candidate_regions.time_modified,
        candidate_regions.dataset_id,
        candidate_regions.volume_id,
        candidate_regions.block_size,
        candidate_regions.blocks_per_extent,
        candidate_regions.extent_count
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
        region.extent_count
    ),
  updated_datasets
    AS (
      UPDATE
        dataset
      SET
        size_used
          = dataset.size_used
          + (
              SELECT
                proposed_dataset_changes.size_used_delta
              FROM
                proposed_dataset_changes
              WHERE
                proposed_dataset_changes.id = dataset.id
              LIMIT
                1
            )
      WHERE
        dataset.id = ANY (SELECT proposed_dataset_changes.id FROM proposed_dataset_changes)
        AND (SELECT do_insert.insert FROM do_insert LIMIT 1)
      RETURNING
        dataset.id,
        dataset.time_created,
        dataset.time_modified,
        dataset.time_deleted,
        dataset.rcgen,
        dataset.pool_id,
        dataset.ip,
        dataset.port,
        dataset.kind,
        dataset.size_used
    )
(
  SELECT
    dataset.id,
    dataset.time_created,
    dataset.time_modified,
    dataset.time_deleted,
    dataset.rcgen,
    dataset.pool_id,
    dataset.ip,
    dataset.port,
    dataset.kind,
    dataset.size_used,
    old_regions.id,
    old_regions.time_created,
    old_regions.time_modified,
    old_regions.dataset_id,
    old_regions.volume_id,
    old_regions.block_size,
    old_regions.blocks_per_extent,
    old_regions.extent_count
  FROM
    old_regions INNER JOIN dataset ON old_regions.dataset_id = dataset.id
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
      updated_datasets.kind,
      updated_datasets.size_used,
      inserted_regions.id,
      inserted_regions.time_created,
      inserted_regions.time_modified,
      inserted_regions.dataset_id,
      inserted_regions.volume_id,
      inserted_regions.block_size,
      inserted_regions.blocks_per_extent,
      inserted_regions.extent_count
    FROM
      inserted_regions
      INNER JOIN updated_datasets ON inserted_regions.dataset_id = updated_datasets.id
  )
