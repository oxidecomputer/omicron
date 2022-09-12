/*
 *
 * TAKEN FROM DBINIT.SQL
 *
 */

/* dbwipe.sql */
CREATE DATABASE IF NOT EXISTS omicron;
CREATE USER IF NOT EXISTS omicron;
ALTER DEFAULT PRIVILEGES FOR ROLE root REVOKE ALL ON TABLES FROM omicron;
DROP DATABASE IF EXISTS omicron;
DROP USER IF EXISTS omicron;

/* dbinit.sql */
CREATE DATABASE IF NOT EXISTS omicron;
CREATE USER IF NOT EXISTS omicron;
ALTER DEFAULT PRIVILEGES GRANT INSERT, SELECT, UPDATE, DELETE ON TABLES to omicron;

set disallow_full_table_scans = on;
set large_full_scan_rows = 0;

/*
 * ZPools of Storage, attached to Sleds.
 * Typically these are backed by a single physical disk.
 */
CREATE TABLE omicron.public.Zpool (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    rcgen INT NOT NULL,

    /* FK into the Sled table */
    sled_id UUID NOT NULL,

    /* TODO: Could also store physical disk FK here */

    total_size INT NOT NULL
);

CREATE TYPE omicron.public.dataset_kind AS ENUM (
  'crucible',
  'cockroach',
  'clickhouse'
);

/*
 * A dataset of allocated space within a zpool.
 */
CREATE TABLE omicron.public.Dataset (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    rcgen INT NOT NULL,

    /* FK into the Pool table */
    pool_id UUID NOT NULL,

    /* Contact information for the dataset */
    ip INET NOT NULL,
    port INT4 CHECK (port BETWEEN 0 AND 65535) NOT NULL,

    kind omicron.public.dataset_kind NOT NULL,

    /* An upper bound on the amount of space that might be in-use */
    size_used INT
);

/* Create an index on the size usage for Crucible's allocation */
CREATE INDEX on omicron.public.Dataset (
    size_used
) WHERE size_used IS NOT NULL AND time_deleted IS NULL AND kind = 'crucible';

/* Create an index on the size usage for any dataset */
CREATE INDEX on omicron.public.Dataset (
    size_used
) WHERE size_used IS NOT NULL AND time_deleted IS NULL;

CREATE TABLE omicron.public.Region (
    /* Identity metadata (asset) */
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    /* FK into the Dataset table */
    dataset_id UUID NOT NULL,

    /* FK into the volume table */
    volume_id UUID NOT NULL,

    /* Metadata describing the region */
    block_size INT NOT NULL,
    blocks_per_extent INT NOT NULL,
    extent_count INT NOT NULL
);

/*
 * Allow all regions belonging to a disk to be accessed quickly.
 */
CREATE INDEX on omicron.public.Region (
    volume_id
);

/*
 * Allow all regions belonging to a dataset to be accessed quickly.
 */
CREATE INDEX on omicron.public.Region (
    dataset_id
);

/*
 * A volume within Crucible
 */
CREATE TABLE omicron.public.volume (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    /* child resource generation number, per RFD 192 */
    rcgen INT NOT NULL,

    /*
     * A JSON document describing the construction of the volume, including all
     * sub volumes. This is what will be POSTed to propolis, and eventually
     * consumed by some Upstairs code to perform the volume creation. The Rust
     * type of this column should be Crucible::VolumeConstructionRequest.
     */
    data TEXT NOT NULL
);

/*
 *
 * TEST DATA
 *
 */

/* Make some zpools */
INSERT INTO omicron.public.Zpool (id, time_created, time_modified, time_deleted, rcgen, sled_id, total_size) VALUES
  (
    '11111111-aaaa-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    NULL,
    0,
    '22222222-aaaa-407e-aa8d-602ed78f38be',
    1000
  ),
  (
    '11111111-bbbb-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    NULL,
    0,
    '22222222-aaaa-407e-aa8d-602ed78f38be',
    1000
  ),
  (
    '11111111-cccc-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    NULL,
    0,
    '22222222-aaaa-407e-aa8d-602ed78f38be',
    1000
  );


/* Make some datasets */
INSERT INTO omicron.public.Dataset (id, time_created, time_modified, time_deleted, rcgen, pool_id, ip, port, kind, size_used) VALUES
  (
    '33333333-aaaa-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    NULL,
    0,
    '11111111-aaaa-407e-aa8d-602ed78f38be',
    '127.0.0.1',
    0,
    'crucible',
    50
  ),
  (
    '33333333-bbbb-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    NULL,
    0,
    '11111111-bbbb-407e-aa8d-602ed78f38be',
    '127.0.0.1',
    0,
    'crucible',
    60
  ),
  (
    '33333333-cccc-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    NULL,
    0,
    '11111111-cccc-407e-aa8d-602ed78f38be',
    '127.0.0.1',
    0,
    'crucible',
    70
  ),
  (
    '33333333-dddd-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    NULL,
    0,
    '11111111-cccc-407e-aa8d-602ed78f38be',
    '127.0.0.1',
    0,
    'cockroach',
    100
  );

/* Make a volume */
INSERT INTO omicron.public.volume (id, time_created, time_modified, time_deleted, rcgen, data) VALUES
  (
    '44444444-aaaa-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    NULL,
    0,
    'asdf'
  );

/* Create some regions */
/*
INSERT INTO omicron.public.Region (id, time_created, time_modified, dataset_id, volume_id, block_size, blocks_per_extent, extent_count) VALUES
  (
    '55555555-aaaa-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    '33333333-aaaa-407e-aa8d-602ed78f38be',
    '44444444-aaaa-407e-aa8d-602ed78f38be',
    50,
    1,
    1
  ),
  (
    '55555555-bbbb-407e-aa8d-602ed78f38be',
    TIMESTAMPTZ '2016-03-26',
    TIMESTAMPTZ '2016-03-26',
    '33333333-bbbb-407e-aa8d-602ed78f38be',
    '44444444-aaaa-407e-aa8d-602ed78f38be',
    50,
    1,
    1
  );
*/

/*
 *
 * CTE
 *
 */

WITH
  /*
   * For idempotency, identify if we've already completed the region allocation.
   */
  previously_allocated_regions AS (
    SELECT
      id, time_created, time_modified, dataset_id, volume_id, block_size, blocks_per_extent, extent_count
    FROM
      omicron.public.Region
    WHERE
      volume_id = '44444444-aaaa-407e-aa8d-602ed78f38be'
  ),

  /*
   * The crux of our allocation function.
   * Find all the Crucible datasets we'd like to use
   * as targets on which to allocate the new regions.
   */
  candidate_datasets AS (
    SELECT
      omicron.public.Dataset.id,
      omicron.public.Dataset.time_created,
      omicron.public.Dataset.time_modified,
      omicron.public.Dataset.time_deleted,
      omicron.public.Dataset.rcgen,
      omicron.public.Dataset.pool_id,
      omicron.public.Dataset.ip,
      omicron.public.Dataset.port,
      omicron.public.Dataset.kind,
      omicron.public.Dataset.size_used
    FROM
      omicron.public.Dataset
    WHERE
      size_used IS NOT NULL AND
      time_deleted IS NULL AND
      kind = 'crucible'
    ORDER BY size_used ASC
    LIMIT 2
  ),

  /*
   * The zpools which contain the target datasets.
   * Used later to ensure we have enough space.
   */
  candidate_zpools AS (
    SELECT
      omicron.public.Zpool.id
    FROM
      omicron.public.Zpool
    /*
     * We JOIN on candidate datasets to only pull the zpools pointed to
     * by our allocation datasets.
     */
    JOIN
      candidate_datasets
    ON
      candidate_datasets.pool_id = omicron.public.Zpool.id
  ),

  /*
   * Construct the regions we are trying to insert.
   */
  candidate_regions AS (
    SELECT
      gen_random_uuid() as id,
      now() as time_created,
      now() as time_modified,
      candidate_datasets.id as dataset_id,
      CAST('44444444-aaaa-407e-aa8d-602ed78f38be' AS UUID) as volume_id,
      50 as block_size,
      1 as blocks_per_extent,
      1 as extent_count
    FROM
      candidate_datasets
  ),

  /*
   * Identify the delta in size we're proposing to the datasets (and zpools).
   */
  proposed_dataset_size_changes AS (
    SELECT
      omicron.public.Dataset.pool_id,
      candidate_regions.dataset_id,
      omicron.public.Zpool.total_size as pool_total_size,
      candidate_regions.block_size *
        candidate_regions.blocks_per_extent *
        candidate_regions.extent_count as size_used_delta
    FROM
      candidate_regions
    LEFT JOIN
      omicron.public.Dataset
    ON
      omicron.public.Dataset.id = candidate_regions.dataset_id
    LEFT JOIN
      omicron.public.Zpool
    ON
      omicron.public.Zpool.id = omicron.public.Dataset.pool_id
  ),

  /*
   * Get the total size used on all datasets in the candidate zpools.
   * This is a necessary calculation to determine if the proposed regions
   * will fit or not.
   */
  previous_zpool_size_usage AS (
    SELECT
      omicron.public.Dataset.pool_id,
      SUM(omicron.public.Dataset.size_used) as previous_size_used
    FROM
      omicron.public.Dataset
    LEFT JOIN
      candidate_zpools
    ON
      omicron.public.Dataset.pool_id = candidate_zpools.id
    WHERE
      omicron.public.Dataset.size_used IS NOT NULL AND
      omicron.public.Dataset.time_deleted IS NULL
    GROUP BY
      pool_id
  ),

  /*
   * Compare the old space usage with the proposed changes,
   * and make a call on whether or not it'll fit.
   */
  proposed_zpool_size_usage AS (
    SELECT
      previous_size_used + size_used_delta <= pool_total_size as valid
    FROM
      previous_zpool_size_usage
    JOIN
      proposed_dataset_size_changes
    ON
      proposed_dataset_size_changes.pool_id = previous_zpool_size_usage.pool_id
  ),

  /*
   * Make a single decision: Should we do the insert/update, or not?
   */
  do_insert AS (
    SELECT IF(
      /* Only insert if we have not previously allocated regions. */
      NOT(EXISTS(SELECT id from previously_allocated_regions)) AND
      /* Only insert if we found the necessary amount of regions. */
      (SELECT COUNT(*) FROM candidate_regions) >= 2 AND
      /* Only insert if all the proposed size changes fit. */
      (
        SELECT(BOOL_AND(valid))
        FROM proposed_zpool_size_usage
      ),
      TRUE,
      FALSE
    )
  ),

  /*
   * (if do_insert is true) Add the regions.
   */
  inserted_regions AS (
    INSERT INTO omicron.public.Region
      (
        SELECT * FROM candidate_regions
        WHERE (SELECT * FROM do_insert)
      )
    RETURNING *
  ),

  /*
   * (if do_insert is true) Update the datasets.
   */
  updated_datasets AS (
    UPDATE omicron.public.Dataset SET
      size_used =
        (
          size_used +
            (
              SELECT size_used_delta FROM proposed_dataset_size_changes
              WHERE proposed_dataset_size_changes.dataset_id = id
            )
        )
    WHERE
      (id IN (SELECT dataset_id FROM proposed_dataset_size_changes)) AND
      (SELECT * FROM do_insert)
    RETURNING *
  )
SELECT * FROM
  /* Return all old regions, if any existed */
  (SELECT * FROM previously_allocated_regions)
  /* Then return all new regions, if any were allocated */
  FULL OUTER JOIN (SELECT * FROM inserted_regions) ON TRUE;

set disallow_full_table_scans = off;
SELECT * FROM omicron.public.Region;
SELECT * FROM omicron.public.Dataset;
