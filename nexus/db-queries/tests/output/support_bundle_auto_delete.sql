WITH
  config
    AS (
      SELECT target_free_percent, min_keep_percent FROM support_bundle_config WHERE singleton = true
    ),
  dataset_count
    AS (SELECT count(*) AS total FROM rendezvous_debug_dataset WHERE time_tombstoned IS NULL),
  used_count
    AS (SELECT count(*) AS used FROM support_bundle WHERE state IN ('collecting', 'active')),
  active_count AS (SELECT count(*) AS active FROM support_bundle WHERE state = 'active'),
  thresholds
    AS (
      SELECT
        ceil(
          (SELECT total FROM dataset_count) * (SELECT target_free_percent FROM config) / 100.0
        )::INT8
          AS target_free,
        ceil(
          (SELECT total FROM dataset_count) * (SELECT min_keep_percent FROM config) / 100.0
        )::INT8
          AS min_keep
    ),
  deletion_calc
    AS (
      SELECT
        (SELECT total FROM dataset_count) AS total_datasets,
        (SELECT used FROM used_count) AS used_datasets,
        (SELECT active FROM active_count) AS active_bundles,
        greatest(
          0,
          (SELECT target_free FROM thresholds)
          - ((SELECT total FROM dataset_count) - (SELECT used FROM used_count))
        )
          AS bundles_needed,
        greatest(0, (SELECT active FROM active_count) - (SELECT min_keep FROM thresholds))
          AS max_deletable
    ),
  candidates
    AS (
      SELECT
        id
      FROM
        support_bundle
      WHERE
        state = 'active'
      ORDER BY
        time_created ASC, id ASC
      LIMIT
        (SELECT least(bundles_needed, max_deletable) FROM deletion_calc)
    ),
  deleted
    AS (
      UPDATE
        support_bundle
      SET
        state = 'destroying'
      WHERE
        id IN (SELECT id FROM candidates) AND state = 'active'
      RETURNING
        id
    )
SELECT
  (SELECT total_datasets FROM deletion_calc),
  (SELECT used_datasets FROM deletion_calc),
  (SELECT active_bundles FROM deletion_calc),
  ARRAY (SELECT id FROM deleted) AS deleted_ids
