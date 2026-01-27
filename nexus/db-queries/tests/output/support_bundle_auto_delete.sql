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
  deletion_calc
    AS (
      SELECT
        d.total AS total_datasets,
        u.used AS used_datasets,
        a.active AS active_bundles,
        greatest(0, ceil(d.total * c.target_free_percent / 100.0)::INT8 - (d.total - u.used))
          AS autodeletion_count,
        greatest(0, a.active - ceil(d.total * c.min_keep_percent / 100.0)::INT8) AS max_deletable
      FROM
        dataset_count AS d
        CROSS JOIN used_count AS u
        CROSS JOIN active_count AS a
        CROSS JOIN config AS c
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
        (SELECT least(autodeletion_count, max_deletable) FROM deletion_calc)
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
