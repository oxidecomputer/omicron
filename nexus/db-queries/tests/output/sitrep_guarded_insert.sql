WITH
  latest_history
    AS MATERIALIZED (
      SELECT sitrep_id FROM omicron.public.fm_sitrep_history ORDER BY version DESC LIMIT 1
    ),
  stale_guard
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            (
              (
                SELECT
                  s.dummy_generation
                FROM
                  omicron.public.fm_sitrep AS s JOIN latest_history AS lh ON lh.sitrep_id = s.id
              )
              = $1
            ),
            'TRUE',
            'stale generation'
          )
            AS BOOL
        )
          AS ok
    ),
  prior_marker_guard
    AS MATERIALIZED (
      SELECT
        CAST(
          IF(
            NOT EXISTS(SELECT 1 FROM test_schema.dummy_marker WHERE dummy_id = $2),
            'TRUE',
            'marker already exists'
          )
            AS BOOL
        )
          AS ok
    ),
  new_resource
    AS (
      INSERT
      INTO
        test_schema.dummy_resource (id, name)
      VALUES
        ($3, $4)
      ON CONFLICT
        (id)
      DO
        NOTHING
      RETURNING
        test_schema.dummy_resource.id, test_schema.dummy_resource.name
    ),
  new_marker
    AS (
      INSERT
      INTO
        test_schema.dummy_marker (dummy_id, created_at_generation)
      SELECT
        $5, $6
      WHERE
        EXISTS(SELECT 1 FROM new_resource)
      ON CONFLICT
        (dummy_id)
      DO
        NOTHING
      RETURNING
        dummy_id
    )
SELECT
  nr.*
FROM
  new_resource AS nr
