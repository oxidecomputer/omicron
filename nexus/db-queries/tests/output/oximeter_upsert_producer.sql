WITH
  existing_oximeter
    AS (
      SELECT
        oximeter.id
      FROM
        metric_producer INNER JOIN oximeter ON metric_producer.oximeter_id = oximeter.id
      WHERE
        oximeter.time_expunged IS NULL AND metric_producer.id = $1
    ),
  random_oximeter
    AS (SELECT id FROM oximeter WHERE time_expunged IS NULL ORDER BY random() LIMIT 1),
  chosen_oximeter
    AS (
      SELECT
        COALESCE(existing_oximeter.id, random_oximeter.id) AS oximeter_id
      FROM
        random_oximeter LEFT JOIN existing_oximeter ON true
    ),
  inserted_producer
    AS (
      INSERT
      INTO
        metric_producer (id, time_created, time_modified, kind, ip, port, "interval", oximeter_id)
      SELECT
        $2, now(), now(), $3, $4, $5, $6, oximeter_id
      FROM
        chosen_oximeter
      ON CONFLICT
        (id)
      DO
        UPDATE SET
          time_modified = now(),
          kind = excluded.kind,
          ip = excluded.ip,
          port = excluded.port,
          "interval" = excluded.interval,
          oximeter_id = excluded.oximeter_id
      RETURNING
        oximeter_id
    )
SELECT
  oximeter.id,
  oximeter.time_created,
  oximeter.time_modified,
  oximeter.time_expunged,
  oximeter.ip,
  oximeter.port
FROM
  oximeter INNER JOIN inserted_producer ON oximeter.id = inserted_producer.oximeter_id
WHERE
  oximeter.time_expunged IS NULL
