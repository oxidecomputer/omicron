WITH
  chosen_oximeter
    AS (
      SELECT id AS oximeter_id FROM oximeter WHERE time_expunged IS NULL ORDER BY random() LIMIT 1
    )
INSERT
INTO
  metric_producer (id, time_created, time_modified, kind, ip, port, "interval", oximeter_id)
SELECT
  $1, now(), now(), $2, $3, $4, $5, oximeter_id
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
    "interval" = excluded.interval
