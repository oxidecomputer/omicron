WITH
  available_oximeters AS (SELECT ARRAY (SELECT id FROM oximeter WHERE time_deleted IS NULL) AS ids),
  new_assignments
    AS (
      SELECT
        metric_producer.id AS producer_id,
        ids[1 + floor(random() * array_length(ids, 1)::FLOAT8)::INT8] AS new_id
      FROM
        metric_producer LEFT JOIN available_oximeters ON true
      WHERE
        oximeter_id = $1
    )
UPDATE
  metric_producer
SET
  oximeter_id
    = (SELECT new_id FROM new_assignments WHERE new_assignments.producer_id = metric_producer.id)
WHERE
  oximeter_id = $2
