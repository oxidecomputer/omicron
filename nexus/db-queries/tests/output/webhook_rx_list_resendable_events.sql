SELECT
  DISTINCT
  alert.id,
  alert.time_created,
  alert.time_modified,
  alert.time_dispatched,
  alert.event_class,
  alert.event,
  alert.num_dispatched
FROM
  alert INNER JOIN webhook_delivery AS delivery ON delivery.event_id = alert.id
WHERE
  (alert.event_class != $1 AND delivery.rx_id = $2)
  AND NOT
      (
        EXISTS(
          SELECT
            also_delivey.id
          FROM
            webhook_delivery AS also_delivey
          WHERE
            (also_delivey.event_id = alert.id AND also_delivey.state != $3)
            AND also_delivey.triggered_by != $4
        )
      )
