SELECT
  DISTINCT
  alert.id,
  alert.time_created,
  alert.time_modified,
  alert.alert_class,
  alert.payload,
  alert.time_dispatched,
  alert.num_dispatched,
  alert.case_id,
  alert.alert_version
FROM
  alert INNER JOIN webhook_delivery AS delivery ON delivery.alert_id = alert.id
WHERE
  (alert.alert_class != $1 AND delivery.rx_id = $2)
  AND NOT
      (
        EXISTS(
          SELECT
            also_delivery.id
          FROM
            webhook_delivery AS also_delivery
          WHERE
            (also_delivery.alert_id = alert.id AND also_delivery.state != $3)
            AND also_delivery.triggered_by != $4
        )
      )
