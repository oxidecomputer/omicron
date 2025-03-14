SELECT
  DISTINCT
  webhook_event.id,
  webhook_event.time_created,
  webhook_event.time_modified,
  webhook_event.time_dispatched,
  webhook_event.event_class,
  webhook_event.event,
  webhook_event.num_dispatched
FROM
  webhook_event INNER JOIN webhook_delivery AS delivery ON delivery.event_id = webhook_event.id
WHERE
  (webhook_event.event_class != $1 AND delivery.rx_id = $2)
  AND NOT
      (
        EXISTS(
          SELECT
            also_delivey.id
          FROM
            webhook_delivery AS also_delivey
          WHERE
            (also_delivey.event_id = webhook_event.id AND also_delivey.state != $3)
            AND also_delivey.trigger != $4
        )
      )
