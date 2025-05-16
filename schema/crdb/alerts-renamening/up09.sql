set
  local disallow_full_table_scans = off;

INSERT INTO omicron.public.alert_subscription (
    rx_id,
    alert_class,
    glob,
    time_created
)
SELECT
    rx_id,
    event_class::text::omicron.public.alert_class,
    glob,
    time_created
FROM omicron.public.webhook_rx_subscription
-- this makes it idempotent
ON CONFLICT (rx_id, alert_class)
DO NOTHING;
