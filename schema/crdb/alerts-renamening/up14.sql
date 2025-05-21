set
  local disallow_full_table_scans = off;

INSERT INTO omicron.public.alert (
    id,
    time_created,
    time_modified,
    alert_class,
    payload,
    time_dispatched,
    num_dispatched
)
SELECT
    id,
    time_created,
    time_modified,
    event_class::text::omicron.public.alert_class,
    event as payload,
    time_dispatched,
    num_dispatched
FROM omicron.public.webhook_event
-- this makes it idempotent
ON CONFLICT (id)
DO NOTHING;
