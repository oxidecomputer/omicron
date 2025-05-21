set
  local disallow_full_table_scans = off;

INSERT INTO omicron.public.webhook_delivery (
    id,
    alert_id,
    rx_id,
    triggered_by,
    attempts,
    time_created,
    time_completed,
    state,
    deliverator_id,
    time_leased
)
SELECT
    id,
    webhook_delivery_old.event_id as alert_id,
    rx_id,
    CASE webhook_delivery_old.triggered_by
        WHEN 'event' THEN 'alert'::omicron.public.alert_delivery_trigger
        WHEN 'resend' THEN 'resend'::omicron.public.alert_delivery_trigger
        WHEN 'probe' THEN 'probe'::omicron.public.alert_delivery_trigger
    END,
    attempts,
    time_created,
    time_completed,
    webhook_delivery_old.state::text::omicron.public.alert_delivery_state,
    deliverator_id,
    time_leased
FROM omicron.public.webhook_delivery_old
-- this makes it idempotent
ON CONFLICT (id)
DO NOTHING;
