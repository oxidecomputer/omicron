-- Ensure that initial delivery attempts (nexus-dispatched) are unique to avoid
-- duplicate work when an alert is dispatched. For deliveries created by calls
-- to the webhook event resend API, we don't enforce this constraint, to allow
-- re-delivery to be triggered multiple times.
CREATE UNIQUE INDEX IF NOT EXISTS one_webhook_event_dispatch_per_rx
ON omicron.public.webhook_delivery (
    alert_id, rx_id
)
WHERE
    triggered_by = 'alert';
