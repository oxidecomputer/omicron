-- Index for looking up all currently in-flight webhook messages, and ordering
-- them by their creation times.
CREATE INDEX IF NOT EXISTS webhook_deliveries_in_flight
ON omicron.public.webhook_delivery (
    time_created, id
) WHERE
    time_completed IS NULL;
