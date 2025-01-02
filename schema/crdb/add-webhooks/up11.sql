-- Index for looking up all currently in-flight webhook deliveries, and ordering
-- them by their creation times.
CREATE INDEX IF NOT EXISTS webhook_deliverey_in_flight
ON omicron.public.webhook_delivery (
    time_created, id
) WHERE
    time_completed IS NULL;
