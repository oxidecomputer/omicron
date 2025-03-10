CREATE INDEX IF NOT EXISTS webhook_deliveries_in_flight
ON omicron.public.webhook_delivery (
    time_created, id
) WHERE
    time_completed IS NULL;
