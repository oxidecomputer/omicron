-- Look up all webhook receivers subscribed to an event class. This is used by
-- the dispatcher to determine who is interested in a particular event.
CREATE INDEX IF NOT EXISTS lookup_webhook_rxs_for_event_class
ON omicron.public.webhook_rx_subscription (
    event_class
);
