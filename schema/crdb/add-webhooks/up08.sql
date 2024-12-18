-- Look up webhook messages in need of dispatching.
--
-- This is used by the message dispatcher when looking for messages to dispatch.
CREATE INDEX IF NOT EXISTS lookup_undispatched_webhook_msgs
ON omicron.public.webhook_msg (
    id, time_created
) WHERE time_dispatched IS NULL;
