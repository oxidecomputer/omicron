CREATE INDEX IF NOT EXISTS lookup_webhook_event_globs_by_schema_version
ON omicron.public.webhook_rx_event_glob (schema_version);
