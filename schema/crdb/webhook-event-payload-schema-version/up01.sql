ALTER TABLE omicron.public.webhook_event
ADD COLUMN IF NOT EXISTS payload_schema_version INT8 NOT NULL DEFAULT 1;
