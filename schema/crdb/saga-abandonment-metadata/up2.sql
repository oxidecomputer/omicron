ALTER TABLE omicron.public.saga
    ADD COLUMN IF NOT EXISTS time_abandoned TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reason_abandoned omicron.public.saga_reason_abandoned,
    ADD COLUMN IF NOT EXISTS abandon_information TEXT;
