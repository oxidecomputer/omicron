ALTER TABLE omicron.public.saga
    ADD COLUMN IF NOT EXISTS abandon_time TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS abandon_reason omicron.public.saga_abandon_reason,
    ADD COLUMN IF NOT EXISTS abandon_comment TEXT;
