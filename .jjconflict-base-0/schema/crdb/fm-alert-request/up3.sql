ALTER TABLE omicron.public.alert
    ADD COLUMN IF NOT EXISTS case_id UUID;
