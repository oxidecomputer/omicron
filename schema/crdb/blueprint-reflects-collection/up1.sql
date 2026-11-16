ALTER TABLE omicron.public.blueprint
    ADD COLUMN IF NOT EXISTS reflects_collection_id UUID,
    ADD COLUMN IF NOT EXISTS reflects_collection_time_started TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reflects_collection_time_done TIMESTAMPTZ;
