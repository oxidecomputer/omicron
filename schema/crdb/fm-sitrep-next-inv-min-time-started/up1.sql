ALTER TABLE omicron.public.fm_sitrep
    ADD COLUMN IF NOT EXISTS next_inv_min_time_started TIMESTAMPTZ;
