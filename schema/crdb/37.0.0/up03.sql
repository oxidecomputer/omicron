-- Modify the existing sled table to add the columns as required.
ALTER TABLE omicron.public.sled
    -- Nullable for now -- we're going to set the data in sled_policy in the
    -- next migration statement.
    ADD COLUMN IF NOT EXISTS sled_policy omicron.public.sled_policy,
    ADD COLUMN IF NOT EXISTS sled_state omicron.public.sled_state
        NOT NULL DEFAULT 'active';
