ALTER TABLE omicron.public.dataset
    ADD COLUMN IF NOT EXISTS quota INT8,
    ADD COLUMN IF NOT EXISTS reservation INT8,
    ADD COLUMN IF NOT EXISTS compression TEXT
