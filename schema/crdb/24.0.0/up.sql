ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS public_keys STRING[] NOT NULL;
