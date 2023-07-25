ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS boot_on_fault BOOL NOT NULL DEFAULT false;
