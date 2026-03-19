ALTER TABLE omicron.public.ereport
    ADD COLUMN IF NOT EXISTS slot_type omicron.public.sp_type;
