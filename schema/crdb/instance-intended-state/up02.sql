-- create the column. it's nullable for now, as we must backfill the intended
-- states of existing instances.
ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS intended_state omicron.public.instance_intended_state;
