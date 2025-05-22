-- now make the new column non-nullable
ALTER TABLE omicron.public.instance
ALTER COLUMN intended_state
SET NOT NULL;
