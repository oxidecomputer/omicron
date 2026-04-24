-- Using `DEFAULT 1` here is a white lie: any system with a series of blueprints
-- will have (presumably!) gone through some external networking changes, which
-- means they should have multiple external networking generation values. But
-- all we really care about is incrementing this value moving forward, so for
-- simplicity, we backfill all old blueprints with 1. The next time a new
-- blueprint is created with a change, we'll bump to 2 (and so on moving
    -- forward).
ALTER TABLE omicron.public.blueprint
    ADD COLUMN IF NOT EXISTS external_networking_generation INT8 NOT NULL DEFAULT 1;
