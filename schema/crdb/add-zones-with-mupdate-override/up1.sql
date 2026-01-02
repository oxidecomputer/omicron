ALTER TABLE omicron.public.reconfigurator_chicken_switches
    -- Default this to true on customer systems for r16. We'll set it to false
    -- in the future, possibly through another migration in the future.
    ADD COLUMN IF NOT EXISTS add_zones_with_mupdate_override BOOL NOT NULL DEFAULT TRUE;
