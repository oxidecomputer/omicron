-- Add the disposition column to the bp_omicron_zone table.
ALTER TABLE omicron.public.bp_omicron_zone
    ADD COLUMN IF NOT EXISTS disposition omicron.public.bp_zone_disposition
    NOT NULL
    -- The only currently-representable zones are in-service and quiesced
    -- (represented by bp_omicron_zones_not_in_service, which we're going to
    -- drop in the next statement). We don't actually have any quiesced zones
    -- yet, so it's fine to just do this.
    DEFAULT 'in_service';
