ALTER TABLE omicron.public.bp_omicron_zone
    ADD COLUMN IF NOT EXISTS disposition_temp bp_zone_disposition_temp;
