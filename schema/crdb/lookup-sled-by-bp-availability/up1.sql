CREATE INDEX IF NOT EXISTS lookup_sled_by_bp_availability
    ON omicron.public.rendezvous_sled_bp_availability (bp_availability);
