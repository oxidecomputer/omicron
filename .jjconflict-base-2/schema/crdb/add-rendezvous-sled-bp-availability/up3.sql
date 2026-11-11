CREATE INDEX IF NOT EXISTS lookup_available_sled
    ON omicron.public.rendezvous_sled_bp_availability (sled_id)
    WHERE bp_availability = 'available';
