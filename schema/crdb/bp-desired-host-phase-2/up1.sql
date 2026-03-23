ALTER TABLE omicron.public.bp_sled_metadata
    ADD COLUMN IF NOT EXISTS
    host_phase_2_desired_slot_a STRING(64);
