ALTER TABLE omicron.public.inv_omicron_sled_config
ADD COLUMN IF NOT EXISTS host_phase_2_desired_slot_b STRING(64);
