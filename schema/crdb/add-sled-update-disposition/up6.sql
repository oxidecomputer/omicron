ALTER TABLE omicron.public.bp_sled_metadata
    ADD COLUMN IF NOT EXISTS update_disruption_policy
        omicron.public.reconfigurator_disruption_policy;
