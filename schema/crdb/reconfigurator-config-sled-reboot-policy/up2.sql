ALTER TABLE omicron.public.reconfigurator_config
    ADD COLUMN IF NOT EXISTS sled_update_reboot_policy
        omicron.public.sled_update_reboot_policy
        NOT NULL DEFAULT 'immediate_no_evacuation';
