ALTER TABLE omicron.public.reconfigurator_config
    ADD COLUMN IF NOT EXISTS sled_reboot_policy
        omicron.public.reconfigurator_planner_sled_reboot_policy
        NOT NULL DEFAULT 'immediate_no_evacuation';
