ALTER TABLE omicron.public.reconfigurator_config
    ADD COLUMN IF NOT EXISTS disruption_policy
        omicron.public.reconfigurator_disruption_policy
        NOT NULL DEFAULT 'terminate';
