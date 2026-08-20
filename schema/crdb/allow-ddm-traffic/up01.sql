ALTER TABLE omicron.public.switch_port_settings_port_config
    ADD COLUMN IF NOT EXISTS allow_ddm_traffic BOOL NOT NULL DEFAULT false;
