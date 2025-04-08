ALTER TABLE omicron.public.switch_port_settings_link_config ADD COLUMN IF NOT EXISTS autoneg BOOL NOT NULL DEFAULT false;
