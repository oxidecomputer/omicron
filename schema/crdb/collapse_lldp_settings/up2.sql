/*
 * Add a pointer to this link's LLDP config settings.
 */
ALTER TABLE omicron.public.switch_port_settings_link_config ADD COLUMN IF NOT EXISTS lldp_link_config_id UUID;
