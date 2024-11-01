/*
 * Add a pointer to this link's transceiver equalization config settings.
 */
ALTER TABLE omicron.public.switch_port_settings_link_config ADD COLUMN IF NOT EXISTS tx_eq_config_id UUID;
