/*
 * The old lldp_service_config_id is being replaced with lldp_link_config_id.
 */
ALTER TABLE omicron.public.switch_port_settings_link_config DROP COLUMN IF EXISTS lldp_service_config_id;
