/*
 * Drop the old lldp_service_config table, which has been incorporated into the
 * new lldp_link_config.
 */
DROP TABLE IF EXISTS omicron.public.lldp_service_config;
