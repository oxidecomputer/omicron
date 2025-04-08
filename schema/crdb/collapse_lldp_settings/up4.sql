/*
 * Drop the old lldp_config table, which has been replaced by lldp_link_config.
 */
DROP TABLE IF EXISTS omicron.public.lldp_config;
