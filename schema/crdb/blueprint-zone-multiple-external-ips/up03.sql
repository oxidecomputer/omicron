/*
 * Now that the Nexus and external DNS external IPs live in the child table,
 * clear the inline `second_service_ip` / `second_service_port` columns for
 * those zones. These columns are no longer for external address information.
 *
 * NOTE: We don't need to delete the SNAT-related data for boundary NTP zones,
 * because we drop those columns wholesale in the following migration files. The
 * same is true of `external_ip_id`, which we drop below.
 */
SET LOCAL disallow_full_table_scans = 'off';

UPDATE omicron.public.bp_omicron_zone
SET second_service_ip = NULL, second_service_port = NULL
WHERE zone_type IN ('nexus', 'external_dns');
