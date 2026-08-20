/*
 * Backfill the new external IP child table from `inv_omicron_sled_config_zone`.
 *
 * These addresses were previously stored inline with the rest of the row. We
 * need to move them out to allow multiple IPs per zone. From each row we take:

 *   - Nexus:        the IP in `second_service_ip` (no port).
 *   - External DNS: the IP + port in `second_service_ip` /
 *                   `second_service_port`.
 *   - Boundary NTP: the IP + port range in `snat_ip` / `snat_first_port` /
 *                   `snat_last_port`.
 */
SET LOCAL disallow_full_table_scans = 'off';

INSERT INTO omicron.public.inv_omicron_sled_config_zone_external_ip
    (inv_collection_id, sled_config_id, zone_id, ip, port, snat_first_port, snat_last_port)
SELECT
    inv_collection_id, sled_config_id, id, second_service_ip,
    NULL::INT4, NULL::INT4, NULL::INT4
FROM omicron.public.inv_omicron_sled_config_zone
WHERE zone_type = 'nexus' AND second_service_ip IS NOT NULL
UNION ALL
SELECT
    inv_collection_id, sled_config_id, id, second_service_ip,
    second_service_port, NULL::INT4, NULL::INT4
FROM omicron.public.inv_omicron_sled_config_zone
WHERE zone_type = 'external_dns' AND second_service_ip IS NOT NULL
UNION ALL
SELECT
    inv_collection_id, sled_config_id, id, snat_ip,
    NULL::INT4, snat_first_port, snat_last_port
FROM omicron.public.inv_omicron_sled_config_zone
WHERE zone_type = 'boundary_ntp' AND snat_ip IS NOT NULL;
