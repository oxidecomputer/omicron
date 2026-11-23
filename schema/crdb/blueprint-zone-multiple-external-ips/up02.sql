/*
 * Backfill the new external IP child table from `bp_omicron_zone`.
 *
 * These addresses were previously stored inline with the rest of the row. We
 * need to move them out to allow multiple IPs per zone. Unlike the inventory
 * equivalent, each blueprint external IP is an *allocated* IP, so we also carry
 * its `external_ip_id`. From each row we take:
 *
 *   - Nexus:        the IP in `second_service_ip` (no port).
 *   - External DNS: the IP + port in `second_service_ip` /
 *                   `second_service_port`.
 *   - Boundary NTP: the IP + port range in `snat_ip` / `snat_first_port` /
 *                   `snat_last_port`.
 */
SET LOCAL disallow_full_table_scans = 'off';

INSERT INTO omicron.public.bp_omicron_zone_external_ip
    (blueprint_id, zone_id, external_ip_id, ip, port, snat_first_port, snat_last_port)
SELECT
    blueprint_id, id, external_ip_id, second_service_ip,
    NULL::INT4, NULL::INT4, NULL::INT4
FROM omicron.public.bp_omicron_zone
WHERE zone_type = 'nexus'
    AND external_ip_id IS NOT NULL AND second_service_ip IS NOT NULL
UNION ALL
SELECT
    blueprint_id, id, external_ip_id, second_service_ip,
    second_service_port, NULL::INT4, NULL::INT4
FROM omicron.public.bp_omicron_zone
WHERE zone_type = 'external_dns'
    AND external_ip_id IS NOT NULL AND second_service_ip IS NOT NULL
UNION ALL
SELECT
    blueprint_id, id, external_ip_id, snat_ip,
    NULL::INT4, snat_first_port, snat_last_port
FROM omicron.public.bp_omicron_zone
WHERE zone_type = 'boundary_ntp'
    AND external_ip_id IS NOT NULL AND snat_ip IS NOT NULL
ON CONFLICT DO NOTHING;
