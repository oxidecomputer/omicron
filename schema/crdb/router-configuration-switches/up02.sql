SET LOCAL disallow_full_table_scans = 'off';
INSERT INTO omicron.public.router_configuration_switch
    (router_configuration_id, switch, bgp_asn, bgp_max_paths, bgp_announce_set_id)
SELECT id, switch, bgp_asn, bgp_max_paths, bgp_announce_set_id
FROM omicron.public.router_configuration
WHERE time_deleted IS NULL
ON CONFLICT DO NOTHING;
