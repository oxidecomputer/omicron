SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.router_configuration_bfd_peer AS child SET switch = parent.switch
FROM omicron.public.router_configuration AS parent
WHERE child.router_configuration_id = parent.id AND child.switch IS NULL;
