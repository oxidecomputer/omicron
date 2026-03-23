SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.bp_sled_metadata as bp
    SET subnet = (
        SELECT set_masklen(netmask('::/64') & ip, 64)
        FROM omicron.public.sled AS s
        WHERE bp.sled_id = s.id
    );
