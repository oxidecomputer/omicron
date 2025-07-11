set local disallow_full_table_scans = off;

DELETE FROM bp_oximeter_read_policy WHERE blueprint_id NOT IN (SELECT blueprint_id FROM blueprint);
