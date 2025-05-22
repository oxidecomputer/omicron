SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.vpc_firewall_rule
SET
  filter_protocols = filter_protocols_2;
