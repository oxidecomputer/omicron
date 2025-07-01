SET LOCAL disallow_full_table_scans = off;

-- Convert to lower-case strings. Existing filters like "ICMP"
-- will still be interpreted as 'all ICMP'.
UPDATE omicron.public.vpc_firewall_rule
SET
  filter_protocols_2 = lower(filter_protocols::STRING)::STRING(32)[];
