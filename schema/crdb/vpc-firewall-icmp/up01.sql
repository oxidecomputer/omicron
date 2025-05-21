-- We need to support more specific filters on given protocol families
-- (e.g., optional ICMP code/type specifiers). Move to have Nexus
-- parse/deparse these. This also opens the door for matches by IpProtocol
-- ID (numeric) in future.
ALTER TABLE omicron.public.vpc_firewall_rule
  ALTER COLUMN filter_protocols TYPE STRING(32)[];
