-- We need to support more specific filters on given protocol families
-- (e.g., optional ICMP code/type specifiers). Move to have Nexus
-- parse/deparse these. This also opens the door for matches by IpProtocol
-- ID (numeric) in future.
--
-- However, ALTER COLUMN type from ENUM[] to STRING[] is 'experimental', given
-- the error messages. So we'll be roundtripping through a new column.

ALTER TABLE omicron.public.vpc_firewall_rule
  ADD COLUMN IF NOT EXISTS filter_protocols_2 STRING(32)[];
