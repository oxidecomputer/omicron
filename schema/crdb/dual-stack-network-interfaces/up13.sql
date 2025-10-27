-- Recreate the index on NICs by parent that we dropped
-- to add the `ipv6` column to it.
CREATE INDEX IF NOT EXISTS network_interface_by_parent
ON omicron.public.network_interface (parent_id)
STORING (name, kind, vpc_id, subnet_id, mac, ip, ipv6, slot);
