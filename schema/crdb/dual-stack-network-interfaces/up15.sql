-- Recreate the index on NICs for V2P mapping details that
-- we dropped to rename the columns
CREATE INDEX IF NOT EXISTS v2p_mapping_details
ON omicron.public.network_interface (
  time_deleted, kind, subnet_id, vpc_id, parent_id
) STORING (mac, ipv4, ipv6);
