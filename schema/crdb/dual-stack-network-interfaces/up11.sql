-- Recreate the index on ip we dropped earlier to rename the column.
-- NOTE: It has a new name here.
CREATE UNIQUE INDEX IF NOT EXISTS
network_interface_subnet_id_ipv4_key
ON omicron.public.network_interface (
    subnet_id,
    ip
) WHERE
    time_deleted IS NULL AND ip IS NOT NULL;
