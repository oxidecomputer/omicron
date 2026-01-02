-- Create the new index on `ipv6`.
CREATE UNIQUE INDEX IF NOT EXISTS
network_interface_subnet_id_ipv6_key
ON omicron.public.network_interface (
    subnet_id,
    ipv6
) WHERE
    time_deleted IS NULL AND ipv6 IS NOT NULL;
