-- Recreate the view we dropped earlier to add the `ipv6` column
CREATE VIEW IF NOT EXISTS omicron.public.instance_network_interface AS
SELECT
    id,
    name,
    description,
    time_created,
    time_modified,
    time_deleted,
    parent_id AS instance_id,
    vpc_id,
    subnet_id,
    mac,
    ip AS ipv4,
    ipv6,
    slot,
    is_primary,
    transit_ips
FROM
    omicron.public.network_interface
WHERE
    kind = 'instance';
