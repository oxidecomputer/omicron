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
    ip,
    slot,
    is_primary,
    transit_ips
FROM
    omicron.public.network_interface
WHERE
    kind = 'instance';
