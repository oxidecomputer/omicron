CREATE VIEW IF NOT EXISTS omicron.public.floating_ip AS
SELECT
    id,
    name,
    description,
    time_created,
    time_modified,
    time_deleted,
    ip_pool_id,
    ip_pool_range_id,
    is_service,
    parent_id,
    ip,
    project_id
FROM
    omicron.public.external_ip
WHERE
    omicron.public.external_ip.kind = 'floating' AND
    project_id IS NOT NULL;
