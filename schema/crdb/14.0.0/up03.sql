CREATE UNIQUE INDEX IF NOT EXISTS lookup_floating_ip_by_name on omicron.public.external_ip (
    name
) WHERE
    kind = 'floating' AND
    time_deleted is NULL AND
    project_id is NULL;
