CREATE UNIQUE INDEX IF NOT EXISTS lookup_floating_ip_by_name_and_project on omicron.public.external_ip (
    project_id,
    name
) WHERE
    kind = 'floating' AND
    time_deleted is NULL AND
    project_id is NOT NULL;
