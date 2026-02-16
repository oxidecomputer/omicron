CREATE UNIQUE INDEX IF NOT EXISTS external_subnet_project_id_name_key
ON omicron.public.external_subnet (
    project_id,
    name
)
WHERE
    time_deleted IS NULL;
