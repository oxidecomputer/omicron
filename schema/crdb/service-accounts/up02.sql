CREATE UNIQUE INDEX IF NOT EXISTS lookup_service_account_by_project
    ON omicron.public.service_account (project_id, name)
    WHERE time_deleted IS NULL;
