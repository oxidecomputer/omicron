ALTER TABLE omicron.public.ip_pool
    DROP COLUMN IF EXISTS project_id,
    ADD COLUMN IF NOT EXISTS is_default BOOLEAN NOT NULL DEFAULT FALSE,
    DROP CONSTRAINT IF EXISTS project_implies_silo,
    DROP CONSTRAINT IF EXISTS internal_pools_have_null_silo_and_project;
