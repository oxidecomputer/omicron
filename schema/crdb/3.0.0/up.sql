ALTER TABLE omicron.public.ip_pool
    ADD COLUMN IF NOT EXISTS silo_ID UUID,
    ADD COLUMN IF NOT EXISTS project_id UUID,

    -- if silo_id is null, then project_id must be null
    ADD CONSTRAINT IF NOT EXISTS project_implies_silo CHECK (
      NOT ((silo_id IS NULL) AND (project_id IS NOT NULL))
    ),

    -- if internal = true, non-null silo_id and project_id are not allowed
    ADD CONSTRAINT IF NOT EXISTS internal_pools_have_null_silo_and_project CHECK (
       NOT (INTERNAL AND ((silo_id IS NOT NULL) OR (project_id IS NOT NULL)))
    );
