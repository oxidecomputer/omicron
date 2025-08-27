ALTER TABLE omicron.public.blueprint ADD COLUMN IF NOT EXISTS nexus_generation INT8 NOT NULL DEFAULT 1;
