CREATE TABLE IF NOT EXISTS omicron.public.silo_quotas (
  silo_id UUID PRIMARY KEY,
  time_created TIMESTAMPTZ NOT NULL,
  time_modified TIMESTAMPTZ NOT NULL,
  cpus INT8 NOT NULL,
  memory_bytes INT8 NOT NULL,
  storage_bytes INT8 NOT NULL
);