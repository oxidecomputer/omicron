CREATE TYPE IF NOT EXISTS omicron.public.migration_state AS ENUM (
  'in_progress',
  'failed',
  'completed'
);
