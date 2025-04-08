CREATE TYPE IF NOT EXISTS omicron.public.migration_state AS ENUM (
  'pending',
  'in_progress',
  'failed',
  'completed'
);
