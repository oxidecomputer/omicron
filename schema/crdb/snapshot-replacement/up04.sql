CREATE TYPE IF NOT EXISTS omicron.public.snapshot_replacement_step_state AS ENUM (
  'requested',
  'running',
  'complete',
  'volume_deleted'
);
