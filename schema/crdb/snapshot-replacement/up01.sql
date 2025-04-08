CREATE TYPE IF NOT EXISTS omicron.public.region_snapshot_replacement_state AS ENUM (
  'requested',
  'allocating',
  'replacement_done',
  'deleting_old_volume',
  'running',
  'complete'
);
