CREATE TYPE IF NOT EXISTS omicron.public.region_replacement_state AS ENUM (
  'requested',
  'allocating',
  'running',
  'driving',
  'replacement_done',
  'completing',
  'complete'
);
