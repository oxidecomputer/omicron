CREATE TYPE IF NOT EXISTS omicron.public.user_data_export_state AS ENUM (
  'requested',
  'assigning',
  'live',
  'deleting',
  'deleted'
);
