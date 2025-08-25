CREATE TYPE IF NOT EXISTS omicron.public.db_metadata_nexus_state AS ENUM (
  'active',
  'not_yet',
  'quiesced'
);

