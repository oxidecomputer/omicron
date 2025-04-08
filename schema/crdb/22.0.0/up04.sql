CREATE TYPE IF NOT EXISTS omicron.public.zone_type AS ENUM (
  'boundary_ntp',
  'clickhouse',
  'clickhouse_keeper',
  'cockroach_db',
  'crucible',
  'crucible_pantry',
  'external_dns',
  'internal_dns',
  'internal_ntp',
  'nexus',
  'oximeter'
);
