CREATE TYPE IF NOT EXISTS omicron.public.timeseries_authz_scope AS ENUM (
    'fleet',
    'silo',
    'project',
    'viewable_to_all'
);
