-- The source of the software release that should be deployed to the rack.
CREATE TYPE IF NOT EXISTS omicron.public.target_release_source AS ENUM (
    'unspecified',
    'system_version'
);
