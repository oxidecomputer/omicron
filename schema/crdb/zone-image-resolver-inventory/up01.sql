-- Add the inv_zone_manifest_source enum.
CREATE TYPE IF NOT EXISTS inv_zone_manifest_source AS ENUM (
    'installinator',
    'sled-agent'
);