-- Add this enum.
CREATE TYPE IF NOT EXISTS omicron.public.bp_zone_image_source AS ENUM (
    'install_dataset',
    'artifact'
);
