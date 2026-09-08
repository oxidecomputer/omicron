CREATE TYPE IF NOT EXISTS omicron.public.sled_bp_availability AS ENUM (
    'available',
    'unavailable',
    'decommissioned'
);
