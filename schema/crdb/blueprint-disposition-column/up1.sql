-- Add the disposition enum.
CREATE TYPE IF NOT EXISTS omicron.public.bp_zone_disposition AS ENUM (
    'in_service',
    'quiesced',
    'expunged'
);
