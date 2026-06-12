CREATE TYPE IF NOT EXISTS omicron.public.saga_reason_abandoned AS ENUM (
    'omdb',
    'unrecoverable'
);
