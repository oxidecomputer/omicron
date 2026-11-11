CREATE TYPE IF NOT EXISTS omicron.public.saga_abandon_reason AS ENUM (
    'omdb',
    'unrecoverable'
);
