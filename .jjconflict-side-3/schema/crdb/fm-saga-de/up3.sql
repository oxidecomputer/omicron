CREATE TYPE IF NOT EXISTS omicron.public.fm_fact_saga_orphan_reason AS ENUM (
    'quiesced',
    'expunged'
);
