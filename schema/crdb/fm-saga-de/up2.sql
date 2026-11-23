CREATE TYPE IF NOT EXISTS omicron.public.fm_fact_saga_kind AS ENUM (
    'not_progressing',
    'owner_not_current_generation',
    'abandoned'
);
