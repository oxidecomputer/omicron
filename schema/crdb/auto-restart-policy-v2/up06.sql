CREATE TYPE IF NOT EXISTS omicron.public.instance_auto_restart AS ENUM (
    'never',
    'best_effort'
);
