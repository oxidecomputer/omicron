CREATE TYPE IF NOT EXISTS omicron.public.sp_component_presence AS ENUM (
    'present',
    'not_present',
    'failed',
    'unavailable',
    'timeout',
    'error'
);
