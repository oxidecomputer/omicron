CREATE TYPE IF NOT EXISTS omicron.public.external_service_kind AS ENUM (
    'nexus',
    'boundary_ntp',
    'external_dns'
);
