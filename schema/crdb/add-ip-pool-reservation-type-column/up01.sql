CREATE TYPE IF NOT EXISTS
omicron.public.ip_pool_reservation_type AS ENUM (
    "external_silos",
    "oxide_internal"
);
