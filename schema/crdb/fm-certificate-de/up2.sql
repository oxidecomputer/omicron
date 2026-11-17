CREATE TYPE IF NOT EXISTS omicron.public.fm_fact_certificate_kind AS ENUM (
    'best_certificate_expiring',
    'best_certificate_expired'
);
