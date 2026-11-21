CREATE TYPE IF NOT EXISTS omicron.public.nvme_health_error_kind AS ENUM (
    'log_page_read_failed',
    'not_polled'
);
