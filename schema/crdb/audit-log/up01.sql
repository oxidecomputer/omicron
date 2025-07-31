CREATE TYPE IF NOT EXISTS omicron.public.audit_log_result_kind AS ENUM (
    'success',
    'error', 
    'timeout'
);
