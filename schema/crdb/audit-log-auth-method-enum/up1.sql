CREATE TYPE IF NOT EXISTS omicron.public.audit_log_auth_method AS ENUM (
    'session_cookie',
    'access_token',
    'scim_token',
    'spoof'
);
