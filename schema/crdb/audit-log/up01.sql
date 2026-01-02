CREATE TYPE IF NOT EXISTS omicron.public.audit_log_actor_kind AS ENUM (
    'user_builtin',
    'silo_user',
    'unauthenticated'
);