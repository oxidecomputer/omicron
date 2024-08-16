CREATE TYPE IF NOT EXISTS omicron.public.ip_attach_state AS ENUM (
    'detached',
    'attached',
    'detaching',
    'attaching'
);
