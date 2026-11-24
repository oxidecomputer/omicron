CREATE TABLE IF NOT EXISTS omicron.public.wicket_ssh_auth (
    generation INT8 PRIMARY KEY CHECK (generation >= 1),
    time_created TIMESTAMPTZ NOT NULL,
    description STRING NOT NULL,
    allow_support_keys BOOL
);
