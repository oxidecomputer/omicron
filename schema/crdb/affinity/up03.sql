CREATE TABLE IF NOT EXISTS omicron.public.affinity_group (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    policy omicron.public.affinity_policy NOT NULL,
    distance omicron.public.affinity_distance NOT NULL,
);

