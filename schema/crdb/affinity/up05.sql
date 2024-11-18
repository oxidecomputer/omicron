-- Describes a collection of instances that should not be co-located.
CREATE TABLE IF NOT EXISTS omicron.public.anti_affinity_group (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    policy omicron.public.affinity_policy NOT NULL,
    failure_domain omicron.public.failure_domain NOT NULL,
);

