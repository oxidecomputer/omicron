CREATE TABLE IF NOT EXISTS omicron.public.anti_affinity_group (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    -- Anti-Affinity groups are contained within projects
    project_id UUID NOT NULL,
    policy omicron.public.affinity_policy NOT NULL,
    failure_domain omicron.public.failure_domain NOT NULL
);

