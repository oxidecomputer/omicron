CREATE TABLE IF NOT EXISTS omicron.public.external_subnet (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    subnet_pool_id UUID NOT NULL,
    subnet_pool_member_id UUID NOT NULL,
    project_id UUID NOT NULL,
    subnet INET NOT NULL,
    first_address INET AS (subnet & netmask(subnet)) VIRTUAL,
    last_address INET AS (
        broadcast(subnet) & (netmask(subnet) | hostmask(subnet))
    ) VIRTUAL,
    attach_state omicron.public.ip_attach_state NOT NULL,
    instance_id UUID
);
