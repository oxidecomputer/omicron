CREATE TABLE IF NOT EXISTS omicron.public.vmm (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    instance_id UUID NOT NULL,
    state omicron.public.instance_state NOT NULL,
    time_state_updated TIMESTAMPTZ NOT NULL,
    state_generation INT NOT NULL,
    sled_id UUID NOT NULL,
    propolis_ip INET NOT NULL
);
