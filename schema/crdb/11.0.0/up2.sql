CREATE TABLE IF NOT EXISTS omicron.public.ipv4_nat_entry (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_address INET NOT NULL,
    first_port INT4 NOT NULL,
    last_port INT4 NOT NULL,
    sled_address INET NOT NULL,
    vni INT4  NOT NULL,
    mac INT8 NOT NULL,
    version_added INT8 NOT NULL DEFAULT nextval('omicron.public.ipv4_nat_version'),
    version_removed INT8,
    time_created TIMESTAMPTZ NOT NULL DEFAULT now(),
    time_deleted TIMESTAMPTZ
);
