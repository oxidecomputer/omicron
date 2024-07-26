CREATE TABLE IF NOT EXISTS omicron.public.lldp_link_config (
    id UUID PRIMARY KEY,
    enabled BOOL NOT NULL,
    link_name STRING(63),
    link_description STRING(512),
    chassis_id STRING(63),
    system_name STRING(63),
    system_description STRING(512),
    management_ip TEXT,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);
