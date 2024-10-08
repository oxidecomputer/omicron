CREATE TABLE IF NOT EXISTS omicron.public.internet_gateway_ip_address (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    internet_gateway_id UUID,
    address INET
);

