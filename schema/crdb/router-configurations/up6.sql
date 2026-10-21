CREATE TABLE IF NOT EXISTS omicron.public.silo_router_configuration (
    silo_id UUID NOT NULL,
    router_configuration_id UUID NOT NULL,
    priority INT4 NOT NULL CHECK (priority >= 0 AND priority <= 65535),

    PRIMARY KEY (silo_id, priority)
);
