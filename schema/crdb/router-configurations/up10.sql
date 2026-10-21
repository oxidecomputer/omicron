CREATE TABLE IF NOT EXISTS omicron.public.control_plane_router_configuration (
    priority INT4 NOT NULL CHECK (priority >= 0 AND priority <= 65535),
    router_configuration_id UUID,

    PRIMARY KEY (priority)
);
