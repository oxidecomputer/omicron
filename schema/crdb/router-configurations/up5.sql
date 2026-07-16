CREATE TABLE IF NOT EXISTS omicron.public.router_configuration_bfd_peer (
    router_configuration_id UUID NOT NULL,
    name STRING(63) NOT NULL,
    remote INET NOT NULL,
    local INET,
    mode omicron.public.bfd_mode NOT NULL,
    detection_threshold INT2 NOT NULL,
    required_rx INT8 NOT NULL,
    switch omicron.public.switch_slot NOT NULL,

    PRIMARY KEY (router_configuration_id, name)
);
