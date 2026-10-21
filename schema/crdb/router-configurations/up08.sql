CREATE TABLE IF NOT EXISTS omicron.public.router_configuration_static_route (
    router_configuration_id UUID NOT NULL,
    name STRING(63) NOT NULL,
    dst INET NOT NULL,
    gw INET NOT NULL,
    rib_priority INT2,
    vlan_id INT4,
    switch omicron.public.switch_slot NOT NULL,

    PRIMARY KEY (router_configuration_id, switch, name)
);
