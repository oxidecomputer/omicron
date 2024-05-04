CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_bgp_peer_config_communities (
    port_settings_id UUID NOT NULL,
    interface_name TEXT NOT NULL,
    addr INET NOT NULL,
    community INT8 NOT NULL,

    PRIMARY KEY (port_settings_id, interface_name, addr, community)
);
