CREATE TABLE IF NOT EXISTS omicron.public.switch_port_settings_bgp_peer_config_allow_export (
    port_settings_id UUID NOT NULL,
    interface_name TEXT NOT NULL,
    addr INET NOT NULL,
    prefix INET NOT NULL,

    PRIMARY KEY (port_settings_id, interface_name, addr, prefix)
);
