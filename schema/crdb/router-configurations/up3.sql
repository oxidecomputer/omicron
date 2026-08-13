CREATE TABLE IF NOT EXISTS omicron.public.router_configuration_bgp_peer (
    router_configuration_id UUID NOT NULL,
    name STRING(63) NOT NULL,
    addr INET CHECK (host(addr) != '0.0.0.0' AND host(addr) != '::'),
    port_name TEXT,
    remote_asn INT8,
    allowed_import INET[],
    allowed_export INET[],
    hold_time INT8 NOT NULL,
    keepalive INT8 NOT NULL,
    connect_retry INT8 NOT NULL,
    delay_open INT8 NOT NULL,
    idle_hold_time INT8 NOT NULL,
    local_pref INT8,
    communities INT8[] NOT NULL,
    multi_exit_discriminator INT8,
    enforce_first_as BOOL NOT NULL,
    md5_auth_key TEXT,
    min_ttl INT2,
    vlan_id INT4,
    router_lifetime INT4 CHECK (
        router_lifetime >= 0 AND router_lifetime <= 9000
    ),

    CONSTRAINT numbered_xor_unnumbered_peer CHECK (
        (
            addr IS NOT NULL
            AND port_name IS NULL
            AND router_lifetime IS NULL
        )
        OR
        (
            addr IS NULL
            AND port_name IS NOT NULL
            AND router_lifetime IS NOT NULL
        )
    ),

    PRIMARY KEY (router_configuration_id, name)
);
