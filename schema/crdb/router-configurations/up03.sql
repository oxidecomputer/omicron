CREATE TABLE IF NOT EXISTS omicron.public.router_configuration_switch (
    router_configuration_id UUID NOT NULL,
    switch omicron.public.switch_slot NOT NULL,

    bgp_asn INT8,
    bgp_max_paths INT2 CHECK (
        bgp_max_paths IS NULL
        OR (bgp_max_paths > 0 AND bgp_max_paths <= 32)
    ),
    bgp_announce_set_id UUID,

    PRIMARY KEY (router_configuration_id, switch),

    CONSTRAINT bgp_config_set_together CHECK (
        (
            bgp_asn IS NULL
            AND bgp_max_paths IS NULL
            AND bgp_announce_set_id IS NULL
        )
        OR
        (
            bgp_asn IS NOT NULL
            AND bgp_max_paths IS NOT NULL
            AND bgp_announce_set_id IS NOT NULL
        )
    )
);
