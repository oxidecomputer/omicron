CREATE INDEX IF NOT EXISTS lookup_router_configuration_switch_by_bgp_announce_set_id
    ON omicron.public.router_configuration_switch (bgp_announce_set_id)
    WHERE bgp_announce_set_id IS NOT NULL;
