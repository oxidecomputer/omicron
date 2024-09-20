CREATE INDEX IF NOT EXISTS lookup_bgp_config_by_bgp_announce_set_id ON omicron.public.bgp_config (
    bgp_announce_set_id
) WHERE
    time_deleted IS NULL;
