CREATE INDEX IF NOT EXISTS lookup_bgp_config_by_asn ON omicron.public.bgp_config (
    asn
) WHERE time_deleted IS NULL;
