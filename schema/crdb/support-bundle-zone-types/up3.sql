ALTER TABLE omicron.public.support_bundle_data_selection_host_info
    ADD CONSTRAINT IF NOT EXISTS all_zone_types_and_specific_zone_types_are_mutually_exclusive CHECK (
        NOT (all_zone_types AND cardinality(zone_types) > 0)
    );
