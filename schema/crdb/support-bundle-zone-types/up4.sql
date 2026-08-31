ALTER TABLE omicron.public.fm_support_bundle_request_data_selection_host_info
    ADD COLUMN IF NOT EXISTS all_zone_types BOOL NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS zone_types TEXT[] NOT NULL DEFAULT ARRAY[];
