-- As in up1.sql: existing requests predate SP component collection. The
-- default is dropped in the next step.
ALTER TABLE omicron.public.fm_support_bundle_request_data_selection_flags
    ADD COLUMN IF NOT EXISTS include_sp_components BOOL NOT NULL DEFAULT FALSE;
