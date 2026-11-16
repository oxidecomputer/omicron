-- Existing data selections predate SP component collection, so backfill
-- them with FALSE. The default is dropped in the next step.
ALTER TABLE omicron.public.support_bundle_data_selection_flags
    ADD COLUMN IF NOT EXISTS include_sp_components BOOL NOT NULL DEFAULT FALSE;
