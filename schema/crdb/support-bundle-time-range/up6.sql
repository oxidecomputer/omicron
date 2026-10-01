-- Drop start_time from the FM ereports table. CASCADE removes the
-- dependent (anonymous) CHECK constraint.

ALTER TABLE omicron.public.fm_support_bundle_request_data_selection_ereports
    DROP COLUMN IF EXISTS start_time CASCADE;
