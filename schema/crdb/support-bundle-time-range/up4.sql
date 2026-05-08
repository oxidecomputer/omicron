-- Drop start_time from the support_bundle ereports table. CASCADE
-- removes the dependent (anonymous) CHECK constraint on
-- start_time/end_time, which is no longer relevant once the time
-- columns live in support_bundle_data_selection_time_range.

ALTER TABLE omicron.public.support_bundle_data_selection_ereports
    DROP COLUMN IF EXISTS start_time CASCADE;
