-- Migrate any pre-existing time bounds from the per-category ereports
-- tables into the new bundle-level time_range tables. Idempotent via
-- ON CONFLICT DO NOTHING (the new tables are keyed by the same id(s)
-- as the source rows).
--
-- Reading every row of the ereports tables is the point here, so
-- override CockroachDB's full-table-scan guardrail for this txn.

SET LOCAL disallow_full_table_scans = off;

INSERT INTO omicron.public.support_bundle_data_selection_time_range
    (bundle_id, start_time, end_time)
SELECT bundle_id, start_time, end_time
FROM omicron.public.support_bundle_data_selection_ereports
WHERE start_time IS NOT NULL OR end_time IS NOT NULL
ON CONFLICT (bundle_id) DO NOTHING;

INSERT INTO omicron.public.fm_support_bundle_request_data_selection_time_range
    (sitrep_id, request_id, start_time, end_time)
SELECT sitrep_id, request_id, start_time, end_time
FROM omicron.public.fm_support_bundle_request_data_selection_ereports
WHERE start_time IS NOT NULL OR end_time IS NOT NULL
ON CONFLICT (sitrep_id, request_id) DO NOTHING;
