-- Nexus stamps a default start bound onto every data selection it
-- persists, and collection uses the stored selection exactly as given.
-- Bundles persisted before that stamping existed and still awaiting
-- collection would otherwise collect unbounded log history, so fill in
-- the default 7-day lookback for them here, anchored to the end bound
-- when one is set (matching how Nexus fills a missing start). Bundles in
-- terminal states are left as-is: their collection already happened, and
-- inventing a start bound would misrecord it.
--
-- Idempotent: the UPDATE is guarded on start_time IS NULL and the INSERT
-- on the primary key. Scanning every row of these tables is the point
-- here, so override CockroachDB's full-table-scan guardrail for this txn.

SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.support_bundle_data_selection_time_range
SET start_time = COALESCE(end_time, NOW()) - INTERVAL '7 days'
WHERE start_time IS NULL
  AND bundle_id IN (
    SELECT id FROM omicron.public.support_bundle WHERE state = 'collecting'
  );

INSERT INTO omicron.public.support_bundle_data_selection_time_range
    (bundle_id, start_time, end_time)
SELECT id, NOW() - INTERVAL '7 days', NULL
FROM omicron.public.support_bundle
WHERE state = 'collecting'
ON CONFLICT (bundle_id) DO NOTHING;
