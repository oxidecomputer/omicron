-- This step used to copy everything from console_session to console_session_new
-- (see https://github.com/oxidecomputer/omicron/pull/9816), but we discovered
-- an exciting new class of bug when applying this migration on the colo rack.
-- That bug is https://github.com/oxidecomputer/omicron/issues/9866.
--
-- CREATE INDEX in CRDB proceeds in two steps: one that creates the index
-- metadata allowing it to be found by name, and another that actually makes
-- the schema change and populates the index. Nexus instance A starts the
-- CREATE INDEX step (up06, up07, up08) but because the table is so large (3
-- million rows in this case) the backfill takes a while. While that's going,
-- Nexus B also tries to pick up step 6, and it immediately succeeds because
-- CREATE INDEX IF NOT EXISTS sees that the index exists. This marks the step
-- as successful even though it's actually still running. Then the backfill runs
-- out of memory and fails, and the index metadata is rolled back, so there is
-- no index, but the migration system thinks it worked.
--
-- We are working on more reliable ways to avoid this (it could happen while
-- creating an index on any large table) but in the meantime we can avoid
-- this particular manifestation by making sure the console_session table is
-- small when the CREATE INDEX runs. So, instead of copying all the rows from
-- console_session, we only copy recent ones by filtering for
--
--   time_created >= now() - INTERVAL '24 hours'.
-- 
-- The absolute session timeout is 24 hours (session_absolute_timeout_minutes
-- = 1440), so any session older than that guaranteed to be expired already.
--
-- Full table scan is unavoidable when copying data
SET LOCAL disallow_full_table_scans = off;

INSERT INTO omicron.public.console_session_new
    (id, token, time_created, time_last_used, silo_user_id)
SELECT
    id, token, time_created, time_last_used, silo_user_id
FROM omicron.public.console_session
WHERE time_created >= now() - INTERVAL '24 hours'
ON CONFLICT DO NOTHING;
