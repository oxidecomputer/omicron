-- Backfill any existing bgp_config rows that have NULL max_paths.
-- The column was added as nullable in migration 230, but nexus
-- rust code (SqlU8, not Option<SqlU8>) assumes it is NOT NULL.

SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.bgp_config SET max_paths = 1 WHERE max_paths IS NULL;
