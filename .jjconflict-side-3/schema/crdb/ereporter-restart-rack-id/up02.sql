-- Backfill `rack_id` for ereporter restarts that were recorded before this
-- column existed.
SET LOCAL disallow_full_table_scans = off;

UPDATE
    omicron.public.ereporter_restart
SET
    -- This relies on there being EXACTLY ONE record in the `rack` table.
    -- Because the left-hand side of the SET is a scalar value, the UPDATE
    -- statement will *fail* if the `rack` table contains multiple rows, as the
    -- `SELECT id FROM omicron.public.rack` subquery will return multiple
    -- values. This is on purpose! We could avoid this by using `LIMIT 1` in the
    -- subquery, but this would result in arbitrary rows being returned if the
    -- `rack` table contains multiple rows.
    --
    -- This migration will only run prior to any multi-rack support in the
    -- Omicron database, so there will never be more than one row in the `rack`
    -- table. And, because this migration will only run after rack init, it
    -- should never encounter an empty `rack` table. Thus, we can safely rely on
    -- the assumption that this query will return exactly one row.
    rack_id = (SELECT id FROM omicron.public.rack)
WHERE
    rack_id IS NULL;
