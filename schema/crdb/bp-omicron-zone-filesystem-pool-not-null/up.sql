-- This migration will fail if there are any remaining bp_omicron_zone entries
-- with a NULL filesystem_pool. We've confirmed via the `omdb reconfigurator
-- archive` outputs from R13 upgrades that all deployed systems have
-- fully-populated `filesystem_pool` values.
ALTER TABLE omicron.public.bp_omicron_zone
    ALTER COLUMN filesystem_pool
    SET NOT NULL;
