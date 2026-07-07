-- It is exceedingly unlikely that any rows exist with a NULL mode, but this
-- backfill ensures the subsequent NOT NULL constraint always succeeds.
SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.bfd_session
SET mode = 'single_hop'
WHERE mode IS NULL;

ALTER TABLE omicron.public.bfd_session
ALTER COLUMN mode SET NOT NULL;
